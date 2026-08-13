#include "mem_pool.hpp"

#include <cassert>
#include <iostream>
#include <memory_resource>
#include <numa.h>
#include "numaif.h"
#include <vector>
#include <cerrno>
#include <stdexcept>
#include <algorithm>
#include <cstring>
#include <thread>

#include <sys/mman.h>

using namespace hyrise;

namespace
{
constexpr std::size_t HUGE_PAGE_SIZE = std::size_t{2} << 20;
constexpr std::size_t BITS_PER_MASK_WORD = 8 * sizeof(unsigned long);

// Write one byte into every huge page of [base, base + bytes) so the pages are faulted in
// (and, thanks to the preceding mbind, placed on the target node) before anybody copies data
// into the pool. Done in parallel because faulting is the dominant cost of a fresh pool.
void prefault_parallel(char *base, std::size_t bytes)
{
    const auto page_count = bytes / HUGE_PAGE_SIZE;
    if (page_count == 0)
    {
        return;
    }

    // Faulting is bandwidth-bound; a handful of threads saturates it.
    auto thread_count = std::min({std::size_t{std::max(1u, std::thread::hardware_concurrency())}, page_count,
                                  std::size_t{16}});

    auto threads = std::vector<std::thread>{};
    threads.reserve(thread_count);
    const auto pages_per_thread = (page_count + thread_count - 1) / thread_count;

    for (auto thread_id = std::size_t{0}; thread_id < thread_count; ++thread_id)
    {
        const auto first_page = thread_id * pages_per_thread;
        const auto last_page = std::min(first_page + pages_per_thread, page_count);
        if (first_page >= last_page)
        {
            break;
        }
        threads.emplace_back([base, first_page, last_page] {
            for (auto page = first_page; page < last_page; ++page)
            {
                base[page * HUGE_PAGE_SIZE] = 0;
            }
        });
    }

    for (auto &thread : threads)
    {
        thread.join();
    }
}
} // namespace

bool NumaMonotonicResource::track_peak = false;

// Allocate raw memory on a given NUMA node.
//
// Deliberately does NOT use numa_alloc_onnode: that hands back a plain 4 KiB-page mapping, and on
// hosts where transparent hugepages are set to "madvise" (the common default) every byte written
// into a fresh pool costs a 4 KiB fault. That alone caps migration throughput at ~1.5-2 GB/s.
// Instead we map a 2 MiB-aligned region, ask for THP explicitly, bind it to the target node, and
// pre-fault it in parallel, so the actual segment copies hit warm, huge-page-backed memory.
//
// `prefault` is opt-in because it eagerly commits the whole mapping: right for a pool sized to the
// data that is about to fill it, wrong for a deliberately oversized pool (`hsh new_mem 64000000000`)
// that would otherwise only occupy what it actually uses. THP is requested either way.
//
// Returns the mapping, whose size is the requested size rounded up to a huge page. The caller must
// release it with free_numa_allocation (NOT numa_free).
NumaAllocation allocate_on_numa_node(std::size_t bytes, int node, bool prefault)
{
    if (numa_available() < 0)
    {
        std::cerr << "NUMA not available on this system\n";
        std::abort();
    }
    if (node < 0 || node > numa_max_node())
    {
        std::cerr << "Invalid NUMA node " << node
                  << " (valid: 0.." << numa_max_node() << ")\n";
        std::abort();
    }
    if (bytes == 0)
    {
        return NumaAllocation{};
    }

    const auto mapped_size = (bytes + HUGE_PAGE_SIZE - 1) & ~(HUGE_PAGE_SIZE - 1);

    // Over-map by one huge page so we can hand back a 2 MiB-aligned start: the kernel only backs a
    // region with huge pages on 2 MiB-aligned boundaries, so an unaligned start silently degrades
    // most of the mapping to 4 KiB pages.
    auto *raw = static_cast<char *>(mmap(nullptr, mapped_size + HUGE_PAGE_SIZE, PROT_READ | PROT_WRITE,
                                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0));
    if (raw == MAP_FAILED)
    {
        std::cerr << "Failed to map " << bytes << " bytes for node " << node << ": " << std::strerror(errno) << "\n";
        std::abort();
    }

    auto *aligned = reinterpret_cast<char *>((reinterpret_cast<std::uintptr_t>(raw) + HUGE_PAGE_SIZE - 1) &
                                             ~(static_cast<std::uintptr_t>(HUGE_PAGE_SIZE) - 1));
    // Trim the slack on both sides so the mapping is exactly [aligned, aligned + mapped_size).
    if (aligned > raw)
    {
        munmap(raw, static_cast<std::size_t>(aligned - raw));
    }
    auto *tail = aligned + mapped_size;
    auto *raw_end = raw + mapped_size + HUGE_PAGE_SIZE;
    if (raw_end > tail)
    {
        munmap(tail, static_cast<std::size_t>(raw_end - tail));
    }

    if (madvise(aligned, mapped_size, MADV_HUGEPAGE) != 0)
    {
        // Not fatal: the pool still works, just with 4 KiB pages and the old throughput.
        std::cerr << "madvise(MADV_HUGEPAGE) failed: " << std::strerror(errno) << "\n";
    }

    // Bind before faulting -- the node is chosen at fault time, so the order matters. Note that this
    // is also why MAP_POPULATE cannot be used above: it would fault the pages in before the bind.
    const auto node_count = static_cast<std::size_t>(numa_max_node()) + 1;
    auto node_mask = std::vector<unsigned long>((node_count + BITS_PER_MASK_WORD - 1) / BITS_PER_MASK_WORD, 0UL);
    node_mask[static_cast<std::size_t>(node) / BITS_PER_MASK_WORD] |=
        1UL << (static_cast<std::size_t>(node) % BITS_PER_MASK_WORD);
    if (mbind(aligned, mapped_size, MPOL_BIND, node_mask.data(), node_count + 1, MPOL_MF_STRICT) != 0)
    {
        std::cerr << "mbind of " << mapped_size << " bytes to node " << node << " failed: " << std::strerror(errno)
                  << "\n";
        std::abort();
    }

    if (prefault)
    {
        prefault_parallel(aligned, mapped_size);
    }

    return NumaAllocation{aligned, mapped_size};
}

void free_numa_allocation(const NumaAllocation &allocation)
{
    if (allocation.ptr && allocation.size)
    {
        munmap(allocation.ptr, allocation.size);
    }
}

size_t MemPoolManager::create_pool(uint64_t size, int numa_node, bool serialize, bool prefault)
{
    auto mem_pool_ptr = std::make_shared<NumaMonotonicResource>(size, numa_node, serialize,
                                                                std::pmr::null_memory_resource(), prefault);
    size_t pool_id = unique_pool_id();
    _pools.insert(std::make_pair(pool_id, mem_pool_ptr));
    return pool_id;
}

std::shared_ptr<NumaMonotonicResource> MemPoolManager::get_pool(const size_t pool_id)
{
    if (_pools.find(pool_id) == _pools.end())
    {
        std::cerr << "Pool with id " << pool_id << " not found\n";
        exit(2);
    }
    return _pools.find(pool_id)->second;
}

void *NumaMonotonicResource::do_allocate(std::size_t bytes, std::size_t alignment)
{
    // Lock-free bump-pointer allocate. Standard CAS loop:
    //   1. Read current bump position.
    //   2. Compute aligned start and new bump position.
    //   3. CAS — if another thread moved the pointer in between, retry against the new value.
    char *cur = _next.load(std::memory_order_relaxed);
    char *aligned;
    char *new_next;
    for (;;)
    {
        const auto a = (reinterpret_cast<std::uintptr_t>(cur) + alignment - 1) & ~(alignment - 1);
        aligned = reinterpret_cast<char *>(a);
        new_next = aligned + bytes;
        if (new_next > _end)
        {
            std::cerr << "NumaMonotonicResource allocation failed: requested " << bytes << " bytes with alignment " << alignment
                      << ", but only " << (_end - cur) << " bytes remain in the pool of size " << _size
                      << " on NUMA node " << _numa_node << "\n";
            std::cerr << print_backtrace() << std::endl;
            throw std::bad_alloc{};
        }
        if (_next.compare_exchange_weak(cur, new_next,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed))
        {
            break;
        }
        // cur was updated by compare_exchange_weak to the current value; retry.
    }
    _allocated_bytes.fetch_add(bytes, std::memory_order_relaxed);
    _total_allocated_bytes.fetch_add(bytes, std::memory_order_relaxed);
    // Update peak with a lock-free max(): load, compare, CAS, retry if stale.
    const auto current = _allocated_bytes.load(std::memory_order_relaxed);
    if (track_peak)
    {
        auto prev_peak = _peak_allocated_bytes.load(std::memory_order_relaxed);
        while (current > prev_peak &&
               !_peak_allocated_bytes.compare_exchange_weak(prev_peak, current,
                                                            std::memory_order_relaxed,
                                                            std::memory_order_relaxed))
        {
            // prev_peak refreshed by CAS; loop continues if current still exceeds it.
        }
    }
    return aligned;
}