#pragma once

#include "numaif.h"
#include <atomic>
#include <cassert>
#include <cerrno>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <numa.h>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "memory/mem_common.hpp"
#include "utils/debug_util.hpp"

void *allocate_on_numa_node(std::size_t bytes, int node);

// NUMA-backed bump allocator. One numa_alloc upfront; every do_allocate advances an
// atomic bump pointer via lock-free CAS, so multiple threads can allocate concurrently
// without a mutex on the hot path. Monotonic semantics: do_deallocate decrements the
// logical counter but never returns memory; pages return to the OS only when this
// resource is destroyed.
//
// The `_serialize` constructor flag is retained for API compatibility but is now a
// no-op label — all access is thread-safe by construction.
class NumaMonotonicResource : public std::pmr::memory_resource
{
  public:
    NumaMonotonicResource(std::size_t size_bytes, int numa_node, bool serialize = false,
                          std::pmr::memory_resource * /*upstream*/ = std::pmr::null_memory_resource())
        : _size(size_bytes),
          _numa_node(numa_node),
          _buffer(static_cast<char *>(allocate_on_numa_node(size_bytes, numa_node))),
          _next(_buffer),
          _end(_buffer + size_bytes),
          _serialize(serialize)
    {
    }

    ~NumaMonotonicResource() override
    {
        // Important: free NUMA memory
        if (_buffer && _size)
        {
            numa_free(_buffer, _size);
        }
    }

    std::uintptr_t start_address() const
    {
        return reinterpret_cast<std::uintptr_t>(_buffer);
    }

    std::uintptr_t end_address() const
    {
        return reinterpret_cast<std::uintptr_t>(_end);
    }

    std::size_t bump_ptr_offset() const
    {
        return static_cast<std::size_t>(_next.load(std::memory_order_relaxed) - _buffer);
    }

    std::size_t size() const
    {
        return _size;
    }

    size_t allocated_bytes() const
    {
        return _allocated_bytes.load(std::memory_order_relaxed);
    }

    // Kept for API compatibility — atomic loads are already safe; no lock needed.
    size_t allocated_bytes_mutex() const
    {
        return _allocated_bytes.load(std::memory_order_acquire);
    }

    size_t numa_node() const
    {
        return _numa_node;
    }

    void reset_peak()
    {
        _peak_allocated_bytes.store(0, std::memory_order_relaxed);
        _total_allocated_bytes.store(0, std::memory_order_relaxed);
        std::cout << __func__ << "\n";
    }

    // Rewind the bump pointer to the start of the buffer, reusing the underlying pages.
    // MUST only be called when nothing still holds memory from this pool — otherwise the
    // next allocation hands back live addresses, causing silent memory corruption.
    void reset()
    {
        const auto live = _allocated_bytes.load(std::memory_order_acquire);
        Assertf(live == 0, "NumaMonotonicResource::reset called with %lu live bytes remaining\n", live);
        _next.store(_buffer, std::memory_order_release);
        _allocated_bytes.store(0, std::memory_order_relaxed);
        _peak_allocated_bytes.store(0, std::memory_order_relaxed);
        _total_allocated_bytes.store(0, std::memory_order_relaxed);
    }

    int verify_numa_node() const
    {
        // Allocate a tiny page to test
        void *test_ptr = numa_alloc_onnode(4096, _numa_node);
        if (!test_ptr)
            return -1;

        // Fault it in
        *(volatile char *)test_ptr = 0;

        void *pages[1] = {test_ptr};
        int status[1] = {-1};
        int rc = move_pages(0, 1, pages, nullptr, status, 0);

        numa_free(test_ptr, 4096);

        if (rc == 0 && status[0] >= 0)
        {
            return status[0]; // kernel says this node works
        }
        return -1; // couldn't verify
    }

    hyrise::MemResourceStatus status() const
    {
        hyrise::MemResourceStatus status;
        status.description = std::string("NumaMonotonicResource") + (_serialize ? "_serialized" : "");
        status.resource_id = std::numeric_limits<size_t>::max(); // No fixed ID
        status.capacity_bytes = _size;
        status.allocated_bytes = _allocated_bytes.load(std::memory_order_relaxed);
        status.peak_allocated_bytes = _peak_allocated_bytes.load(std::memory_order_relaxed);
        status.total_allocated_bytes = _total_allocated_bytes.load(std::memory_order_relaxed);
        status.numa_node = _numa_node;
        status.bump_ptr_loc = bump_ptr_offset();
        return status;
    }

  protected:
    void *do_allocate(std::size_t bytes, std::size_t alignment) override
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
        auto prev_peak = _peak_allocated_bytes.load(std::memory_order_relaxed);
        while (current > prev_peak &&
               !_peak_allocated_bytes.compare_exchange_weak(prev_peak, current,
                                                            std::memory_order_relaxed,
                                                            std::memory_order_relaxed))
        {
            // prev_peak refreshed by CAS; loop continues if current still exceeds it.
        }

        return aligned;
    }

    void do_deallocate(void * /*p*/, std::size_t bytes, std::size_t /*alignment*/) override
    {
        // Monotonic: the memory is not actually returned. We only decrement the logical
        // counter so allocated_bytes() reflects "still-live bytes" rather than
        // "ever-allocated bytes".
        _allocated_bytes.fetch_sub(bytes, std::memory_order_relaxed);
    }

    bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override
    {
        return this == &other;
    }

  private:
    std::size_t _size{};
    int _numa_node{-1};
    char *_buffer{};
    std::atomic<char *> _next;          // bump pointer; only moves forward
    char *_end{};

    std::atomic<size_t> _allocated_bytes{0};      // alloc minus dealloc — "live" bytes
    std::atomic<size_t> _peak_allocated_bytes{0}; // max value of _allocated_bytes since last reset_peak
    std::atomic<size_t> _total_allocated_bytes{0}; // cumulative alloc only — "ever allocated" bytes

    // Retained only so status() can label the pool with "_serialized" if you want to
    // mark it that way externally. No longer used for synchronization — the resource
    // is lock-free and always safe for concurrent allocate.
    bool _serialize{false};
};

namespace hyrise
{
class MemPoolManager
{
  public:
    size_t create_pool(uint64_t size, int numa_node, bool serialize = false);
    std::shared_ptr<NumaMonotonicResource> get_pool(const size_t pool_id);
    bool exists(const size_t pool_id) const
    {
        return _pools.find(pool_id) != _pools.end();
    }
    void delete_pool(const size_t pool_id)
    {
        auto it = _pools.find(pool_id);
        Assertf(it != _pools.end(), "Trying to delete non-existing pool %lu\n", pool_id);
        Assertf(it->second.use_count() == 1, "Pool has %d sharers left, not 1", it->second.use_count());
        _pools.erase(pool_id);
    }

    void reset_pool(const size_t pool_id)
    {
        auto it = _pools.find(pool_id);
        Assertf(it != _pools.end(), "Trying to reset non-existing pool %lu\n", pool_id);
        it->second->reset();
    }

    void print_status() const
    {
        std::unordered_map<size_t, size_t> size_by_numa;

        std::cout << "Original Table Pools:\n";
        for (const auto &[pool_id, pool] : _pools)
        {
            std::cout << "  Pool ID: " << pool_id
                      << ", Size: " << pool->size() << " bytes"
                      << ", Allocated: " << pool->allocated_bytes() << " bytes"
                      << ", NUMA Node: " << pool->numa_node() << "\n";

            if (size_by_numa.find(pool->numa_node()) == size_by_numa.end())
            {
                size_by_numa[pool->numa_node()] = 0;
            }
            size_by_numa[pool->numa_node()] += pool->allocated_bytes();
        }
        std::cout << "Total allocated bytes by NUMA node:\n";
        for (const auto &[numa_node, total_bytes] : size_by_numa)
        {
            std::cout << "  NUMA Node " << numa_node << ": " << total_bytes << " bytes\n";
        }
    }

  private:
    size_t unique_pool_id()
    {
        return _unique_pool_id++;
    }

    std::unordered_map<size_t, std::shared_ptr<NumaMonotonicResource>> _pools;
    size_t _unique_pool_id{0};
};
} // namespace hyrise