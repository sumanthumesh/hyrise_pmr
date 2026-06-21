#include "mem_manager.hpp"

#include <algorithm>
#include <cstddef>
#include <numa.h>
#include <stdexcept>

#include "memory/default_memory_resource.hpp"
#include "memory/runtime_exec_resource.hpp"
#include "memory/table_segment_gen_resource.hpp"

namespace hyrise
{

MemManager::MemManager(AllocationStrategy strategy)
    : _strategy(strategy), _invalid_resource_ptr(std::make_shared<InvalidMemResource>())
{
    // No pools by default; user must call add_pool() to create them
    // add_pool(16ull * 1024 * 1024 * 1024, 0);  // Default pool: 16GB on NUMA node 0
    // add_pool(16ull * 1024 * 1024 * 1024, 0);  // Default pool: 16GB on NUMA node 0
    // add_pool(16ull * 1024 * 1024 * 1024, 0);  // Default pool: 16GB on NUMA node 0
    set_strategy(strategy);
}

size_t MemManager::add_pool(std::size_t size_bytes, int numa_node, size_t pool_id, bool serialize)
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};

    auto numa_pool = std::make_shared<NumaMonotonicResource>(size_bytes, numa_node, serialize);

    Assertf(_pools.find(pool_id) == _pools.end(),
            "Pool ID %lu already exists. Choose a different pool ID.\n", pool_id);

    _pools[pool_id] = numa_pool;
    _pool_numa_nodes[pool_id] = numa_node;

    return pool_id; // Return the stable pool ID
}

std::shared_ptr<NumaMonotonicResource> MemManager::get_pool(size_t pool_id) const
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};

    auto it = _pools.find(pool_id);
    if (it == _pools.end())
    {
        throw std::out_of_range("Pool ID " + std::to_string(pool_id) + " does not exist.");
    }

    return it->second;
}

size_t MemManager::pool_count() const
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};
    return _pools.size();
}

void MemManager::set_strategy(AllocationStrategy strategy)
{

    // Need to set strategy and the corresponding memory resources
    _strategy = strategy;
    switch (strategy)
    {
    case AllocationStrategy::Heap:
        /**
         * The default allocation policy on hyrise
         * use the DefaultResource for all pmr allocations
         * Ideally only TableSegmentGen needs to be explicitly specified since we mentioned it a lot in encoders and binary parser
         */
        memory_resources.TableSegmentGen = &DefaultResource::get();
        memory_resources.MiscGen = _invalid_resource_ptr.get();
        memory_resources.MiscExecution = _invalid_resource_ptr.get();
        std::pmr::set_default_resource(&DefaultResource::get());
        break;
    case AllocationStrategy::TableGen:
        /**
         * In this strategy, all table generation allocations go to a single local NUMA pool
         * Any miscellaneous allocation will be to pool 1
         */
        Assertf(exists(0), "Pool with ID 0 for allocating table segments during table generation does not exist.\n");
        Assertf(exists(1), "Pool with ID 1 for allocating miscellaneous data during table generation does not exist.\n");
        memory_resources.TableSegmentGen = get_pool(0).get();         // Use first pool for table generation
        memory_resources.MiscGen = get_pool(1).get();                 // Use second pool for misc table gen allocations
        memory_resources.MiscExecution = _invalid_resource_ptr.get(); // Shouldn't be used if still in table generation
        std::pmr::set_default_resource(memory_resources.MiscGen); // Default to misc gen pool for any allocation that doesn't explicitly specify
        break;
    case AllocationStrategy::Local:
        /**
         * In this strategy, table generation has already happened. So table generation resources are not needed.
         * All misc allocations go to the local NUMA pool
         * The other resources should not be used
         */
        memory_resources.TableSegmentGen = _invalid_resource_ptr.get(); // Table generation is done, should be invalid
        memory_resources.MiscGen = _invalid_resource_ptr.get();         // Table generation is done, should be invalid
        Assertf(exists(2), "Pool with ID 2 for allocating local execution data does not exist.\n");
        memory_resources.MiscExecution = get_pool(2).get(); // Use third pool for local heap allocations
        std::pmr::set_default_resource(memory_resources.MiscExecution); // Default to local execution pool for any allocation that doesn't explicitly specify
        break;
    case AllocationStrategy::Remote:
        /**
         * In this strategy, table generation has already happened. So table generation resources are not needed.
         * All misc allocations go to the remote NUMA pool
         * The other resources should not be used
         */
        memory_resources.TableSegmentGen = _invalid_resource_ptr.get(); // Table generation is done, should be invalid
        memory_resources.MiscGen = _invalid_resource_ptr.get();         // Table generation is done, should be invalid
        Assertf(exists(3), "Pool with ID 3 for allocating remote execution data does not exist.\n");
        memory_resources.MiscExecution = get_pool(3).get(); // Use third pool for local heap allocations
        std::pmr::set_default_resource(memory_resources.MiscExecution); // Default to remote execution pool for any allocation that doesn't explicitly specify
        break;
    case AllocationStrategy::Greedy:
        /**
         * In this strategy, table generation has already happened. So table generation resources are not needed.
         * All misc allocations either goto local or remote based on greedy approach
         */
        Assertf(exists(2), "Pool with ID 2 for allocating local execution data does not exist.\n");
        Assertf(exists(3), "Pool with ID 3 for allocating remote execution data does not exist.\n");
        memory_resources.TableSegmentGen = _invalid_resource_ptr.get(); // Table generation is done, should be invalid
        memory_resources.MiscGen = _invalid_resource_ptr.get();         // Table generation is done, should be invalid
        memory_resources.MiscExecution = this;                          // Use third pool for local heap allocations
        std::pmr::set_default_resource(this);
        break;
    }
}

bool MemManager::exists(size_t pool_id)
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};

    return _pools.find(pool_id) != _pools.end();
}

void MemManager::destroy_pool(size_t pool_id)
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};

    auto it = _pools.find(pool_id);
    if (it == _pools.end())
    {
        throw std::out_of_range("Pool ID " + std::to_string(pool_id) + " does not exist.");
    }

    // Get pool info before destroying for logging
    auto pool_ptr = it->second;
    auto numa_it = _pool_numa_nodes.find(pool_id);
    int numa_node = (numa_it != _pool_numa_nodes.end()) ? numa_it->second : -1;
    size_t pool_size = pool_ptr->size();
    size_t allocated_bytes = pool_ptr->allocated_bytes();

    // Remove pool from maps - this triggers the NumaMonotonicResource destructor
    // which calls numa_free and returns pages to the OS
    _pools.erase(it);
    _pool_numa_nodes.erase(numa_it);

    std::cout << "Destroyed pool " << pool_id << " on NUMA node " << numa_node
              << ": freed " << pool_size << " bytes (had " << allocated_bytes
              << " bytes allocated). Pages returned to OS.\n";
}

void *MemManager::do_allocate(std::size_t bytes, std::size_t alignment)
{

    // Should never reach here. All allocations should go through the specific memory resources (e.g. memory_resources.MiscExecution) that are set to point to specific pools or the default resource based on the strategy. If we do reach here, it means something is trying to allocate on the MemManager directly instead of a specific pool or the default resource, which is not intended.
    // Exit by printing an error
    std::cerr << "Direct allocation on MemManager is not allowed.\n";
    throw std::bad_alloc();

    // auto lock = std::lock_guard<std::mutex>{_pools_mutex};

    // if (_pools.empty() && _strategy != AllocationStrategy::Heap)
    // {
    //     std::cerr << "No NUMA pools available for allocation\n";
    //     std::cerr << print_backtrace() << std::endl;
    //     throw std::bad_alloc();
    // }
    // if (_strategy == AllocationStrategy::TableGen)
    // {
    //     // Allocate on the first pool only (used during table generation)
    //     try
    //     {
    //         // std::printf("TG,%lu\n", bytes);
    //         return _pools.find(1)->second->allocate(bytes, alignment);
    //     }
    //     catch (const std::bad_alloc &)
    //     {
    //         std::cerr << "Tried allocating on table generation pool but it is full\n";
    //         throw; // Pool full
    //     }
    // }
    // else if (_strategy == AllocationStrategy::Local)
    // {
    //     // Allocate on local pool (first pool in map)
    //     try
    //     {
    //         // std::printf("LL,%lu\n", bytes);
    //         return _pools.find(2)->second->allocate(bytes, alignment);
    //     }
    //     catch (const std::bad_alloc &)
    //     {
    //         std::cerr << "Tried allocating on local pool but it is full\n";
    //         throw; // Local pool full
    //     }
    // }
    // else if (_strategy == AllocationStrategy::Remote)
    // {
    //     try
    //     {
    //         // std::printf("RR,%lu\n", bytes);
    //         return _pools.find(3)->second->allocate(bytes, alignment);
    //     }
    //     catch (const std::bad_alloc &)
    //     {
    //         std::cerr << "Tried allocating on remote pool but it is full\n";
    //         throw; // Remote pool full
    //     }
    // }
    // else if (_strategy == AllocationStrategy::Greedy)
    // {
    //     // Fetch NUMA pools
    //     const auto &local_pool = _pools.find(2)->second;
    //     const auto &remote_pool = _pools.find(3)->second;

    //     // Current memory usage check before allocation
    //     std::pair<size_t, size_t> current_usage = quick_size_check();
    //     // std::printf("Current usage before allocation: Local=%lu bytes, Remote=%lu bytes\n", current_usage.first, current_usage.second);

    //     // Try local first, then remote
    //     if (current_usage.first + bytes <= _local_mem_capacity_bytes)
    //     {
    //         // std::printf("GL,%lu\n", bytes);
    //         return local_pool->allocate(bytes, alignment);
    //     }
    //     else if (current_usage.second + bytes <= _remote_mem_capacity_bytes)
    //     {
    //         // std::printf("GR,%lu\n", bytes);
    //         return remote_pool->allocate(bytes, alignment);
    //     }
    //     else
    //     {
    //         std::cerr << "Allocating failed on both pools\n";
    //         throw std::bad_alloc(); // Both pools full
    //     }
    // }
    // // else if (_strategy == AllocationStrategy::Heap)
    // // {
    // //     // std::printf("HP,%lu\n", bytes);
    // //     return DefaultResource::get().allocate(bytes, alignment);
    // // }
    // else
    // {
    //     std::cerr << "Allocating failed on all strategies\n";
    //     throw std::bad_alloc(); // Unknown strategy
    // }
}

std::pair<size_t, size_t> MemManager::quick_size_check() const
{
    // No locking this for now. It's just a quick check and doesn't need to be perfectly accurate. If we find inconsistencies we can add locking later.

    size_t local_allocated = 0, remote_allocated = 0;

    // This is the mem manager's view of allocated bytes by NUMA node based on the pools it manages. This is not necessarily perfectly accurate since there is no locking, but should be good enough for a quick check.
    for (const auto &[pool_id, pool] : _pools)
    {
        switch (pool->numa_node())
        {
        case 0:
            local_allocated += pool->allocated_bytes();
            break;
        case 1:
            remote_allocated += pool->allocated_bytes();
            break;
        default:
            std::printf("Unexpected NUMA node %lu for pool %lu in quick size check. Ignoring this pool for the quick size check.\n", pool->numa_node(), pool_id);
            exit(EXIT_FAILURE);
        }
    }

    // This is the migrated status that the MigrationEngine is tracking. This is updated when we call mem_usage from _hshell and should be accurate since it's updated with locking in the MigrationEngine.
    // Look up each node by key independently — the previous size-based dispatch crashed when
    // only the remote node was populated (find(0) returned end() and we dereferenced it).
    if (auto it = _migrated_status.find(0); it != _migrated_status.end())
    {
        local_allocated += it->second.allocated_bytes;
    }
    if (auto it = _migrated_status.find(1); it != _migrated_status.end())
    {
        remote_allocated += it->second.allocated_bytes;
    }
    if (_migrated_status.size() > 2)
    {
        std::printf("Expected 0, 1 or 2 NUMA nodes in migrated status, but found %lu. Extra nodes ignored.\n",
                    _migrated_status.size());
    }

    return std::make_pair(local_allocated, remote_allocated);
}

void MemManager::do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment)
{

    // Should never reach here. All deallocations should go through the specific memory resources (e.g. memory_resources.MiscExecution) that are set to point to specific pools or the default resource based on the strategy. If we do reach here, it means something is trying to deallocate on the MemManager directly instead of a specific pool or the default resource, which is not intended.
     // Exit by printing an error
    std::cerr << "Direct deallocation on MemManager is not allowed.\n";
    throw std::bad_alloc();

    // if (!pointer)
    // {
    //     return;
    // }

    // auto lock = std::lock_guard<std::mutex>{_pools_mutex};

    // // Find which pool owns this pointer
    // int pool_idx = _find_pool_for_pointer(pointer);

    // if (pool_idx >= 0)
    // {
    //     _pools[pool_idx]->deallocate(pointer, bytes, alignment);
    // }
    // else
    // {
    //     std::cerr << "Could not find pool for this pointer\n";
    //     exit(EXIT_FAILURE);
    // }
    // If pointer not found in any pool, silently ignore (could be from default allocator)
    // or you could log a warning if needed
}

bool MemManager::do_is_equal(const MemoryResource &other) const noexcept
{
    return &other == this;
}

int MemManager::_find_pool_for_pointer(void *p) const
{
    // Linear search through pools to find which one owns the pointer
    // Each pool has a start and end address
    for (const auto &[pool_id, pool] : _pools)
    {
        uintptr_t ptr_addr = reinterpret_cast<uintptr_t>(p);
        uintptr_t pool_start = pool->start_address();
        uintptr_t pool_end = pool->end_address();

        if (ptr_addr >= pool_start && ptr_addr < pool_end)
        {
            return static_cast<int>(pool_id);
        }
    }

    return -1; // Not found in any pool
}

void MemManager::print_status() const
{
    std::unordered_map<size_t, size_t> size_by_numa;

    std::cout << "Default Pools:\n";
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

std::vector<MemResourceStatus> MemManager::all_pool_status() const
{
    std::vector<MemResourceStatus> statuses;

    auto lock = std::lock_guard<std::mutex>{_pools_mutex};

    for (const auto &[pool_id, pool] : _pools)
    {
        auto s = pool->status();
        s.resource_id = pool_id;
        statuses.push_back(s);
    }

    return statuses;
}

std::unordered_map<int, MemResourceStatus> &MemManager::aggregate_manager_status()
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};

    _aggregate_manager_status.clear();

    for (const auto &[pool_id, pool] : _pools)
    {
        auto status = pool->status();
        if (_aggregate_manager_status.find(status.numa_node) == _aggregate_manager_status.end())
        {
            _aggregate_manager_status[status.numa_node] = MemResourceStatus{};
            _aggregate_manager_status[status.numa_node].description = "MemManagerAggregate";
            _aggregate_manager_status[status.numa_node].numa_node = status.numa_node;
        }

        _aggregate_manager_status[status.numa_node].capacity_bytes += status.capacity_bytes;
        _aggregate_manager_status[status.numa_node].allocated_bytes += status.allocated_bytes;
        _aggregate_manager_status[status.numa_node].peak_allocated_bytes += status.peak_allocated_bytes;
    }
    return _aggregate_manager_status;
}

void MemManager::update_migrated_status(std::unordered_map<int, MemResourceStatus> &migrated_status)
{
    _migrated_status = migrated_status;
}

void MemManager::set_numa_node_capacities(size_t local_capacity_bytes, size_t remote_capacity_bytes)
{
    _local_mem_capacity_bytes = local_capacity_bytes;
    _remote_mem_capacity_bytes = remote_capacity_bytes;
}

// Defined here so that base_segment_encoder.hpp (and the four encoder headers downstream
// of it) can fetch the TableSegmentGen resource without including mem_manager.hpp.
std::pmr::memory_resource *get_table_segment_gen_resource()
{
    return MemManager::get().memory_resources.TableSegmentGen;
}

// Defined here so that operator headers (e.g., join_hash_steps.hpp) can fetch the
// runtime exec resource at construction time without pulling in mem_manager.hpp.
std::pmr::memory_resource *runtime_exec_resource()
{
    return MemManager::get().pick_runtime_exec_resource();
}

std::pmr::memory_resource *MemManager::pick_runtime_exec_resource() const
{
    switch (_strategy)
    {
    case AllocationStrategy::Local:
    {
        auto it = _pools.find(3);
        Assertf(it != _pools.end(),
                "Local strategy: pool 2 (local execution) does not exist.\n");
        return it->second.get();
    }
    case AllocationStrategy::Remote:
    {
        auto it = _pools.find(3);
        Assertf(it != _pools.end(),
                "Remote strategy: pool 3 (remote execution) does not exist.\n");
        return it->second.get();
    }
    case AllocationStrategy::Greedy:
    {
        // Per-call decision: prefer local, fall back to remote when local is full.
        // Capacity check uses quick_size_check() which counts both manager pools and
        // migrated columns toward each node's allocated bytes.
        const auto usage = quick_size_check();
        auto local_it = _pools.find(2);
        auto remote_it = _pools.find(3);
        Assertf(local_it != _pools.end() && remote_it != _pools.end(),
                "Greedy strategy: pools 2 and 3 must both exist.\n");

        if (usage.first < _local_mem_capacity_bytes)
        {
            return local_it->second.get();
        }
        if (usage.second < _remote_mem_capacity_bytes)
        {
            return remote_it->second.get();
        }
        // Both pools at capacity — hard-stop via the invalid resource so the caller surfaces
        // the condition immediately rather than overcommitting either pool.
        std::cerr << "Greedy pick_runtime_exec_resource: both local and remote pools at capacity\n";
        return _invalid_resource_ptr.get();
    }
    case AllocationStrategy::Heap:
        return &DefaultResource::get();
    case AllocationStrategy::TableGen:
    default:
        // Runtime execution allocations shouldn't happen during table generation; surface this
        // immediately rather than silently routing to a pool that isn't the caller's intent.
        return _invalid_resource_ptr.get();
    }
}
} // namespace hyrise
