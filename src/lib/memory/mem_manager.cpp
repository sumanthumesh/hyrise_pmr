#include "mem_manager.hpp"

#include <algorithm>
#include <cstddef>
#include <numa.h>
#include <stdexcept>

namespace hyrise
{

MemManager::MemManager(AllocationStrategy strategy)
    : _strategy(strategy)
{
    // No pools by default; user must call add_pool() to create them
    add_pool(16ull * 1024 * 1024 * 1024, 0);  // Default pool: 16GB on NUMA node 0
    add_pool(16ull * 1024 * 1024 * 1024, 1);  // Default pool: 16GB on NUMA node 1
}

size_t MemManager::add_pool(std::size_t size_bytes, int numa_node)
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};
    
    auto numa_pool = std::make_shared<NumaMonotonicResource>(size_bytes, numa_node);
    _pools.emplace_back(numa_pool);
    _pool_numa_nodes.emplace_back(numa_node);
    
    return _pools.size() - 1;  // Return the pool ID (index)
}

std::shared_ptr<NumaMonotonicResource> MemManager::get_pool(size_t pool_id) const
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};
    
    if (pool_id >= _pools.size())
    {
        throw std::out_of_range("Pool ID " + std::to_string(pool_id) + " does not exist.");
    }
    
    return _pools[pool_id];
}

size_t MemManager::pool_count() const
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};
    return _pools.size();
}

void MemManager::set_strategy(AllocationStrategy strategy)
{
    _strategy = strategy;
}

void *MemManager::do_allocate(std::size_t bytes, std::size_t alignment)
{
    auto lock = std::lock_guard<std::mutex>{_pools_mutex};
    
    if (_pools.empty())
    {
        std::cerr << "No NUMA pools available for allocation\n";
        throw std::bad_alloc();
    }

    if (_strategy == AllocationStrategy::Local)
    {
        // Allocate on local pool (first pool in list)
        try
        {
            return _pools[0]->allocate(bytes, alignment);
        }
        catch (const std::bad_alloc &)
        {
            std::cerr << "Tried alllocating on local pool but it is full\n";
            throw;  // Local pool full
        }
    }
    else if (_strategy == AllocationStrategy::Remote)
    {
        // Allocate on remote pool (second pool in list, if exists)
        if (_pools.size() < 2)
        {
            std::cerr << "Remote pool doesn't exist\n";
            throw std::bad_alloc();  // No remote pool available
        }
        try
        {
            return _pools[1]->allocate(bytes, alignment);
        }
        catch (const std::bad_alloc &)
        {
            std::cerr << "Tried alllocating on remote pool but it is full\n";
            throw;  // Remote pool full
        }
    }
    else if (_strategy == AllocationStrategy::Greedy)
    {
        // Fetch NUMA pools
        const auto& local_pool = _pools[0];
        const auto & remote_pool = (_pools.size() > 1) ? _pools[1] : nullptr;
        
        // Try local first, then remote
        if (local_pool->allocated_bytes() + bytes <= local_pool->size())
        {
            return local_pool->allocate(bytes, alignment);
        }
        else if (remote_pool && (remote_pool->allocated_bytes() + bytes <= remote_pool->size()))
        {
            return remote_pool->allocate(bytes, alignment);
        }
        else
        {
            std::cerr << "Allocating failed on both pools\n";
            throw std::bad_alloc();  // Both pools full
        }
    }
    throw std::bad_alloc();  // Unknown strategy
}

void MemManager::do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment)
{
    if (!pointer)
    {
        return;
    }

    auto lock = std::lock_guard<std::mutex>{_pools_mutex};
    
    // Find which pool owns this pointer
    int pool_idx = _find_pool_for_pointer(pointer);
    
    if (pool_idx >= 0 && pool_idx < static_cast<int>(_pools.size()))
    {
        _pools[pool_idx]->deallocate(pointer, bytes, alignment);
    }
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
    for (size_t i = 0; i < _pools.size(); ++i)
    {
        const auto &pool = _pools[i];
        uintptr_t ptr_addr = reinterpret_cast<uintptr_t>(p);
        uintptr_t pool_start = pool->start_address();
        uintptr_t pool_end = pool->end_address();
        
        if (ptr_addr >= pool_start && ptr_addr < pool_end)
        {
            return static_cast<int>(i);
        }
    }
    
    return -1;  // Not found in any pool
}

} // namespace hyrise
