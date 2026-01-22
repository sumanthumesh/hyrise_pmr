#include "mem_pool.hpp"

#include <cassert>
#include <iostream>
#include <memory_resource>
#include <numa.h>
#include "numaif.h"
#include <vector>
#include <cerrno>
#include <stdexcept>

using namespace hyrise;

// Allocate raw memory on a given NUMA node
void *allocate_on_numa_node(std::size_t bytes, int node)
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

    void *ptr = numa_alloc_onnode(bytes, node);
    if (!ptr)
    {
        std::cerr << "Failed to allocate " << bytes << " bytes on node " << node << "\n";
        std::abort();
    }
    return ptr;
}

size_t MemPoolManager::create_pool(uint64_t size, int numa_node)
{
    auto mem_pool_ptr = std::make_shared<NumaMonotonicResource>(size, numa_node);
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