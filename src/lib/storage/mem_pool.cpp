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

void MemPoolManager::create_pool(const std::string &pool_name, uint64_t size, int numa_node)
{
    // Check if pool already exists
    if (_pools.find(pool_name) != _pools.end())
    {
        std::cerr << "Pool with name " << pool_name << " already exists\n";
        exit(2);
    }
    auto mem_pool_ptr = std::make_shared<NumaMonotonicResource>(size, numa_node);
    _pools.insert(std::make_pair(pool_name, mem_pool_ptr));
    _unique_pool_id++;
}

std::shared_ptr<NumaMonotonicResource> MemPoolManager::get_pool(const std::string &pool_name)
{
    if (_pools.find(pool_name) == _pools.end())
    {
        std::cerr << "Pool with name " << pool_name << " not found\n";
        exit(2);
    }
    return _pools.find(pool_name)->second;
}