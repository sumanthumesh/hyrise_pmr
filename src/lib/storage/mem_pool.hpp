#pragma once

#include "numaif.h"
#include <cassert>
#include <cerrno>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <numa.h>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "utils/debug_util.hpp"

void *allocate_on_numa_node(std::size_t bytes, int node);

// Simple RAII wrapper for a NUMA-backed monotonic_buffer_resource
class NumaMonotonicResource : public std::pmr::memory_resource
{
  public:
    NumaMonotonicResource(std::size_t size_bytes, int numa_node,
                          std::pmr::memory_resource *upstream = std::pmr::null_memory_resource())
        : _size(size_bytes),
          _numa_node(numa_node),
          _buffer(allocate_on_numa_node(size_bytes, numa_node)),
          //   _upstream(upstream),
          _mono(_buffer, _size, upstream)
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
        return reinterpret_cast<std::uintptr_t>(_buffer) + _size;
    }

    std::size_t size() const
    {
        return _size;
    }

    size_t allocated_bytes() const
    {
        return _allocated_bytes;
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

  protected:
    void *do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        _allocated_bytes += bytes;
        return _mono.allocate(bytes, alignment);
    }

    void do_deallocate(void *p, std::size_t bytes, std::size_t alignment) override
    {
        _allocated_bytes -= bytes;
        _mono.deallocate(p, bytes, alignment);
    }

    bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override
    {
        return this == &other;
    }

  private:
    std::size_t _size{};
    int _numa_node{-1};
    void *_buffer{};
    // std::pmr::memory_resource* _upstream;
    std::pmr::monotonic_buffer_resource _mono;
    size_t _allocated_bytes{0};
};

namespace hyrise
{
class MemPoolManager
{
  public:
    size_t create_pool(uint64_t size, int numa_node);
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
    
    private:
    size_t unique_pool_id()
    {
        return _unique_pool_id++;
    }
    
    std::unordered_map<size_t, std::shared_ptr<NumaMonotonicResource>> _pools;
    size_t _unique_pool_id{0};
};
} // namespace hyrise