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

    std::uintptr_t size() const
    {
        return _size;
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
        return _mono.allocate(bytes, alignment);
    }

    void do_deallocate(void *p, std::size_t bytes, std::size_t alignment) override
    {
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
};


namespace hyrise
{
class MemPoolManager
{
  public:
    void create_pool(const std::string &pool_name, uint64_t size, int numa_node);
    std::shared_ptr<NumaMonotonicResource> get_pool(const std::string &pool_name);

  private:
    std::unordered_map<std::string, std::shared_ptr<NumaMonotonicResource>> _pools;
};
} // namespace hyrise