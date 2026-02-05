#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "storage/mem_pool.hpp"
#include "types.hpp"
#include "utils/singleton.hpp"

namespace hyrise
{

class InvalidMemResource : public MemoryResource
{
  public:
  protected:
    void *do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        std::cerr << "Tried to allocate on InvalidMemoryResouce\n";
        exit(EXIT_FAILURE);
    }

    void do_deallocate(void *p, std::size_t bytes, std::size_t alignment) override
    {
        std::cerr << "Tried to deallocate on InvalidMemoryResouce\n";
        exit(EXIT_FAILURE);
    }

    bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override
    {
        std::cerr << "Tried to do equality check InvalidMemoryResouce\n";
        exit(EXIT_FAILURE);
    }
};
/**
 * MemManager is a polymorphic memory resource that manages multiple NUMA-backed memory pools.
 * It allows allocation to be distributed across multiple NUMA nodes and can be set as the
 * default std::pmr::memory_resource to automatically use NUMA-aware allocation for PMR containers.
 *
 * Usage:
 *   auto &mem_mgr = MemManager::get();
 *   mem_mgr.add_pool(pool_size_bytes, numa_node_id);  // Create pools for multiple nodes
 *   mem_mgr.add_pool(pool_size_bytes, another_node);
 *   std::pmr::set_default_resource(&mem_mgr);
 *   // Now all pmr containers will use the MemManager for allocation
 */
class MemManager : public MemoryResource, public Singleton<MemManager>
{
  public:
    // Allocation strategy enum to control how allocations are distributed
    enum class AllocationStrategy
    {
        Heap,     // Fallback to heap allocation
        TableGen, // Use this only while generating tables
        Local,    // Allocate on local node
        Remote,   // Allocate on remote node
        Greedy,   // Try local first, then remote
    };

    MemManager(AllocationStrategy strategy = AllocationStrategy::Heap);

    /**
     * Add a NUMA pool to this MemManager.
     * Pools are managed internally and kept alive as long as MemManager exists.
     * @param size_bytes The size of the pool buffer
     * @param numa_node The NUMA node ID to allocate on
     * @return pool_id that can be used to reference this pool later
     */
    size_t add_pool(std::size_t size_bytes, int numa_node, size_t pool_id);

    /**
     * Get a reference to a specific pool by its ID (for introspection)
     */
    std::shared_ptr<NumaMonotonicResource> get_pool(size_t pool_id) const;

    /**
     * Returns total number of pools managed
     */
    size_t pool_count() const;

    /**
     * Set the allocation strategy for how allocations are distributed
     */
    void set_strategy(AllocationStrategy strategy);

    /**
     * Destroy a pool by its ID. This releases all NUMA memory back to the OS via numa_free.
     * The pool is removed from the manager, triggering the NumaMonotonicResource destructor.
     * @param pool_id The ID of the pool to destroy
     * @throws std::out_of_range if pool_id does not exist
     */
    void destroy_pool(size_t pool_id);

    /**
     * Returns true if a pool with this id exists
     */
    bool exists(size_t pool_id);

    void print_status() const;

    /**
     * PMR interface implementation
     */
    void *do_allocate(std::size_t bytes, std::size_t alignment) override;
    void do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment) override;
    [[nodiscard]] bool do_is_equal(const MemoryResource &other) const noexcept override;

    /**
     * Memory resources used during hyrise execution
     * These are placeholders. We will need to assign NUMA pools or default resources to these as needed.
     * There will be other resources maintained by the MigrationEngine for migrating data between NUMA nodes.
     */
    struct AvailableMemoryResources
    {
        std::pmr::memory_resource *TableSegmentGen = nullptr; // For allocating table segments during generation
        std::pmr::memory_resource *MiscGen = nullptr;         // For all table generation misc. allocations (chunk objects, metadata, etc.)
        std::pmr::memory_resource *MiscExecution = nullptr;   // For all misc allocations
    } memory_resources;

  private:
    // Helper to find which pool owns a pointer
    int _find_pool_for_pointer(void *p) const;

    // Pool storage and metadata - use maps to keep pool IDs stable even after deletion
    std::unordered_map<size_t, std::shared_ptr<NumaMonotonicResource>> _pools; // pool_id -> pool
    std::unordered_map<size_t, int> _pool_numa_nodes;                          // pool_id -> NUMA node
    mutable std::mutex _pools_mutex;                                           // protect access to maps

    AllocationStrategy _strategy;

    std::shared_ptr<InvalidMemResource> _invalid_resource_ptr;
};

} // namespace hyrise