#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "memory/make_on.hpp"
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
        PartialTableGen, // Use this to allocate table data on Heap but other misc data during generation on NUMA pools
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
    size_t add_pool(std::size_t size_bytes, int numa_node, size_t pool_id, bool serialize = false);

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
    std::vector<MemResourceStatus> all_pool_status() const;
    std::unordered_map<int, MemResourceStatus>& aggregate_manager_status();

    /**
     * A very quick total of local and remote node allocated bytes based on the manager's view of its pools and the migrated status from the MigrationEngine. This is not perfectly accurate since there is no locking, but should be good enough for a quick check to see if we're in the right ballpark before doing more expensive calls to get detailed status.
     */
    std::pair<size_t,size_t> quick_size_check() const;

    void update_migrated_status(std::unordered_map<int, MemResourceStatus>& migrated_status);

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

    /**
     * Interface to set memory capacities for local and remote NUMA nodes. This is used by the MigrationEngine to inform the MemManager of the total capacities of local and remote memory based on the pools it has created for migration.
     */
    void set_numa_node_capacities(size_t local_capacity_bytes, size_t remote_capacity_bytes);

    /**
     * Pick the memory_resource that should back "large, footprint-tracked" runtime execution
     * allocations — pos lists, reference segments, hash tables, sort buffers, etc. The choice
     * is driven by the current allocation strategy:
     *   - Local   : pool 2 (local execution pool, NUMA node 0)
     *   - Remote  : pool 3 (remote execution pool, NUMA node 1)
     *   - Greedy  : pool 2 if local has headroom, else pool 3 (decided per call)
     *   - Heap    : the default resource (heap)
     *   - TableGen: invalid — runtime execution allocations are not expected in this strategy
     *
     * This is the function call sites should use when constructing structures we want to
     * track and potentially migrate (see ReferenceSegment::make_on, RowIDPosList::make_on,
     * and similar factories).
     */
    std::pmr::memory_resource *pick_runtime_exec_resource() const;

  private:
    // Helper to find which pool owns a pointer
    int _find_pool_for_pointer(void *p) const;

    // Pool storage and metadata - use maps to keep pool IDs stable even after deletion
    std::unordered_map<size_t, std::shared_ptr<NumaMonotonicResource>> _pools; // pool_id -> pool
    std::unordered_map<size_t, int> _pool_numa_nodes;                          // pool_id -> NUMA node
    mutable std::mutex _pools_mutex;                                           // protect access to maps

    AllocationStrategy _strategy;

    std::shared_ptr<InvalidMemResource> _invalid_resource_ptr;
    std::unordered_map<int, MemResourceStatus> _aggregate_manager_status;
    std::unordered_map<int, MemResourceStatus> _migrated_status;

    // Sizes of local and remote memories
    size_t _local_mem_capacity_bytes{1ull*1024*1024*1024}; //1GB default local memory capacity
    size_t _remote_mem_capacity_bytes{1ull*1024*1024*1024}; //1GB default remote memory capacity

    // Did we use the local pool for most recent allocation?
    mutable bool _last_allocation_used_local_pool{false};
};

// make_on<T>(resource, args...) is declared in memory/make_on.hpp (included above).
// It used to live here, but was extracted to a thin header so storage headers can
// pick it up without pulling in the full MemManager declaration. Anyone including
// mem_manager.hpp still gets make_on transitively.

} // namespace hyrise