#include "storage/migration.hpp"

#include <algorithm>
#include <atomic>
#include <deque>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

namespace hyrise
{

bool MigrationEngine::print_migration_stats = false;
size_t MigrationEngine::migration_threads = 0;

namespace
{
// Empirically, 16 threads already saturates the CXL copy bandwidth (~18 GB/s practical peak).
constexpr size_t DEFAULT_MAX_MIGRATION_THREADS = 16;

size_t resolve_thread_count(size_t work_items, size_t configured)
{
    if (work_items == 0)
    {
        return 0;
    }
    auto thread_count = configured;
    if (thread_count == 0)
    {
        thread_count = std::min(static_cast<size_t>(std::max(1u, std::thread::hardware_concurrency())),
                                DEFAULT_MAX_MIGRATION_THREADS);
    }
    return std::min(thread_count, work_items);
}
} // namespace

void MigrationEngine::migrate_segment(std::shared_ptr<Chunk> &chunk_ptr, std::shared_ptr<AbstractSegment> &segment_ptr, ColumnID column_id, std::shared_ptr<NumaMonotonicResource> &memory_resource)
{
    if (std::dynamic_pointer_cast<AbstractEncodedSegment>(segment_ptr))
    {
        // Dictionary Segment
        switch (segment_ptr->data_type())
        {
        case DataType::Int:
            migrate_numerical_dictionary_segment<int32_t>(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        case DataType::Long:
            migrate_numerical_dictionary_segment<int64_t>(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        case DataType::Float:
            migrate_numerical_dictionary_segment<float>(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        case DataType::Double:
            migrate_numerical_dictionary_segment<double>(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        case DataType::String:
            migrate_string_dictionary_segment(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        default:
            Fail("Unsupported data type for dictionary segment migration");
        }
    }
    else if (std::dynamic_pointer_cast<BaseValueSegment>(segment_ptr))
    {
        // Value Segment
        switch (segment_ptr->data_type())
        {
        case DataType::Int:
            migrate_numerical_value_segment<int32_t>(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        case DataType::Long:
            migrate_numerical_value_segment<int64_t>(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        case DataType::Float:
            migrate_numerical_value_segment<float>(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        case DataType::Double:
            migrate_numerical_value_segment<double>(chunk_ptr, segment_ptr, column_id, memory_resource);
            break;
        default:
            Fail("Unsupported data type for value segment migration");
        }
    }
    else
    {
        Fail("Unsupported segment type for migration");
    }
}

std::vector<ChunkID> MigrationEngine::migrate_chunks(std::shared_ptr<Table> &table, ColumnID column_id,
                                                    const std::vector<ChunkID> &chunk_ids,
                                                    std::shared_ptr<NumaMonotonicResource> &memory_resource)
{
    auto failed_chunk_ids = std::vector<ChunkID>{};
    const auto thread_count = resolve_thread_count(chunk_ids.size(), migration_threads);
    if (thread_count == 0)
    {
        return failed_chunk_ids;
    }

    // Chunks are handed out dynamically because segments differ wildly in size (a string dictionary
    // costs far more to copy than a fixed-width one), so a static split would leave threads idle.
    auto next_index = std::atomic<size_t>{0};
    auto failed_mutex = std::mutex{};

    const auto worker = [&] {
        for (;;)
        {
            const auto index = next_index.fetch_add(1, std::memory_order_relaxed);
            if (index >= chunk_ids.size())
            {
                return;
            }
            const auto chunk_id = chunk_ids[index];

            auto chunk_ptr = table->get_chunk(chunk_id);
            auto segment_ptr = chunk_ptr->get_segment(column_id);
            try
            {
                migrate_segment(chunk_ptr, segment_ptr, column_id, memory_resource);
            }
            catch (const std::bad_alloc &)
            {
                // The pool ran out. The segment is untouched (Chunk::replace_segment is the last
                // step and is atomic), so the caller can retry it against a fresh pool. Whatever
                // this thread already bumped off the exhausted pool is lost, exactly as in the
                // former serial implementation.
                auto lock = std::lock_guard<std::mutex>{failed_mutex};
                failed_chunk_ids.push_back(chunk_id);
            }
        }
    };

    auto threads = std::vector<std::thread>{};
    threads.reserve(thread_count - 1);
    for (auto thread_id = size_t{1}; thread_id < thread_count; ++thread_id)
    {
        threads.emplace_back(worker);
    }
    worker(); // The calling thread pulls its share too.
    for (auto &thread : threads)
    {
        thread.join();
    }

    // Threads finish out of order; keep retries in chunk order so behaviour stays reproducible.
    std::sort(failed_chunk_ids.begin(), failed_chunk_ids.end());
    return failed_chunk_ids;
}

void MigrationEngine::migrate_column(std::shared_ptr<Table> &table_name, const std::string &column_name, int numa_node_id)
{
    // Skip if the column already lives in a dedicated pool on the requested node.
    // Every pool for a column is allocated with the same numa_node_id in this function,
    // so checking the front pool is sufficient. First-time migrations are not covered
    // here (unmigrated columns aren't in _columns_to_pools_mapping) and must proceed.
    if (const auto it = _columns_to_pools_mapping.find(column_name);
        it != _columns_to_pools_mapping.end() && !it->second.empty())
    {
        const auto current_node = _pool_manager.get_pool(it->second.front())->status().numa_node;
        if (current_node == numa_node_id)
        {
            std::cout<<"Skipped migration of column " << column_name << " to NUMA node " << numa_node_id << "\n";
            return;
        }
    }

    ColumnID column_id = table_name->column_id_by_name(column_name);

    // Size every segment once, up front. memory_usage(Full) walks each string of a string
    // dictionary, so calling it per segment inside the migration loop (as this used to) doubled
    // that scan and charged it to the migration timer.
    const auto chunk_count = table_name->chunk_count();
    auto segment_sizes = std::vector<size_t>(chunk_count, 0); // indexed by ChunkID
    auto pending_chunk_ids = std::vector<ChunkID>{};
    pending_chunk_ids.reserve(chunk_count);

    size_t column_size = 0;
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id)
    {
        const auto segment_size =
            table_name->get_chunk(chunk_id)->get_segment(column_id)->memory_usage(MemoryUsageCalculationMode::Full);
        segment_sizes[chunk_id] = segment_size;
        column_size += segment_size;
        pending_chunk_ids.push_back(chunk_id);
    }
    // std::cout << "Migrating column " << column_name << " of size " << column_size << "B to NUMA node " << numa_node_id << "\n";

    // Decide on an initial pool size
    size_t pool_size = static_cast<size_t>((float)column_size * 1.2); // 20% overhead
    // size_t pool_size = static_cast<size_t>((float)column_size * 0.9); // 10% undershoot

    size_t bytes_migrated = 0;

    std::deque<size_t> pools_used_for_column;

    auto start_time = std::chrono::high_resolution_clock::now();

    while (!pending_chunk_ids.empty())
    {
        // prefault=true: this pool is sized to the column that is about to fill it, so faulting it
        // in up front (in parallel) is strictly better than paying per-page faults inside the copy.
        size_t pool_id = _pool_manager.create_pool(pool_size, numa_node_id, false, true);
        auto memory_resource = _pool_manager.get_pool(pool_id);
        // std::cout << "Pool " << pool_id << " created of size " << pool_size << "B for column " << column_name << " on NUMA node " << numa_node_id << "\n";

        const auto failed_chunk_ids = migrate_chunks(table_name, column_id, pending_chunk_ids, memory_resource);

        const auto segments_migrated_to_pool = pending_chunk_ids.size() - failed_chunk_ids.size();
        for (const auto chunk_id : pending_chunk_ids)
        {
            if (!std::binary_search(failed_chunk_ids.begin(), failed_chunk_ids.end(), chunk_id))
            {
                bytes_migrated += segment_sizes[chunk_id];
            }
        }

        if (segments_migrated_to_pool > 0)
        {
            pools_used_for_column.push_back(pool_id);
            // std::cout << "Pool " << pool_id << " committed for column " << column_name << " with " << segments_migrated_to_pool << " segments\n";
        }
        else
        {
            memory_resource.reset();
            _pool_manager.delete_pool(pool_id);
            // std::cout << "Pool " << pool_id << " of size " << pool_size << "B discarded since it accomodated 0 segments\n";
            // Not one segment fit, so growing by the remaining size would just fail again.
            pool_size *= 2;
        }

        if (segments_migrated_to_pool > 0 && !failed_chunk_ids.empty())
        {
            // Some segments were migrated. Create a pool for the remaining size.
            pool_size = static_cast<size_t>((float)(column_size - bytes_migrated) * 1.2); // 20% overhead
        }

        pending_chunk_ids = failed_chunk_ids;
    }

    // // Verify total migrated size
    // size_t total_migrated_size = 0;
    // for (const auto &pool_id : pools_used_for_column)
    // {
    //     total_migrated_size += _pool_manager.get_pool(pool_id)->allocated_bytes();
    // }

    // Delete the original column to pool mapping
    if (_columns_to_pools_mapping.find(column_name) != _columns_to_pools_mapping.end())
    {
        delete_column_pool(column_name);
    }
    // Update the mapping
    _columns_to_pools_mapping[column_name] = pools_used_for_column;

    // Log migration summary
    // std::printf("Columns %s of size %luB migrated to %d with total migrated size %luB across %lu pools\n",
    //             column_name.c_str(), column_size, numa_node_id, total_migrated_size,
    //             _columns_to_pools_mapping[column_name].size());
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    if (print_migration_stats)
    {
        double bandwidth = (static_cast<double>(column_size) / (1ULL << 30)) / (static_cast<double>(duration) / 1000000000.0);
        std::printf("##Migration: column %s of size %luB to NUMA node %d in %ld ns (Bandwidth: %.2f GB/s)\n", column_name.c_str(), column_size, numa_node_id, duration, bandwidth);
    }

}

void MigrationEngine::delete_column_pool(const std::string &column_name)
{
    auto it = _columns_to_pools_mapping.find(column_name);
    Assertf(it != _columns_to_pools_mapping.end(), "Trying to delete non-existing pools for column %s\n", column_name.c_str());

    for (auto &pool_id : it->second)
    {
        _pool_manager.delete_pool(pool_id);
    }

    _columns_to_pools_mapping.erase(it);
}

void MigrationEngine::run_parallel_migrations(const std::vector<MigrationRequest>& requests, const std::string& label)
{
    if (requests.empty()) return;

    // ---- Phase 1 (serial): size columns, provision destination pools ----
    // All writes to _columns_to_pools_mapping happen here and nowhere else.
    struct ColInfo {
        std::shared_ptr<Table>                 table;
        std::string                            column_name;
        int                                    numa_node;
        ColumnID                               column_id;
        size_t                                 pool_id;
        std::shared_ptr<NumaMonotonicResource> pool;
        std::vector<size_t>                    segment_sizes;
        size_t                                 column_size{0};
        std::deque<size_t>                     old_pool_ids;  // freed in Phase 3, after replace_segment
    };

    auto col_infos = std::vector<ColInfo>{};
    col_infos.reserve(requests.size());

    for (const auto& req : requests) {
        auto& ci    = col_infos.emplace_back();
        ci.table       = req.table;
        ci.column_name = req.column_name;
        ci.numa_node   = req.numa_node;
        ci.column_id   = req.table->column_id_by_name(req.column_name);

        const auto chunk_count = req.table->chunk_count();
        ci.segment_sizes.resize(chunk_count, 0);
        for (ChunkID cid{0}; cid < chunk_count; ++cid) {
            const auto sz = req.table->get_chunk(cid)->get_segment(ci.column_id)
                                ->memory_usage(MemoryUsageCalculationMode::Full);
            ci.segment_sizes[cid] = sz;
            ci.column_size += sz;
        }

        const auto pool_size = static_cast<size_t>(static_cast<float>(ci.column_size) * 1.2f);
        // prefault=true: pages are faulted into the destination NUMA node before any copy starts
        ci.pool_id = _pool_manager.create_pool(pool_size, ci.numa_node, false, true);
        ci.pool    = _pool_manager.get_pool(ci.pool_id);

        // Save old pool ids but do NOT free them yet — the chunks still hold shared_ptrs to
        // segments in those pools. Freeing now would make Phase 2 workers segfault on get_segment.
        // The old pools are released in Phase 3 after all replace_segment calls are done.
        const auto old_it = _columns_to_pools_mapping.find(ci.column_name);
        if (old_it != _columns_to_pools_mapping.end()) {
            ci.old_pool_ids = old_it->second;
            _columns_to_pools_mapping.erase(old_it);
        }
        _columns_to_pools_mapping[ci.column_name].push_back(ci.pool_id);
    }

    // ---- Build round-robin task list (serial) ----
    // Round-robin interleaving ensures threads from different columns hit CXL simultaneously,
    // maximising aggregate CXL read bandwidth instead of serialising column-by-column.
    struct MigrationTask {
        std::shared_ptr<Table>                 table;
        ColumnID                               column_id;
        ChunkID                                chunk_id;
        std::shared_ptr<NumaMonotonicResource> pool;
        size_t                                 col_idx;
    };

    size_t max_chunks = 0;
    for (const auto& ci : col_infos) {
        max_chunks = std::max(max_chunks, static_cast<size_t>(ci.table->chunk_count()));
    }

    auto tasks = std::vector<MigrationTask>{};
    tasks.reserve(max_chunks * col_infos.size());
    for (size_t round = 0; round < max_chunks; ++round) {
        for (size_t i = 0; i < col_infos.size(); ++i) {
            const auto& ci = col_infos[i];
            if (round < static_cast<size_t>(ci.table->chunk_count())) {
                tasks.push_back({ci.table, ci.column_id,
                                 ChunkID{static_cast<ChunkID::base_type>(round)},
                                 ci.pool, i});
            }
        }
    }

    // ---- Phase 2 (parallel): drain task queue with a fixed thread pool ----
    // Only shared mutable state in the hot path is next_task (one relaxed fetch_add).
    // Each (table, column_id, chunk_id) triple appears exactly once so replace_segment
    // is never called concurrently for the same segment.
    const auto thread_count = std::min(
        tasks.size(),
        static_cast<size_t>(std::max(1u, std::thread::hardware_concurrency())));

    auto next_task    = std::atomic<size_t>{0};
    auto failed_mutex = std::mutex{};
    auto failed_col_indices = std::vector<size_t>{};

    const auto wall_start = std::chrono::high_resolution_clock::now();

    const auto worker = [&] {
        for (;;) {
            const auto idx = next_task.fetch_add(1, std::memory_order_relaxed);
            if (idx >= tasks.size()) return;
            const auto& t = tasks[idx];
            auto chunk_ptr = t.table->get_chunk(t.chunk_id);
            auto seg_ptr   = chunk_ptr->get_segment(t.column_id);
            auto pool      = t.pool;
            try {
                migrate_segment(chunk_ptr, seg_ptr, t.column_id, pool);
            } catch (const std::bad_alloc&) {
                auto lock = std::lock_guard{failed_mutex};
                failed_col_indices.push_back(t.col_idx);
            }
        }
    };

    auto threads = std::vector<std::thread>{};
    threads.reserve(thread_count - 1);
    for (size_t i = 1; i < thread_count; ++i) threads.emplace_back(worker);
    worker();
    for (auto& thr : threads) thr.join();

    const auto wall_end = std::chrono::high_resolution_clock::now();

    // ---- Phase 3 (serial): release old pools and handle any overflow failures ----
    // Old segments have been swapped out by replace_segment; it is now safe to free their pools.
    for (const auto& ci : col_infos) {
        for (const auto old_id : ci.old_pool_ids) {
            _pool_manager.delete_pool(old_id);
        }
    }

    if (!failed_col_indices.empty()) {
        std::sort(failed_col_indices.begin(), failed_col_indices.end());
        failed_col_indices.erase(std::unique(failed_col_indices.begin(), failed_col_indices.end()),
                                 failed_col_indices.end());
        for (const auto col_idx : failed_col_indices) {
            const auto& ci = col_infos[col_idx];
            std::fprintf(stderr, "run_parallel_migrations: pool overflow for column %s, "
                                 "retrying serially\n", ci.column_name.c_str());
            migrate_column(const_cast<std::shared_ptr<Table>&>(ci.table), ci.column_name, ci.numa_node);
        }
    }

    if (print_migration_stats) {
        size_t total_bytes = 0;
        for (const auto& ci : col_infos) total_bytes += ci.column_size;
        const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            wall_end - wall_start).count();
        const double bw = (static_cast<double>(total_bytes) / static_cast<double>(1ULL << 30))
                          / (static_cast<double>(duration_ns) / 1e9);
        std::printf("##ParallelMigration(%s): %zu columns, %zu bytes in %ld ns (%.2f GB/s aggregate)\n",
                    label.c_str(), col_infos.size(), total_bytes, duration_ns, bw);
    }
}

std::unordered_map<int, MemResourceStatus> &MigrationEngine::aggregate_migrated_status()
{
    _migrated_status.clear();

    for (const auto &[column_name, pool_ids] : _columns_to_pools_mapping)
    {
        for (const auto &pool_id : pool_ids)
        {
            auto pool_ptr = _pool_manager.get_pool(pool_id);
            auto status = pool_ptr->status();

            if (_migrated_status.find(status.numa_node) == _migrated_status.end())
            {
                _migrated_status[status.numa_node] = MemResourceStatus{};
                _migrated_status[status.numa_node].description = "MigratedColumnsAggregate";
                _migrated_status[status.numa_node].numa_node = status.numa_node;
            }

            _migrated_status[status.numa_node].capacity_bytes += status.capacity_bytes;
            _migrated_status[status.numa_node].allocated_bytes += status.allocated_bytes;
            _migrated_status[status.numa_node].peak_allocated_bytes += status.peak_allocated_bytes;
        }
    }
    return _migrated_status;
}

std::vector<MemResourceStatus> MigrationEngine::all_pool_status() const
{
    std::vector<MemResourceStatus> statuses;
    for (const auto &[column_name, pool_ids] : _columns_to_pools_mapping)
    {
        for (const auto &pool_id : pool_ids)
        {
            auto pool_ptr = _pool_manager.get_pool(pool_id);
            auto s = pool_ptr->status();
            s.resource_id = pool_id;
            statuses.push_back(s);
        }
    }

    return statuses;
}

std::pair<size_t, size_t> MigrationEngine::quick_size_check() const
{
    return std::make_pair(_migrated_status.find(0)->second.allocated_bytes, _migrated_status.find(1)->second.allocated_bytes);
}
} // namespace hyrise