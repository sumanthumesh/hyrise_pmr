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
// Copying a column is memory-bandwidth-bound, so a handful of threads already saturates the
// interconnect; spawning one per core just adds contention.
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
        double bandwidth = (static_cast<double>(column_size) / 1000000000.0) / (static_cast<double>(duration) / 1000000000.0);
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