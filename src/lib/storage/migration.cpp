#include "storage/migration.hpp"

#include <deque>

namespace hyrise
{

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

void MigrationEngine::migrate_column(std::shared_ptr<Table> &table_name, const std::string &column_name, int numa_node_id)
{
    ColumnID column_id = table_name->column_id_by_name(column_name);

    // Get size of the column
    size_t column_size = get_column_size(table_name, column_id);
    std::cout << "Migrating column " << column_name << " of size " << column_size << "B to NUMA node " << numa_node_id << "\n";

    // Decide on an initial pool size
    size_t pool_size = static_cast<size_t>((float)column_size * 1.2); // 20% overhead
    // size_t pool_size = static_cast<size_t>((float)column_size * 0.9); // 10% undershoot

    // Create a new pool of this size
    size_t pool_id = _pool_manager.create_pool(pool_size, numa_node_id);
    auto memory_resource = _pool_manager.get_pool(pool_id);
    std::cout << "Initial pool " << pool_id << " created of size " << pool_size << "B for column " << column_name << " on NUMA node " << numa_node_id << "\n";

    size_t bytes_migrated = 0;

    size_t num_segments_migrated_to_pool = 0;

    for (ChunkID chunk_id{0}; chunk_id < table_name->chunk_count(); ++chunk_id)
    {
        auto chunk_ptr = table_name->get_chunk(chunk_id);
        auto segment_ptr = chunk_ptr->get_segment(column_id);

        std::cout << "ChunkID" << chunk_id << "\n";

        size_t current_segment_size = segment_ptr->memory_usage(MemoryUsageCalculationMode::Full);

        while (true)
        {
            try
            {
                // Try to migrate the segment
                migrate_segment(chunk_ptr, segment_ptr, column_id, memory_resource);
                num_segments_migrated_to_pool++;
                break; // Migration successful, exit the loop
            }
            catch (const std::bad_alloc &)
            {
                // Allocation failed. Pool was insufficient.
                // Will create a new pool for the remaining segments.

                // Commit the first pool if any segments were migrated to it
                if (num_segments_migrated_to_pool > 0)
                {
                    if (_columns_to_pools_mapping.find(column_name) == _columns_to_pools_mapping.end())
                    {
                        _columns_to_pools_mapping[column_name] = std::deque<size_t>{};
                    }
                    _columns_to_pools_mapping[column_name].push_back(pool_id);
                    std::cout << "Pool " << pool_id << " committed for column " << column_name << " with " << num_segments_migrated_to_pool << " segments\n";
                }
                else
                {
                    memory_resource.reset();
                    _pool_manager.delete_pool(pool_id);
                    std::cout << "Pool " << pool_id << " of size " << pool_size << "B discarded since it accomodated 0 segments\n";
                }

                // Create a new pool for the remaining segments
                if (num_segments_migrated_to_pool == 0)
                {
                    // No segment was migrated. Pool size was too small. Increase pool size
                    pool_size *= 2;
                }
                else
                {
                    // Some segments were migrated. Create a pool for the remaining size
                    pool_size = static_cast<size_t>((float)(column_size - bytes_migrated) * 1.2); // 20% overhead
                }
                
                pool_id = _pool_manager.create_pool(pool_size, numa_node_id);
                memory_resource = _pool_manager.get_pool(pool_id);
                std::cout << "New pool " << pool_id << " created of size " << pool_size << "B for column " << column_name << " on NUMA node " << numa_node_id << "\n";

                num_segments_migrated_to_pool = 0;


                continue;
            }
        }

        bytes_migrated += current_segment_size;
    }

    // Commit the last pool
    if (_columns_to_pools_mapping.find(column_name) == _columns_to_pools_mapping.end())
    {
        _columns_to_pools_mapping[column_name] = std::deque<size_t>{};
    }
    _columns_to_pools_mapping[column_name].push_back(pool_id);

    // Verify total migrated size
    size_t total_migrated_size = 0;
    for (const auto &pool_id : _columns_to_pools_mapping[column_name])
    {
        total_migrated_size += _pool_manager.get_pool(pool_id)->allocated_bytes();
    }

    // Log migration summary
    std::printf("Columns %s of size %luB migrated to %d with total migrated size %luB across %lu pools\n",
                column_name.c_str(), column_size, numa_node_id, total_migrated_size,
                _columns_to_pools_mapping[column_name].size());
}

void MigrationEngine::delete_column_pool(const std::string &column_name)
{
    auto it = _columns_to_pools_mapping.find(column_name);
    Assertf(it != _columns_to_pools_mapping.end(), "Trying to delete non-existing pools for column %s\n", column_name.c_str());

    for (auto &pool_id: it->second)
    {
        _pool_manager.delete_pool(pool_id);
    }

    _columns_to_pools_mapping.erase(it);
}
} // namespace hyrise