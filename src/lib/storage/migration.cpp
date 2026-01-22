#include "storage/migration.hpp"

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

    // Create a new pool of this size
    size_t pool_id = _pool_manager.unique_pool_id();
    std::string pool_name = column_name + "_pool_" + std::to_string(pool_id);
    _pool_manager.create_pool(pool_name, pool_size, numa_node_id);
    auto memory_resource = _pool_manager.get_pool(pool_name);
    std::cout << "Initial pool " << pool_name << " created of size " << pool_size << "B for column " << column_name << " on NUMA node " << numa_node_id << "\n";

    size_t bytes_migrated = 0;

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
                break; // Migration successful, exit the loop
            }
            catch (const std::bad_alloc &)
            {
                // Allocation failed. Pool was insufficient.
                // Will create a new pool for the remaining segments.

                // Commit the first pool
                if (_columns_to_pools_mapping.find(column_name) == _columns_to_pools_mapping.end())
                {
                    _columns_to_pools_mapping[column_name] = std::vector<std::shared_ptr<NumaMonotonicResource>>{};
                }
                _columns_to_pools_mapping[column_name].push_back(memory_resource);

                // Create a new pool for the remaining segments
                size_t remaining_size = column_size - bytes_migrated;
                pool_size = static_cast<size_t>((float)remaining_size * 1.2); // 20% overhead
                pool_id = _pool_manager.unique_pool_id();
                pool_name = column_name + "_pool_" + std::to_string(pool_id);
                _pool_manager.create_pool(pool_name, pool_size, numa_node_id);
                memory_resource = _pool_manager.get_pool(pool_name);
                std::cout << "New pool " << pool_name << " created of size " << pool_size << "B for column " << column_name << " on NUMA node " << numa_node_id << "\n";

                continue;
            }
        }

        bytes_migrated += current_segment_size;
    }

    // Commit the last pool
    if (_columns_to_pools_mapping.find(column_name) == _columns_to_pools_mapping.end())
    {
        _columns_to_pools_mapping[column_name] = std::vector<std::shared_ptr<NumaMonotonicResource>>{};
    }
    _columns_to_pools_mapping[column_name].push_back(memory_resource);

    // Verify total migrated size
    size_t total_migrated_size = 0;
    for (const auto &pool_ptr : _columns_to_pools_mapping[column_name])
    {
        total_migrated_size += pool_ptr->allocated_bytes();
    }

    // Log migration summary
    std::printf("Columns %s of size %luB migrated to %d with total migrated size %luB across %lu pools\n",
                column_name.c_str(), column_size, numa_node_id, total_migrated_size,
                _columns_to_pools_mapping[column_name].size());
}

} // namespace hyrise