#include "segment_access_summary.hpp"

#include <cstdint>

#include "magic_enum/magic_enum.hpp"

#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_access_counter.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise
{

std::vector<SegmentAccessSummary> collect_segment_access_summary()
{
    auto summary = std::vector<SegmentAccessSummary>{};

    for (const auto &[table_name, table] : Hyrise::get().storage_manager.tables())
    {
        const auto chunk_count = table->chunk_count();
        const auto column_count = table->column_count();

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id)
        {
            const auto &chunk = table->get_chunk(chunk_id);
            // Skip physically deleted chunks.
            if (!chunk)
            {
                continue;
            }

            for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id)
            {
                const auto &segment = chunk->get_segment(column_id);
                const auto &access_counter = segment->access_counter;

                auto total = uint64_t{0};
                for (const auto access_type : magic_enum::enum_values<SegmentAccessCounter::AccessType>())
                {
                    total += access_counter[access_type].load(std::memory_order_relaxed);
                }

                summary.push_back(
                    {table_name, table->column_name(column_id), chunk_id, total});
            }
        }
    }

    return summary;
}

std::vector<ColumnAccessSummary> collect_column_access_summary()
{
    auto summary = std::vector<ColumnAccessSummary>{};

    for (const auto &[table_name, table] : Hyrise::get().storage_manager.tables())
    {
        const auto chunk_count = table->chunk_count();
        const auto column_count = table->column_count();

        auto per_column_totals = std::vector<uint64_t>(column_count, 0);

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id)
        {
            const auto &chunk = table->get_chunk(chunk_id);
            // Skip physically deleted chunks.
            if (!chunk)
            {
                continue;
            }

            for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id)
            {
                const auto &segment = chunk->get_segment(column_id);
                const auto &access_counter = segment->access_counter;

                for (const auto access_type : magic_enum::enum_values<SegmentAccessCounter::AccessType>())
                {
                    per_column_totals[column_id] += access_counter[access_type].load(std::memory_order_relaxed);
                }
            }
        }

        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id)
        {
            summary.push_back({table_name, table->column_name(column_id), per_column_totals[column_id]});
        }
    }

    return summary;
}

} // namespace hyrise
