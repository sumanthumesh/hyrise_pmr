#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "types.hpp"

namespace hyrise
{

struct SegmentAccessSummary
{
    std::string table_name;
    std::string column_name;
    ChunkID chunk_id;
    uint64_t total_accesses;
};

// Snapshot the per-segment access counters across every table registered with the StorageManager.
// One row is produced per (table, column, chunk); zero-access rows are included. total_accesses is the sum of
// SegmentAccessCounter's five AccessType counters (Point + Sequential + Monotonic + Random + Dictionary).
// Values are cumulative since the segment was created; per-query deltas are the caller's responsibility.
std::vector<SegmentAccessSummary> collect_segment_access_summary();

} // namespace hyrise
