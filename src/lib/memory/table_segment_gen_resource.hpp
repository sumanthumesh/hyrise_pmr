#pragma once

// Tiny accessor header. Returns the PMR resource that table-generation segments should
// allocate from (MemManager::memory_resources.TableSegmentGen). Encoders / parsers need
// this resource pointer but do NOT want a transitive dependency on the full MemManager
// declaration — editing mem_manager.hpp would otherwise cascade through every encoder
// header into ~56 operator translation units.
//
// Definition lives in mem_manager.cpp.

#include <memory_resource>

namespace hyrise
{

std::pmr::memory_resource *get_table_segment_gen_resource();

} // namespace hyrise
