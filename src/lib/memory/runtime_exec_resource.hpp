#pragma once

// Tiny accessor header. Returns the PMR resource that "large, footprint-tracked"
// runtime execution allocations should use (= MemManager::pick_runtime_exec_resource()).
//
// Use this from headers that need the resource at construction time of templated
// objects (e.g., PosHashTable in join_hash_steps.hpp) so they avoid a transitive
// dependency on the full MemManager declaration. Definition lives in mem_manager.cpp.

#include <memory_resource>

namespace hyrise
{

std::pmr::memory_resource *runtime_exec_resource();

} // namespace hyrise
