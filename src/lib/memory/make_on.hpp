#pragma once

// Lightweight header for the make_on<T> shared_ptr factory.
//
// Separated from mem_manager.hpp on purpose: mem_manager.hpp is large (pulls in
// mem_pool.hpp, NUMA headers, etc.) and changes often. Storage-level headers
// (reference_segment.hpp, row_id_pos_list.hpp, ...) need make_on but should NOT
// cause a full operator rebuild every time MemManager's internals change.
// Include this thin header instead.

#include <memory>
#include <memory_resource>
#include <utility>

namespace hyrise
{

/**
 * Construct a shared_ptr<T> whose object and control block are allocated through
 * the given PMR resource (instead of the default heap). Equivalent to
 * std::make_shared<T>(args...) but routed through `resource`.
 */
template <typename T, typename... Args>
std::shared_ptr<T> make_on(std::pmr::memory_resource *resource, Args &&...args)
{
    return std::allocate_shared<T>(std::pmr::polymorphic_allocator<T>{resource},
                                   std::forward<Args>(args)...);
}

} // namespace hyrise
