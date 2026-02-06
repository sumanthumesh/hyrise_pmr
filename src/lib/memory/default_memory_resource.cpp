#include "default_memory_resource.hpp"

#include <cstddef>
#include <cstdlib>

#include "types.hpp"

namespace hyrise
{

// We discourage manual memory management in Hyrise (such as malloc, or new), but in case of allocator/memory resource
// implementations, it is fine.
// NOLINTBEGIN(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)

void *DefaultResource::do_allocate(std::size_t bytes, std::size_t /*alignment*/)
{
    _bytes_allocated += bytes;
    if (_bytes_allocated > _peak_bytes_allocated)
    {
        _peak_bytes_allocated = _bytes_allocated;
    }
    return std::malloc(bytes);
}

void DefaultResource::do_deallocate(void *pointer, std::size_t bytes, std::size_t /*alignment*/)
{
    _bytes_allocated -= bytes;
    std::free(pointer);
}

[[nodiscard]] bool DefaultResource::do_is_equal(const MemoryResource &other) const noexcept
{
    return &other == this;
}
// NOLINTEND(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)

MemResourceStatus DefaultResource::status() const
{
    MemResourceStatus status;
    status.description = "DefaultResource";
    status.resource_id = reinterpret_cast<size_t>(this);
    status.capacity_bytes = std::numeric_limits<size_t>::max(); // No fixed capacity
    status.allocated_bytes = _bytes_allocated;
    status.peak_allocated_bytes = _peak_bytes_allocated;
    status.numa_node = -1; // Not NUMA-aware
    return status;
}

} // namespace hyrise
