#pragma once

#include "types.hpp"
#include "utils/singleton.hpp"
#include "mem_common.hpp"

namespace hyrise
{

class DefaultResource : public MemoryResource, public Singleton<DefaultResource>
{
  public:
    void *do_allocate(std::size_t bytes, std::size_t /*alignment*/) override;
    void do_deallocate(void *pointer, std::size_t /*bytes*/, std::size_t /*alignment*/) override;
    [[nodiscard]] bool do_is_equal(const MemoryResource &other) const noexcept override;
    std::size_t bytes_allocated() const { return _bytes_allocated; }
    std::size_t peak_bytes_allocated() const { return _peak_bytes_allocated; }
    MemResourceStatus status() const;
  private:
    size_t _bytes_allocated{0};
    size_t _peak_bytes_allocated{0};
};

} // namespace hyrise
