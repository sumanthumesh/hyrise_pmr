#pragma once

#include <cstddef>
#include <string>
#include <fmt/format.h>

namespace hyrise
{
struct MemResourceStatus
{
    std::string description{""};
    size_t resource_id{0};
    size_t capacity_bytes{0};
    size_t allocated_bytes{0};
    size_t peak_allocated_bytes{0};
    int numa_node{-1};

    std::string to_string() const
    {
        return "MemResourceStatus {" + description + ","+std::to_string(resource_id)+
               ","+fmt::format("{:x}", capacity_bytes)+","+fmt::format("{:x}", allocated_bytes)+
               ","+fmt::format("{:x}", peak_allocated_bytes)+","+std::to_string(numa_node)+"}";
    }
};
} // namespace hyrise