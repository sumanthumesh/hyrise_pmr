#pragma once

#include <cstddef>
#include <string>
#include <sstream>
#include <iomanip>

namespace hyrise
{
    /**
     * Simple representation of the status of a memory resource, used for logging and debugging.
     */
struct MemResourceStatus
{
    std::string description{""};
    size_t resource_id{std::numeric_limits<size_t>::max()};
    size_t capacity_bytes{0};
    size_t allocated_bytes{0};
    size_t peak_allocated_bytes{0};
    size_t total_allocated_bytes{0};
    int numa_node{-1};

    std::string to_string() const
    {
        std::stringstream ss;
        ss << "MemResourceStatus {" << description << "," << std::dec << resource_id << ","
           << std::dec << capacity_bytes << "," << std::dec << allocated_bytes << ","
           << std::dec << peak_allocated_bytes << "," 
           << std::dec << total_allocated_bytes << "," 
           << std::dec << numa_node << "}";
        return ss.str();
    }
};
} // namespace hyrise