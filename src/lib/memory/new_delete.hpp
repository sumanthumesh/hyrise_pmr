#pragma once

#include <cstddef>

// Start counting allocations done via global new/delete
void start_tracking_allocations();

// Stop counting
void stop_tracking_allocations();

// Return the current tracked byte count (interpretation depends on impl)
std::size_t tracked_bytes();

// Convenience RAII scope helper:
//   { AllocationRegion r; /* code */ } → prints net bytes for the region.
struct AllocationRegion {
    AllocationRegion();
    ~AllocationRegion();

    std::size_t before{};
};
