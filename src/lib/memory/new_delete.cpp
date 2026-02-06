#include "new_delete.hpp"

#include <atomic>
#include <new>
#include <cstdlib>
#include <cstdio>

// ---- Internal state ----

static std::atomic<bool> g_tracking{false};
static std::atomic<std::size_t> g_bytes{0};

// ---- Public API ----

void start_tracking_allocations() {
    g_tracking.store(true, std::memory_order_relaxed);
}

void stop_tracking_allocations() {
    g_tracking.store(false, std::memory_order_relaxed);
}

std::size_t tracked_bytes() {
    return g_bytes.load(std::memory_order_relaxed);
}

AllocationRegion::AllocationRegion() {
    before = tracked_bytes();
    start_tracking_allocations();
}

AllocationRegion::~AllocationRegion() {
    stop_tracking_allocations();
    auto after = tracked_bytes();
    std::fprintf(stderr,
                 "[AllocationRegion] net %zu bytes (after-before)\n",
                 after - before);
}

// ---- Global operator new/delete overrides ----

void* operator new(std::size_t sz) {
    void* p = std::malloc(sz);
    if (!p) throw std::bad_alloc{};
    if (g_tracking.load(std::memory_order_relaxed)) {
        g_bytes.fetch_add(sz, std::memory_order_relaxed);
    }
    return p;
}

void operator delete(void* p) noexcept {
    std::free(p);
}

// Sized delete (used by many compilers)
void operator delete(void* p, std::size_t sz) noexcept {
    // if (g_tracking.load(std::memory_order_relaxed)) {
    //     g_bytes.fetch_sub(sz, std::memory_order_relaxed);
    // }
    std::free(p);
}

// You may also want:
void* operator new[](std::size_t sz) {
    void* p = std::malloc(sz);
    if (!p) throw std::bad_alloc{};
    if (g_tracking.load(std::memory_order_relaxed)) {
        g_bytes.fetch_add(sz, std::memory_order_relaxed);
    }
    return p;
}

void operator delete[](void* p) noexcept {
    std::free(p);
}

void operator delete[](void* p, std::size_t sz) noexcept {
    // if (g_tracking.load(std::memory_order_relaxed)) {
    //     g_bytes.fetch_sub(sz, std::memory_order_relaxed);
    // }
    std::free(p);
}
