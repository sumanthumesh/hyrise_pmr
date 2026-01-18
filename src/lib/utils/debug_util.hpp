#include <cstdlib>
#include <execinfo.h>
#include <iostream>
#include <sstream>
#include <string>

#include <cstdio>
#include <cstdlib>

// The following asserts are printf versions of the asserts defined in utils/assert.hpp

#ifdef HYRISE_DEBUG

// Use only in debug builds. Because checking is not needed or expensive
// Usage: DebugAssertf(cond, "message: %d %s\n", x, s);
#define DebugAssertf(cond, fmt, ...)                              \
    do                                                            \
    {                                                             \
        if (cond)                                                 \
        {                                                         \
            std::fprintf(stderr, "DEBUG_FAIL_IF at %s:%d: " fmt,  \
                         __FILE__, __LINE__ /**/, ##__VA_ARGS__); \
            std::fflush(stderr);                                  \
            std::exit(EXIT_FAILURE);                              \
        }                                                         \
    } while (0)

#else

// When not in debug, do nothing and do not evaluate arguments
#define DebugAssertf(cond, fmt, ...) \
    do                               \
    {                                \
        (void)sizeof(cond);          \
    } while (0)

#endif

template <typename... Args>
inline void assertf_impl(bool cond, const char *file, int line,
                         const char *fmt, const Args &...args)
{
    if (!cond)
    {
        std::fprintf(stderr, "Assertion failed at %s:%d: ", file, line);
        if constexpr (sizeof...(args) == 0)
        {
            // No value arguments: print fmt as plain text
            std::fprintf(stderr, "%s", fmt);
        }
        else
        {
            // With value arguments: use fmt as format string
            std::fprintf(stderr, fmt, args...);
        }
        std::fflush(stderr);
        std::exit(EXIT_FAILURE);
    }
}

// Works with 0 or more args (C++20)
#define Assertf(cond, fmt, ...) \
    assertf_impl(cond, __FILE__, __LINE__, fmt __VA_OPT__(, ) __VA_ARGS__)

std::string print_backtrace();