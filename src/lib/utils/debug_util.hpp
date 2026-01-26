#pragma once

#include <cstdlib>
#include <execinfo.h>
#include <iostream>
#include <sstream>
#include <string>

#include <cstdio>
#include <cstdlib>
#include <unordered_set>

#include "utils/singleton.hpp"

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
inline void assertf_impl(bool cond, const char* file, int line,
                         const char* fmt, const Args&... args)
{
    if (!cond)
    {
        std::ostringstream oss;
        oss << "Assertion failed at " << file << ":" << line << ": ";

        if constexpr (sizeof...(args) == 0)
        {
            oss << fmt;
        }
        else
        {
            char buf[1024];
            
            // Suppress -Wformat-nonliteral for this call
            #pragma GCC diagnostic push
            #pragma GCC diagnostic ignored "-Wformat-nonliteral"
            std::snprintf(buf, sizeof(buf), fmt, args...);
            oss << buf;
            #pragma GCC diagnostic pop
        }

        auto msg = oss.str();
        std::fprintf(stderr, "%s\n", msg.c_str());
        std::fflush(stderr);
        std::exit(EXIT_FAILURE);
    }
}

#define Assertf(cond, fmt, ...) \
    assertf_impl((cond), __FILE__, __LINE__, (fmt) __VA_OPT__(, ) __VA_ARGS__)

std::string print_backtrace();

namespace hyrise
{
    class OperatorsUsed : public Singleton<OperatorsUsed>
    {
      public:
        void add_operator(const std::string& operator_name)
        {
            _operators_used.insert(operator_name);
        }

        void print_operators_used() const
        {
            std::cout << "Operators used in this run:\n";
            for (const auto& op : _operators_used)
            {
                std::cout << "- " << op << "\n";
            }
        }

      private:
        std::unordered_set<std::string> _operators_used;
    };
    class SegmentsUsed : public Singleton<SegmentsUsed>
    {
      public:
        void add_segment(const std::string& segment_name)
        {
            _segments_used.insert(segment_name);
        }

        void print_segments_used() const
        {
            std::cout << "Segments used in this run:\n";
            for (const auto& op : _segments_used)
            {
                std::cout << "- " << op << "\n";
            }
        }

      private:
        std::unordered_set<std::string> _segments_used;
    };
}