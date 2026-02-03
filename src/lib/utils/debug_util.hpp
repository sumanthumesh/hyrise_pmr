#pragma once

#include <cstdlib>
#include <execinfo.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <cstdio>
#include <cstdlib>
#include <unordered_set>
#include <unistd.h>

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
inline void assertf_impl(bool cond, const char *file, int line,
                         const char *fmt, const Args &...args)
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
    assertf_impl((cond), __FILE__, __LINE__, (fmt)__VA_OPT__(, ) __VA_ARGS__)

std::string print_backtrace();

namespace hyrise
{
class OperatorsUsed : public Singleton<OperatorsUsed>
{
  public:
    void add_operator(const std::string &operator_name)
    {
        _operators_used.insert(operator_name);
    }

    void print_operators_used() const
    {
        std::cout << "Operators used in this run:\n";
        for (const auto &op : _operators_used)
        {
            std::cout << "- " << op << "\n";
        }
    }
    inline static bool tracking_enabled{false};

  private:
    std::unordered_set<std::string> _operators_used;
};
class SegmentsUsed : public Singleton<SegmentsUsed>
{
  public:
    void add_segment(const std::string &segment_name)
    {
        _segments_used.insert(segment_name);
    }

    void print_segments_used() const
    {
        std::cout << "Segments used in this run:\n";
        for (const auto &op : _segments_used)
        {
            std::cout << "- " << op << "\n";
        }
    }

    inline static bool tracking_enabled{false};
  private:
    std::unordered_set<std::string> _segments_used;
};
class OperatorMemoryUsage : public Singleton<OperatorMemoryUsage>
{
  public:
    struct TrackerHandle
    {
        std::vector<std::pair<size_t, size_t>>* memory_usage_data;
        std::shared_ptr<std::atomic<bool>> stop_flag;
    };

    TrackerHandle start_tracker(size_t operator_id)
    {
        // Assertf(_operator_memory_usage.find(operator_id) == _operator_memory_usage.end(),
        //         "Operator ID %zu is already being tracked.", operator_id);
        if (_operator_memory_usage.find(operator_id) != _operator_memory_usage.end())
        {
            _operator_memory_usage.erase(operator_id);
        }
        if (!OperatorMemoryUsage::initialized)
        {
            _operator_memory_usage[std::numeric_limits<size_t>::max()] = std::vector<std::pair<size_t, size_t>>{};
            size_t vms = 0, rss = 0;
            get_mem_usage(vms, rss);
            _operator_memory_usage[std::numeric_limits<size_t>::max()].emplace_back(vms, rss);
            OperatorMemoryUsage::initialized = true;
        }
        _operator_memory_usage[operator_id] = std::vector<std::pair<size_t, size_t>>{};
        auto stop_flag = std::make_shared<std::atomic<bool>>(false);
        return TrackerHandle{&_operator_memory_usage[operator_id], stop_flag};
    }
    
    void track_memory(TrackerHandle handle)
    {
        while(!handle.stop_flag->load())
        {
            size_t vms = 0;
            size_t rss = 0;
            get_mem_usage(vms, rss);
            handle.memory_usage_data->emplace_back(vms, rss);
            usleep(_sampling_interval);
        }
    }
    void print_memory_usage(std::string output_filename)
    {
        std::ofstream out_file(output_filename);
        Assertf(out_file.is_open(), "Could not open file %s for writing.", output_filename.c_str());
        for (const auto &entry : _operator_memory_usage)
        {
            size_t operator_id = entry.first;
            const auto &memory_usage_data = entry.second;
            out_file << operator_id << "\n";
            for (size_t i = 0; i < memory_usage_data.size(); ++i)
            {
                out_file << i << "," << memory_usage_data[i].first << "," << memory_usage_data[i].second << "\n";
            }
        }
        out_file.close();
    }
    static void get_mem_usage(size_t &vms, size_t &rss)
    {
        std::ifstream in("/proc/self/status");
        std::string line;
        while (std::getline(in, line))
        {
            // decode from line
            if (line.rfind("VmSize:", 0) == 0)
            {
                std::istringstream iss(line);
                std::string key;
                size_t value;
                std::string unit;
                iss >> key >> value >> unit;
                vms = value; // in KB
            }
            if (line.rfind("VmRSS:", 0) == 0)
            {
                std::istringstream iss(line);
                std::string key;
                size_t value;
                std::string unit;
                iss >> key >> value >> unit;
                rss = value; // in KB
            }
        }
    }
    void reset()
    {
        _operator_memory_usage.clear();
        OperatorMemoryUsage::initialized = false;
    }

    inline static bool initialized{false};
    inline static bool tracking_enabled{false};

  private:
    std::unordered_map<size_t, std::vector<std::pair<size_t, size_t>>> _operator_memory_usage;
    useconds_t _sampling_interval{1000000}; // 1 second
};
} // namespace hyrise