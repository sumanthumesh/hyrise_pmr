#pragma once

#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// Warning: In the past, magic_enum has led to problems with TSan. See #2154 for details.
#include "magic_enum/magic_enum.hpp"

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/format_duration.hpp"

namespace hyrise
{
struct AbstractOperatorPerformanceData : public Noncopyable
{
    enum class NoSteps
    {
        Invalid // Needed by magic_enum for enum_count
    };

    virtual ~AbstractOperatorPerformanceData() = default;

    virtual void output_to_stream(std::ostream &stream, DescriptionMode description_mode) const = 0;

    std::chrono::nanoseconds walltime{0};

    // The instant AbstractOperator::execute() started measuring walltime; the end of the measured
    // region is start_time + walltime. steady_clock, so it is only meaningful relative to other
    // operators' start times (which is the point: it lets a plan's operators be laid out on a
    // common timeline to see what actually overlapped). Left at its default for operators that
    // never executed.
    std::chrono::steady_clock::time_point start_time{};

    // start_time as a plain integer for export. Zero iff the operator never executed.
    int64_t start_time_ns() const
    {
        return start_time.time_since_epoch().count();
    }

    // Some operators do not return a table (e.g., Insert).
    // Note: The operator returning an empty table will be expressed as has_output == true, output_row_count == 0
    bool has_output{false};
    uint64_t output_row_count{0};
    uint64_t output_chunk_count{0};

    // Populated only when AbstractOperator::track_per_operator_memory is on. Keyed by
    // MemResourceStatus::resource_id. Holds the delta of total_allocated_bytes over this
    // operator's _on_execute() call, and the currently allocated_bytes when the operator finishes.
    struct ResourceDelta
    {
        int numa_node{-1};
        int64_t total_allocated_bytes_delta{0};
        size_t allocated_bytes{0};
        // Max live bytes reached DURING this operator's _on_execute(), above the operator's
        // allocated-bytes baseline. Uses the pool's single _peak_allocated_bytes counter,
        // which is mutually exclusive with per-query peak tracking: when
        // track_per_operator_memory is on, sql_pipeline_statement skips its own reset_peak
        // and the query-level peak_mem_usage field is a sentinel (-1).
        int64_t peak_mem_usage_delta{0};
    };
    std::unordered_map<size_t, ResourceDelta> per_resource_allocation_delta;

    // Populated only when AbstractOperator::track_per_operator_memory is on. Deduplicated
    // storage-table column names this operator READ from its left / right inputs respectively,
    // captured during execute() while input tables are still available. Split by side so
    // downstream hotness analysis can pair columns with LeftCard / RightCard.
    std::vector<std::string> left_read_columns;
    std::vector<std::string> right_read_columns;
};

/**
 * General execution information is stored in OperatorPerformanceData through AbstractOperator::execute().
 * The template parameter Steps allows operators to store runtimes for an operators exeuctions steps (e.g., see
 * JoinHash). In case no steps are desired, OperatorPerformanceData should be instantiated with
 * AbstractPerformanceData::NoSteps.
 * Operators can further store additional execution information by inheriting from OperatorPerformanceData (e.g.,
 * see JoinIndex).
 */
template <typename Steps>
struct OperatorPerformanceData : public AbstractOperatorPerformanceData
{
    void output_to_stream(std::ostream &stream, DescriptionMode description_mode) const override
    {
        if (!has_output)
        {
            stream << "No output.";
            return;
        }

        stream << "Output: " << output_row_count << " row" << (output_row_count > 1 ? "s" : "") << " in "
               << output_chunk_count << " chunk" << (output_chunk_count > 1 ? "s" : "") << ", " << format_duration(walltime)
               << ".";

        if constexpr (std::is_same_v<Steps, NoSteps>)
        {
            return;
        }

        // Check that the cumulative step runtimes are not larger than the operator's runtime.
        if constexpr (HYRISE_DEBUG)
        {
            auto cumulative_step_runtime = size_t{0};
            for (auto step_index = size_t{0}; step_index < magic_enum::enum_count<Steps>(); ++step_index)
            {
                cumulative_step_runtime += step_runtimes[step_index].count();
            }
            Assert(static_cast<size_t>(walltime.count()) >= cumulative_step_runtime,
                   "Cumulative step runtimes larger than operator runtime.");
        }

        static_assert(magic_enum::enum_count<Steps>() <= sizeof(step_runtimes), "Too many steps.");
        stream << (description_mode == DescriptionMode::SingleLine ? " " : "\n")
               << "Operator step runtimes:" << (description_mode == DescriptionMode::SingleLine ? "" : "\n");
        for (auto step_index = size_t{0}; step_index < magic_enum::enum_count<Steps>(); ++step_index)
        {
            if (step_index > 0)
            {
                stream << (description_mode == DescriptionMode::SingleLine ? "," : "\n");
            }
            stream << " " << magic_enum::enum_name(static_cast<Steps>(step_index)) << " "
                   << format_duration(step_runtimes[step_index]);
        }
        stream << ".";
    }

    std::chrono::nanoseconds get_step_runtime(const Steps step) const
    {
        DebugAssert(magic_enum::enum_integer(step) < magic_enum::enum_count<Steps>(), "Step index is too large.");
        return step_runtimes[static_cast<size_t>(step)];
    }

    void set_step_runtime(const Steps step, const std::chrono::nanoseconds duration)
    {
        DebugAssert(magic_enum::enum_integer(step) < magic_enum::enum_count<Steps>(), "Invalid step.");
        DebugAssert(step_runtimes[static_cast<size_t>(step)] == std::chrono::nanoseconds{0}, "Overwriting step runtime.");
        step_runtimes[static_cast<size_t>(step)] = duration;
    }

    std::array<std::chrono::nanoseconds, magic_enum::enum_count<Steps>()> step_runtimes{};
};

std::ostream &operator<<(std::ostream &stream, const AbstractOperatorPerformanceData &performance_data);

} // namespace hyrise
