#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <inttypes.h>
#include <iomanip>
#include <iostream>
#include <malloc.h>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "operators/print.hpp"
#include "storage/encoding_type.hpp"
#include "storage/mem_pool.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_vector.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/debug_util.hpp"
#include "utils/print_utils.hpp"

namespace hyrise
{

class MigrationEngine
{
  public:
    MigrationEngine() = default;
    // ~MigrationEngine() = default;

    void migrate_column(const std::string &table_name, const std::string &column_name, int numa_node_id);
    /**
     * Migrate Int,Long,Float,Double DictionarySegments
     */
    template <typename T>
    void migrate_numerical_dictionary_segment(std::shared_ptr<Chunk> &chunk_ptr, std::shared_ptr<AbstractSegment> &segment_ptr, ColumnID column_id, std::shared_ptr<NumaMonotonicResource> &memory_resource)
    {
        // Implementation for migrating numerical dictionary segments
        auto abs_encoded_segment_ptr = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment_ptr);
        Assertf(abs_encoded_segment_ptr != nullptr, "AbstractSegment to AbstractEncodedSegment conversion failed\n");
        auto base_dict_segment_ptr = std::dynamic_pointer_cast<BaseDictionarySegment>(abs_encoded_segment_ptr);
        Assertf(base_dict_segment_ptr != nullptr, "AbstractEncodedSegment to BaseDictionarySegment conversion failed\n");
        auto dict_segment_ptr = std::dynamic_pointer_cast<const DictionarySegment<T>>(base_dict_segment_ptr);
        Assertf(dict_segment_ptr != nullptr, "BaseDictionarySegment to DictionarySegment conversion failed\n");

        // Copy to new dict segment ptr using the built in API
        auto new_dict_segment_ptr = dict_segment_ptr->copy_using_memory_resource(*memory_resource);

        // Replace segment pointer
        chunk_ptr->replace_segment(column_id, new_dict_segment_ptr);

        // Delete original segment
        dict_segment_ptr.reset();
        base_dict_segment_ptr.reset();
        abs_encoded_segment_ptr.reset();
        Assertf(segment_ptr.use_count() == 1, "Original segment pointer is still shared %lu times\n", segment_ptr.use_count() - 1);
        segment_ptr.reset();
    }
};
} // namespace hyrise