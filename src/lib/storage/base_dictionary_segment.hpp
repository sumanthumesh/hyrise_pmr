#pragma once

#include <memory>

#include "abstract_encoded_segment.hpp"

namespace hyrise
{

class BaseCompressedVector;

/**
 * @brief Base class of DictionarySegment<T> exposing type-independent interface
 */
class BaseDictionarySegment : public AbstractEncodedSegment
{
  public:
    using AbstractEncodedSegment::AbstractEncodedSegment;

    EncodingType encoding_type() const override = 0;

    /**
     * @brief Returns index (i.e. ValueID) of first dictionary entry >= search value
     *
     * @param value the search value
     * @return INVALID_VALUE_ID if all entries are smaller than value
     */
    virtual ValueID lower_bound(const AllTypeVariant &value) const = 0;

    /**
     * @brief Returns index (i.e. ValueID) of first dictionary entry > search value
     *
     * @param value the search value
     * @return INVALID_VALUE_ID if all entries are smaller than or equal to value
     */
    virtual ValueID upper_bound(const AllTypeVariant &value) const = 0;

    /**
     * @pre       @param value_id is a valid ValueID of the Dictionary
     * @return    The value associated with @param value_id
     */
    virtual AllTypeVariant value_of_value_id(const ValueID value_id) const = 0;

    /**
     * @brief The size of the dictionary
     */
    virtual ValueID::base_type unique_values_count() const = 0;

    virtual std::shared_ptr<const BaseCompressedVector> attribute_vector() const = 0;

    /**
     * @brief Returns encoding specific null value ID
     */
    virtual ValueID null_value_id() const = 0;

    inline std::string type_description() const override
    {
        std::string desc = "BaseDictionarySegment";
        switch (data_type())
        {
        case DataType::Int:
            desc += " (Int)";
            break;
        case DataType::Long:
            desc += " (Long)";
            break;
        case DataType::Float:
            desc += " (Float)";
            break;
        case DataType::Double:
            desc += " (Double)";
            break;
        case DataType::String:
            desc += " (String)";
            break;
        case DataType::Null:
            desc += " (Null)";
            break;
        default:
            desc += " (Unknown)";
            break;
        }
        return desc;
    }
};
} // namespace hyrise
