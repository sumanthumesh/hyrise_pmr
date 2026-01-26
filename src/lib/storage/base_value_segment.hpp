#pragma once

#include "abstract_segment.hpp"

namespace hyrise
{

/**
 * @brief Super class of value segments
 *
 * Exposes all methods of value segments that do not rely on its specific data type.
 */
class BaseValueSegment : public AbstractSegment
{
  public:
    using AbstractSegment::AbstractSegment;

    // returns true if segment supports null values
    virtual bool is_nullable() const = 0;

    // appends the value at the end of the segment
    virtual void append(const AllTypeVariant &val) = 0;

    /**
     * @brief Returns vector of NULL values (which is true for offsets where the segment's value is NULL).
     *        Cannot be written to, see value_segment.hpp for details.
     */
    virtual const pmr_vector<bool> &null_values() const = 0;

    inline std::string type_description() const override
    {
        std::string desc = "BaseValueSegment";
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
