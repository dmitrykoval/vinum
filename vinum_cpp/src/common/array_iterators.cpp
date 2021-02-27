#include "array_iterators.h"

#include <iostream>

namespace vinum::common {

std::unique_ptr<ArrayIter> array_iter_factory(arrow::Type::type type) {
    switch (type) {
        // -- Numeric types
        case arrow::Type::INT8:
            return std::make_unique<NumericArrayIter<arrow::Int8Type>>();
        case arrow::Type::INT16:
            return std::make_unique<NumericArrayIter<arrow::Int16Type>>();
        case arrow::Type::INT32:
            return std::make_unique<NumericArrayIter<arrow::Int32Type>>();
        case arrow::Type::INT64:
            return std::make_unique<NumericArrayIter<arrow::Int64Type>>();
        case arrow::Type::UINT8:
            return std::make_unique<NumericArrayIter<arrow::UInt8Type>>();
        case arrow::Type::UINT16:
            return std::make_unique<NumericArrayIter<arrow::UInt16Type>>();
        case arrow::Type::UINT32:
            return std::make_unique<NumericArrayIter<arrow::UInt32Type>>();
        case arrow::Type::UINT64:
            return std::make_unique<NumericArrayIter<arrow::UInt64Type>>();
        case arrow::Type::HALF_FLOAT:
            return std::make_unique<NumericArrayIter<arrow::HalfFloatType>>();
        case arrow::Type::FLOAT:
            return std::make_unique<FloatArrayIter<arrow::FloatType>>();
        case arrow::Type::DOUBLE:
            return std::make_unique<FloatArrayIter<arrow::DoubleType>>();
        case arrow::Type::DATE32:
            return std::make_unique<NumericArrayIter<arrow::Date32Type>>();
        case arrow::Type::DATE64:
            return std::make_unique<NumericArrayIter<arrow::Date64Type>>();
        case arrow::Type::TIME32:
            return std::make_unique<NumericArrayIter<arrow::Time32Type>>();
        case arrow::Type::TIME64:
            return std::make_unique<NumericArrayIter<arrow::Time64Type>>();
        case arrow::Type::TIMESTAMP:
            return std::make_unique<NumericArrayIter<arrow::TimestampType>>();
        case arrow::Type::INTERVAL_MONTHS:
            return std::make_unique<NumericArrayIter<arrow::MonthIntervalType>>();
        case arrow::Type::DURATION:
            return std::make_unique<NumericArrayIter<arrow::DurationType>>();
        // -- GetView types, ie types for which corresponding array supports GetView()
        case arrow::Type::BOOL:
            return std::make_unique<GetViewArrayIter<bool, arrow::BooleanArray>>();
        case arrow::Type::INTERVAL_DAY_TIME:
            return std::make_unique<GetViewArrayIter<arrow::DayTimeIntervalType::DayMilliseconds,
                                                     arrow::DayTimeIntervalArray>>();
        case arrow::Type::DECIMAL128:
            return std::make_unique<StringArrayIter<arrow::Decimal128Array>>();
        case arrow::Type::DECIMAL256:
            return std::make_unique<StringArrayIter<arrow::Decimal256Array>>();
        case arrow::Type::STRING:
            return std::make_unique<StringArrayIter<arrow::StringArray>>();
        case arrow::Type::BINARY:
            return std::make_unique<StringArrayIter<arrow::BinaryArray>>();
        case arrow::Type::LARGE_STRING:
            return std::make_unique<StringArrayIter<arrow::LargeStringArray>>();
        case arrow::Type::LARGE_BINARY:
            return std::make_unique<StringArrayIter<arrow::LargeBinaryArray>>();
        case arrow::Type::FIXED_SIZE_BINARY:
            return std::make_unique<StringArrayIter<arrow::FixedSizeBinaryArray>>();
        // -- Generic types - types, where only IsNull() is supported by generic arrow::Array
        case arrow::Type::STRUCT:
        case arrow::Type::LIST:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::MAP:
        case arrow::Type::DENSE_UNION:
        case arrow::Type::SPARSE_UNION:
        case arrow::Type::DICTIONARY:
        case arrow::Type::EXTENSION:
        case arrow::Type::NA:
            return std::make_unique<GenericArrayIter>();
        default:
            throw std::runtime_error("Unsupported data type for aggregation column.");
    }
}

}  // namespace vinum::common
