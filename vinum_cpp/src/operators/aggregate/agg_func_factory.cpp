#include "agg_func_factory.h"
#include "agg_funcs.h"
#include "common/huge_int.h"

#include <arrow/api.h>

#include <cmath>

using namespace std;

namespace vinum::operators::aggregate {

std::shared_ptr<AbstractAggFunc> agg_func_factory(const AggFuncDef &func,
                                                  const std::shared_ptr<arrow::Schema> &schema) {
    std::shared_ptr<arrow::Field> field = nullptr;

    if (!func.column_name.empty()) {
        field = schema->GetFieldByName(func.column_name);
    }

    auto field_type =
            field
            ? field->type()
            : arrow::uint64();
    auto array_type =
            field
            ? field->type()->id()
            : arrow::Type::UINT64;

    switch (func.func) {
        case AggFuncType::COUNT:
            return make_shared<CountFunc>();
        case AggFuncType::COUNT_STAR:
            return make_shared<CountStarFunc>();
        case AggFuncType::MIN:
        case AggFuncType::MAX: {
            bool is_max = (func.func == AggFuncType::MAX);
            switch (array_type) {
                case arrow::Type::BOOL:
                    return make_shared<MinMaxFunc<bool, arrow::BooleanBuilder>>(is_max, field_type);
                case arrow::Type::INT8:
                    return make_shared<MinMaxFunc<int8_t, arrow::Int8Builder>>(is_max, field_type);
                case arrow::Type::INT16:
                    return make_shared<MinMaxFunc<int16_t, arrow::Int16Builder>>(is_max, field_type);
                case arrow::Type::INT32:
                    return make_shared<MinMaxFunc<int32_t, arrow::Int32Builder>>(is_max, field_type);
                case arrow::Type::INT64:
                    return make_shared<MinMaxFunc<int64_t, arrow::Int64Builder>>(is_max, field_type);
                case arrow::Type::UINT8:
                    return make_shared<MinMaxFunc<uint8_t, arrow::UInt8Builder>>(is_max, field_type);
                case arrow::Type::UINT16:
                    return make_shared<MinMaxFunc<uint16_t, arrow::UInt16Builder>>(is_max, field_type);
                case arrow::Type::UINT32:
                    return make_shared<MinMaxFunc<uint32_t, arrow::UInt32Builder>>(is_max, field_type);
                case arrow::Type::UINT64:
                    return make_shared<MinMaxFunc<uint64_t, arrow::UInt64Builder>>(is_max, field_type);
                case arrow::Type::HALF_FLOAT:
                    return make_shared<MinMaxFunc<uint16_t, arrow::HalfFloatBuilder>>(is_max, field_type);
                case arrow::Type::FLOAT:
                    return make_shared<MinMaxFunc<float_t, arrow::FloatBuilder>>(is_max, field_type);
                case arrow::Type::DOUBLE:
                    return make_shared<MinMaxFunc<double_t, arrow::DoubleBuilder>>(is_max, field_type);
                case arrow::Type::DATE32:
                    return make_shared<MinMaxFunc<int32_t, arrow::Date32Builder>>(is_max, field_type);
                case arrow::Type::DATE64:
                    return make_shared<MinMaxFunc<int64_t, arrow::Date64Builder>>(is_max, field_type);
                case arrow::Type::TIME32:
                    return make_shared<MinMaxFunc<int32_t, arrow::Time32Builder>>(is_max, field_type);
                case arrow::Type::TIME64:
                    return make_shared<MinMaxFunc<int64_t, arrow::Time64Builder>>(is_max, field_type);
                case arrow::Type::TIMESTAMP:
                    return make_shared<MinMaxFunc<int64_t, arrow::TimestampBuilder>>(is_max, field_type);
                case arrow::Type::INTERVAL_DAY_TIME:
                    return make_shared<MinMaxFunc<arrow::DayTimeIntervalType::DayMilliseconds,
                            arrow::DayTimeIntervalBuilder>>(is_max, field_type);
                case arrow::Type::INTERVAL_MONTHS:
                    return make_shared<MinMaxFunc<int32_t, arrow::MonthIntervalBuilder>>(is_max, field_type);
                case arrow::Type::DURATION:
                    return make_shared<MinMaxFunc<int64_t, arrow::DurationBuilder>>(is_max, field_type);
                case arrow::Type::DECIMAL128:
                    return make_shared<StringMinMaxFunc<arrow::Decimal128Builder>>(is_max, field_type);
                case arrow::Type::DECIMAL256:
                    return make_shared<StringMinMaxFunc<arrow::Decimal256Builder>>(is_max, field_type);
                case arrow::Type::STRING:
                    return make_shared<StringMinMaxFunc<arrow::StringBuilder>>(is_max, field_type);
                case arrow::Type::BINARY:
                    return make_shared<StringMinMaxFunc<arrow::BinaryBuilder>>(is_max, field_type);
                case arrow::Type::LARGE_STRING:
                    return make_shared<StringMinMaxFunc<arrow::LargeStringBuilder>>(is_max, field_type);
                case arrow::Type::LARGE_BINARY:
                    return make_shared<StringMinMaxFunc<arrow::LargeBinaryBuilder>>(is_max, field_type);
                case arrow::Type::FIXED_SIZE_BINARY:
                    return make_shared<StringMinMaxFunc<arrow::FixedSizeBinaryBuilder>>(is_max, field_type);
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
                default:
                    throw std::runtime_error("Column data type is not supported by min()/max().");
            }
        }
        case AggFuncType::SUM: {
            switch (array_type) {
                case arrow::Type::INT8:
                    return make_shared<SumFunc<arrow::Int8Type, int64_t, arrow::Int64Builder>>(arrow::int64());
                case arrow::Type::INT16:
                    return make_shared<SumFunc<arrow::Int16Type, int64_t, arrow::Int64Builder>>(arrow::int64());
                case arrow::Type::INT32:
                    return make_shared<SumFunc<arrow::Int32Type, int64_t, arrow::Int64Builder>>(arrow::int64());
                case arrow::Type::INT64:
                    return make_shared<SumOverflowFunc<arrow::Int64Type, int64_t, arrow::Int64Builder>>(arrow::int64());
                case arrow::Type::UINT8:
                    return make_shared<SumFunc<arrow::UInt8Type, uint64_t, arrow::UInt64Builder>>(arrow::uint64());
                case arrow::Type::UINT16:
                    return make_shared<SumFunc<arrow::UInt16Type, uint64_t, arrow::UInt64Builder>>(arrow::uint64());
                case arrow::Type::UINT32:
                    return make_shared<SumFunc<arrow::UInt32Type, uint64_t, arrow::UInt64Builder>>(arrow::uint64());
                case arrow::Type::UINT64:
                    return make_shared<SumOverflowFunc<arrow::UInt64Type, uint64_t, arrow::UInt64Builder>>(arrow::uint64());
                case arrow::Type::HALF_FLOAT:
                    return make_shared<SumFunc<arrow::HalfFloatType, double_t, arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::FLOAT:
                    return make_shared<SumFunc<arrow::FloatType, double_t, arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::DOUBLE:
                    return make_shared<SumFunc<arrow::DoubleType, double_t, arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::TIME32: {
                    auto time_type = std::static_pointer_cast<arrow::TimeType>(field_type);
                    return make_shared<SumFunc<arrow::Time32Type,
                            int32_t,
                            arrow::Time32Builder>>(arrow::time32(time_type->unit()));
                }
                case arrow::Type::TIME64: {
                    auto time_type = std::static_pointer_cast<arrow::TimeType>(field_type);
                    return make_shared<SumFunc<arrow::Time64Type,
                            int64_t,
                            arrow::Time64Builder>>(arrow::time64(time_type->unit()));
                }
                case arrow::Type::DURATION: {
                    auto duration_type = std::static_pointer_cast<arrow::DurationType>(field_type);
                    return make_shared<SumFunc<arrow::DurationType,
                            int64_t,
                            arrow::DurationBuilder>>(arrow::time64(duration_type->unit()));
                }
                case arrow::Type::BOOL:
                case arrow::Type::DATE32:
                case arrow::Type::DATE64:
                case arrow::Type::TIMESTAMP:
                case arrow::Type::INTERVAL_DAY_TIME:
                case arrow::Type::INTERVAL_MONTHS:
                case arrow::Type::DECIMAL128:
                case arrow::Type::DECIMAL256:
                case arrow::Type::STRING:
                case arrow::Type::BINARY:
                case arrow::Type::LARGE_STRING:
                case arrow::Type::LARGE_BINARY:
                case arrow::Type::FIXED_SIZE_BINARY:
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
                default:
                    throw std::runtime_error("Column data type is not supported by sum().");
            }
        }
        case AggFuncType::AVG: {
            switch (array_type) {
                case arrow::Type::INT8:
                    return make_shared<AvgFunc<arrow::Int8Type, int64_t, float_t,
                            arrow::FloatBuilder>>(arrow::float32());
                case arrow::Type::INT16:
                    return make_shared<AvgFunc<arrow::Int16Type, int64_t, float_t,
                            arrow::FloatBuilder>>(arrow::float32());
                case arrow::Type::INT32:
                    return make_shared<AvgFunc<arrow::Int32Type, int64_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::INT64:
                    return make_shared<AvgFunc<arrow::Int64Type, common::hugeint_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::UINT8:
                    return make_shared<AvgFunc<arrow::UInt8Type, uint64_t, float_t,
                            arrow::FloatBuilder>>(arrow::float32());
                case arrow::Type::UINT16:
                    return make_shared<AvgFunc<arrow::UInt16Type, uint64_t, float_t,
                            arrow::FloatBuilder>>(arrow::float32());
                case arrow::Type::UINT32:
                    return make_shared<AvgFunc<arrow::UInt32Type, uint64_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::UINT64:
                    return make_shared<AvgFunc<arrow::UInt64Type, common::hugeint_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::HALF_FLOAT:
                    return make_shared<AvgFunc<arrow::HalfFloatType, double_t, double_t,
                            arrow::HalfFloatBuilder>>(arrow::float64());
                case arrow::Type::FLOAT:
                    return make_shared<AvgFunc<arrow::FloatType, double_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::DOUBLE:
                    return make_shared<AvgFunc<arrow::DoubleType, double_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::TIME32:
                    return make_shared<AvgFunc<arrow::Time32Type, int64_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::TIME64:
                    return make_shared<AvgFunc<arrow::Time64Type, int64_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::DURATION:
                    return make_shared<AvgFunc<arrow::DurationType, int64_t, double_t,
                            arrow::DoubleBuilder>>(arrow::float64());
                case arrow::Type::BOOL:
                case arrow::Type::DATE32:
                case arrow::Type::DATE64:
                case arrow::Type::TIMESTAMP:
                case arrow::Type::INTERVAL_DAY_TIME:
                case arrow::Type::INTERVAL_MONTHS:
                case arrow::Type::DECIMAL128:
                case arrow::Type::DECIMAL256:
                case arrow::Type::STRING:
                case arrow::Type::BINARY:
                case arrow::Type::LARGE_STRING:
                case arrow::Type::LARGE_BINARY:
                case arrow::Type::FIXED_SIZE_BINARY:
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
                default:
                    throw std::runtime_error("Column data type is not supported by avg().");
            }
        }
        case AggFuncType::GROUP_BUILDER: {
            switch (array_type) {
                case arrow::Type::BOOL:
                    return make_shared<GroupBuilder<bool, arrow::BooleanBuilder>>(field_type);
                case arrow::Type::INT8:
                    return make_shared<GroupBuilder<int8_t, arrow::Int8Builder>>(field_type);
                case arrow::Type::INT16:
                    return make_shared<GroupBuilder<int16_t, arrow::Int16Builder>>(field_type);
                case arrow::Type::INT32:
                    return make_shared<GroupBuilder<int32_t, arrow::Int32Builder>>(field_type);
                case arrow::Type::INT64:
                    return make_shared<GroupBuilder<int64_t, arrow::Int64Builder>>(field_type);
                case arrow::Type::UINT8:
                    return make_shared<GroupBuilder<uint8_t, arrow::UInt8Builder>>(field_type);
                case arrow::Type::UINT16:
                    return make_shared<GroupBuilder<uint16_t, arrow::UInt16Builder>>(field_type);
                case arrow::Type::UINT32:
                    return make_shared<GroupBuilder<uint32_t, arrow::UInt32Builder>>(field_type);
                case arrow::Type::UINT64:
                    return make_shared<GroupBuilder<uint64_t, arrow::UInt64Builder>>(field_type);
                case arrow::Type::HALF_FLOAT:
                    return make_shared<GroupBuilder<uint16_t, arrow::HalfFloatBuilder>>(field_type);
                case arrow::Type::FLOAT:
                    return make_shared<GroupBuilder<float_t, arrow::FloatBuilder>>(field_type);
                case arrow::Type::DOUBLE:
                    return make_shared<GroupBuilder<double_t, arrow::DoubleBuilder>>(field_type);
                case arrow::Type::DATE32:
                    return make_shared<GroupBuilder<int32_t, arrow::Date32Builder>>(field_type);
                case arrow::Type::DATE64:
                    return make_shared<GroupBuilder<int64_t, arrow::Date64Builder>>(field_type);
                case arrow::Type::TIME32:
                    return make_shared<GroupBuilder<int32_t, arrow::Time32Builder>>(field_type);
                case arrow::Type::TIME64:
                    return make_shared<GroupBuilder<int64_t, arrow::Time64Builder>>(field_type);
                case arrow::Type::TIMESTAMP:
                    return make_shared<GroupBuilder<int64_t, arrow::TimestampBuilder>>(field_type);
                case arrow::Type::INTERVAL_DAY_TIME:
                    return make_shared<GroupBuilder<arrow::DayTimeIntervalType::DayMilliseconds,
                            arrow::DayTimeIntervalBuilder>>(field_type);
                case arrow::Type::INTERVAL_MONTHS:
                    return make_shared<GroupBuilder<int32_t, arrow::MonthIntervalBuilder>>(field_type);
                case arrow::Type::DURATION:
                    return make_shared<GroupBuilder<int64_t, arrow::DurationBuilder>>(field_type);
                case arrow::Type::DECIMAL128:
                    return make_shared<StringGroupBuilder<arrow::Decimal128Array,
                                        arrow::Decimal128Builder>>(field_type);
                case arrow::Type::DECIMAL256:
                    return make_shared<StringGroupBuilder<arrow::Decimal256Array,
                            arrow::Decimal256Builder>>(field_type);
                case arrow::Type::STRING:
                    return make_shared<StringGroupBuilder<arrow::StringArray,
                            arrow::StringBuilder>>(field_type);
                case arrow::Type::BINARY:
                    return make_shared<StringGroupBuilder<arrow::BinaryArray,
                            arrow::BinaryBuilder>>(field_type);
                case arrow::Type::LARGE_STRING:
                    return make_shared<StringGroupBuilder<arrow::LargeStringArray,
                            arrow::LargeStringBuilder>>(field_type);
                case arrow::Type::LARGE_BINARY:
                    return make_shared<StringGroupBuilder<arrow::LargeBinaryArray,
                            arrow::LargeBinaryBuilder>>(field_type);
                case arrow::Type::FIXED_SIZE_BINARY:
                    return make_shared<StringGroupBuilder<arrow::FixedSizeBinaryArray,
                            arrow::FixedSizeBinaryBuilder>>(field_type);
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
                default:
                    throw std::runtime_error("Column data type is not supported.");
            }
        }
        default:
            throw std::runtime_error("Unrecognized Aggregate function type.");
    }
}


}  // namespace vinum::operators::aggregate
