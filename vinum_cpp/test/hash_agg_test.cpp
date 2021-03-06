#include <iostream>
#include <vector>

#include <arrow/csv/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/table.h>

#include <gtest/gtest.h>
#include <arrow/testing/gtest_util.h>

#include "operators/aggregate/base_aggregate.h"
#include "operators/aggregate/generic_hash_aggregate.h"
#include "operators/aggregate/single_numerical_hash_aggregate.h"
#include "operators/aggregate/multi_numerical_hash_aggregate.h"
#include "operators/aggregate/one_group_aggregate.h"
#include "common/util.h"

using AggFuncDef = vinum::operators::aggregate::AggFuncDef;
using AggFuncType = vinum::operators::aggregate::AggFuncType;
using SingleNumericalHashAggregate = vinum::operators::aggregate::SingleNumericalHashAggregate;
using MultiNumericalHashAggregate = vinum::operators::aggregate::MultiNumericalHashAggregate;
using GenericHashAggregate = vinum::operators::aggregate::GenericHashAggregate;
using OneGroupAggregate = vinum::operators::aggregate::OneGroupAggregate;

struct AggTestDef {
    std::vector<std::string> groupby_cols;
    std::vector<std::string> agg_cols;
    std::vector<AggFuncDef> agg_funcs;

    std::shared_ptr<arrow::RecordBatch> result_batch;
};

template <typename T, typename BUILDER>
std::shared_ptr<arrow::Array> create_array(initializer_list<T> vals,
                                           initializer_list<bool> nulls) {
    BUILDER builder;
    std::shared_ptr<arrow::Array> array;
    RAISE_ON_ARROW_FAILURE(builder.AppendValues(
            std::vector<T>(vals),
            std::vector<bool>(nulls)
    ));
    RAISE_ON_ARROW_FAILURE(builder.Finish(&array));
    return array;
}

template <typename T, typename BUILDER>
std::shared_ptr<arrow::Array> create_array(initializer_list<T> vals,
                                           initializer_list<bool> nulls,
                                           const std::shared_ptr<arrow::DataType> &dtype) {
    BUILDER builder(dtype, arrow::default_memory_pool());
    std::shared_ptr<arrow::Array> array;
    RAISE_ON_ARROW_FAILURE(builder.AppendValues(
            std::vector<T>(vals),
            std::vector<bool>(nulls)
    ));
    RAISE_ON_ARROW_FAILURE(builder.Finish(&array));
    return array;
}

template<typename T, typename BUILDER>
std::shared_ptr<arrow::Array> create_flat_array(initializer_list<T> vals,
                                                initializer_list<uint8_t> nulls) {
    BUILDER builder;
    std::shared_ptr<arrow::Array> array;
    RAISE_ON_ARROW_FAILURE(builder.AppendValues(
            std::vector<T>(vals),
            std::vector<uint8_t>(nulls).data()
    ));
    RAISE_ON_ARROW_FAILURE(builder.Finish(&array));
    return array;
}


::arrow::Status sort_table(const std::shared_ptr<arrow::RecordBatch> &batch,
                           std::shared_ptr<arrow::RecordBatch> &sorted_batch,
                           const initializer_list<int> sort_cols = {0}) {
    if (batch->num_rows() == 0) {
        sorted_batch = batch;
        return arrow::Status::OK();
    }

    auto col_names = batch->schema()->field_names();
    std::vector<arrow::compute::SortKey> sort_keys;
    for (int col_idx : sort_cols) {
        sort_keys.push_back(
                arrow::compute::SortKey{col_names[col_idx], arrow::compute::SortOrder::Ascending}
        );
    }

    arrow::compute::SortOptions sort_options(sort_keys);
    auto sort_result = arrow::compute::SortIndices(arrow::Datum{batch}, sort_options);
    if (!sort_result.ok()) {
        return sort_result.status();
    }
    auto sorted_indices = sort_result.ValueOrDie();

    arrow::Datum sorted_datum;
    ARROW_ASSIGN_OR_RAISE(
            sorted_datum,
            arrow::compute::Take(arrow::Datum(batch), arrow::Datum(sorted_indices))
    );
    sorted_batch = std::move(sorted_datum).record_batch();

    return arrow::Status::OK();
}

std::shared_ptr<arrow::RecordBatch> aggregate_and_sort(vinum::operators::aggregate::BaseAggregate& agg,
                                                       const std::shared_ptr<arrow::Table>& table,
                                                       const initializer_list<int> sort_cols = {0}) {
    int mid = table->num_rows() >> 1;
    auto reader = arrow::TableBatchReader(*table);
    if (mid > 0) {
        reader.set_chunksize(mid);
    }

    decltype(reader.Next()) result;
    while ((result = reader.Next()) != nullptr) {
        auto batch = result.ValueOrDie();
        agg.Next(batch);
    }

    std::shared_ptr<arrow::RecordBatch> res_batch = agg.Result();
    EXPECT_NE(table, nullptr);

    std::shared_ptr<arrow::RecordBatch> sorted_batch;
    auto sort_status = sort_table(res_batch, sorted_batch, sort_cols);

    EXPECT_EQ(sort_status, arrow::Status::OK());
    EXPECT_NE(sorted_batch, nullptr);

    return sorted_batch;
}


class HashAggTestFixture : public ::testing::Test {
protected:
    void SetUp() override {
        this->test_table = this->CreateTestTable();
        this->overflow_test_table = this->CreateOverflowTestTable();
        this->empty_test_batch = this->CreateEmptyTestRecordBatch();

        this->string_grp__double_arg_funcs = this->CreateStringGrp_DoubleArgFuncs();
        this->double_grp__int_arg_funcs = this->CreateDoubleGrp_IntArgFuncs();
        this->int64_grp__int_overflow_arg_funcs = this->CreateInt64Grp_IntOverflowArgFuncs();
        this->int64_grp__string_arg_funcs = this->CreateInt64Grp_StringArgFuncs();
        this->int8_grp__double_arg_funcs = this->CreateInt8Grp_DoubleArgFuncs();
        this->multi_int_grp__date_arg_funcs = this->CreateMultiIntGrp_DateArgFuncs();
        this->boolean_grp__date_arg_funcs = this->CreateBooleanGrp_DateArgFuncs();
        this->neg_int64_grp__timestamp_arg_funcs = this->CreateNegInt64Grp_TimestampArgFuncs();
        this->no_grp__agg_funcs = this->CreateNoGrp_AggFuncs();
        this->empty_table__agg_funcs = this->CreateEmptyTable_AggFuncs();
    }

    std::shared_ptr<arrow::Table> CreateTestTable() {
        auto ids = create_array<int64_t, arrow::Int64Builder>({1, 2, 3, 4, 5, 6, 7, 8},
                                                              {true, true, true, true, true, true, true, true});

        auto timestamp_int64 = create_array<int64_t, arrow::Int64Builder>(
                {1602127614, 1602217613, 1602304012, 1602390411, 0, 1602563209, 0, 1602736007},
                {true, true, true, true, false, true, false, true});

        auto date = create_flat_array<std::string, arrow::StringBuilder>(
                {"", "2020-10-09T04:26:53", "2020-10-10T04:26:52", "2020-10-11T04:26:51",
                 "2020-10-12T04:26:50", "2020-10-13T04:26:49", "0", "2020-10-15T04:26:47"},
                {false, true, true, true, true, true, false, true});

        auto is_vendor = create_array<uint8_t, arrow::BooleanBuilder>(
                {true, true, false, false, true, false, false, false},
                {true, true, true, false, true, false, false, false});

        auto city_from = create_flat_array<std::string, arrow::StringBuilder>(
                {"", "Munich", "", "San Francisco", "Berlin", "Munich", "Berlin", "Berlin"},
                {false, true, false, true, true, true, true, true});

        auto city_to = create_flat_array<std::string, arrow::StringBuilder>(
                {"Munich", "Riva", "Naples", "Naples", "Riva", "Riva", "Munich", "Munich"},
                {true, true, true, true, true, true, true, true});

        auto lat = create_array<double_t, arrow::DoubleBuilder>(
                {52.51, 48.51, 44.89, 42.89, 44.89, 48.51, 44.89, 52.51},
                {true, true, true, true, true, true, true, true});

        auto lng = create_array<double_t, arrow::DoubleBuilder>(
                {13.66, 12.3, 14.23, 15.89, 14.23, 12.3, 14.23, 13.66},
                {true, true, true, true, true, true, true, true});

        auto name = create_flat_array<std::string, arrow::StringBuilder>(
                {"Joe", "", "Joseph", "Joseph", "", "Jonas", "Joseph", "Joe"},
                {true, false, true, true, false, true, true, true});

        auto total = create_array<double_t, arrow::DoubleBuilder>(
                {0, 143.15, 33.4, 53.1, 0, 0, 33.4, 0},
                {false, true, true, true, false, false, true, false});

        auto grp8_int = create_array<int8_t, arrow::Int8Builder>(
                {0, 2, 7, 3, 1, 2, 1, 1},
                {false, true, false, true, true, true, true, true});

        auto grp8_neg_int = create_array<int8_t, arrow::Int8Builder>(
                {0, -1, -1, 3, 1, -1, 1, 1},
                {false, true, false, true, true, true, true, true});

        auto date64 = create_array<int64_t, arrow::Date64Builder>(
                {1611664426519, 1611664426386, 1611664426519, 1611664416382,
                 1611664416382, 1611664426519, 1611664416382, 1611664426386},
                {false, true, true, true, false, true, true, true});

        auto time32 = create_array<int32_t, arrow::Time32Builder>(
                {130, 7, 41, 7, 41, 130, 7, 130},
                {false, true, false, true, true, true, false, true},
                arrow::time32(arrow::TimeUnit::MILLI));

        auto ts_grp = create_array<int64_t, arrow::TimestampBuilder>(
                {1611664420588, 1611663913570, 1611663913570, 1611664414385,
                 1611664420588, 130, 1611664420588, 1611664414385},
                {true, true, false, true, true, false, false, true},
                arrow::time32(arrow::TimeUnit::MILLI));

        auto grp_neg_int64 = create_array<int64_t, arrow::Int64Builder>(
                {-9223372036854775807, -9223372036854775806, 9223372036854775807, -9223372036854775807,
                 9223372036854775806, 9223372036854775806, 9223372036854775807, -9223372036854775806},
                {true, true, true, true, true, true, true, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("id", arrow::int64()),
                arrow::field("timestamp_int64", arrow::int64()),
                arrow::field("date", arrow::utf8()),
                arrow::field("is_vendor", arrow::boolean()),
                arrow::field("city_from", arrow::utf8()),
                arrow::field("city_to", arrow::utf8()),
                arrow::field("lat", arrow::float64()),
                arrow::field("lng", arrow::float64()),
                arrow::field("name", arrow::utf8()),
                arrow::field("total", arrow::float64()),
                arrow::field("grp_int8", arrow::int8()),
                arrow::field("grp_neg_int8", arrow::int8()),
                arrow::field("date64", arrow::date64()),
                arrow::field("time32", arrow::time32(arrow::TimeUnit::MILLI)),
                arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("grp_neg_int64", arrow::int64()),

        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        return arrow::Table::Make(schema, {
                ids, timestamp_int64, date, is_vendor, city_from, city_to, lat, lng,
                name, total, grp8_int, grp8_neg_int, date64, time32, ts_grp, grp_neg_int64
        });
    }

    std::shared_ptr<arrow::Table> CreateOverflowTestTable() {
        auto ids = create_array<int64_t, arrow::Int64Builder>(
                {1, 2, 1, 1, 2, 2, 1, 1},
                {true, true, true, true, true, true, true, true});

        auto int_64 = create_array<int64_t, arrow::Int64Builder>(
                {9223372036854775807, 9223372036854775806, 9223372036854775805, 9223372036854775804,
                 9223372036854775803, 9223372036854775802, 9223372036854775801, 9223372036854775799},
                {true, true, true, true, false, true, false, true});

        auto uint_64 = create_array<uint64_t, arrow::UInt64Builder>(
                {18446744073709551615U, 18446744073709551614U, 18446744073709551613U, 18446744073709551612U,
                 18446744073709551611U, 18446744073709551610U, 18446744073709551609U, 18446744073709551608U},
                {true, true, true, true, false, true, false, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("id", arrow::int64()),
                arrow::field("int_64", arrow::int64()),
                arrow::field("uint_64", arrow::uint64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        return arrow::Table::Make(schema, {ids, int_64, uint_64});
    }

    std::shared_ptr<arrow::RecordBatch> CreateEmptyTestRecordBatch() {
        auto ids = create_array<int64_t, arrow::Int64Builder>({}, {});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("id", arrow::int64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        return arrow::RecordBatch::Make(schema, 0, {ids});
    }


    AggTestDef CreateStringGrp_DoubleArgFuncs() {
        std::vector<std::string> groupby_cols({"city_from"});
        std::vector<std::string> agg_cols({"city_from"});
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT_STAR, "", "count"},
                 AggFuncDef{AggFuncType::COUNT, "total", "count_9"},
                 AggFuncDef{AggFuncType::MIN, "lat", "min_6"},
                 AggFuncDef{AggFuncType::MAX, "lat", "max_6"},
                 AggFuncDef{AggFuncType::SUM, "lat", "sum_6"},
                 AggFuncDef{AggFuncType::AVG, "lat", "avg_6"},
         });

        auto city_from = create_flat_array<std::string, arrow::StringBuilder>(
                {"Berlin", "Munich", "San Francisco", ""},
                {true, true, true, false});

        auto count = create_array<uint64_t, arrow::UInt64Builder>(
                {3, 2, 1, 2},
                {true, true, true, true});

        auto count_total = create_array<uint64_t, arrow::UInt64Builder>(
                {1, 1, 1, 1},
                {true, true, true, true});

        auto min = create_array<double_t, arrow::DoubleBuilder>(
                {44.89, 48.51, 42.89, 44.89},
                {true, true, true, true});

        auto max = create_array<double_t, arrow::DoubleBuilder>(
                {52.51, 48.51, 42.89, 52.51},
                {true, true, true, true});

        auto sum = create_array<double_t, arrow::DoubleBuilder>(
                {142.29, 97.02, 42.89, 97.4},
                {true, true, true, true});

        auto avg = create_array<double_t, arrow::DoubleBuilder>(
                {47.43, 48.51, 42.89, 48.7},
                {true, true, true, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("city_from", arrow::utf8()),
                arrow::field("count", arrow::uint64()),
                arrow::field("count_total", arrow::uint64()),
                arrow::field("min", arrow::float64()),
                arrow::field("max", arrow::float64()),
                arrow::field("sum", arrow::float64()),
                arrow::field("avg", arrow::float64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, city_from->length(), {city_from, count, count_total, min, max, sum, avg});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateDoubleGrp_IntArgFuncs() {
        std::vector<std::string> groupby_cols({"lat"});
        std::vector<std::string> agg_cols({"lat"});
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT_STAR, "", "count"},
                 AggFuncDef{AggFuncType::MIN, "id", "min_0"},
                 AggFuncDef{AggFuncType::MAX, "id", "max_0"},
                 AggFuncDef{AggFuncType::SUM, "id", "sum_0"},
                 AggFuncDef{AggFuncType::AVG, "id", "avg_0"},
         });

        auto lat = create_array<double_t, arrow::DoubleBuilder>(
                {42.89, 44.89, 48.51, 52.51},
                {true, true, true, true});

        auto count = create_array<uint64_t, arrow::UInt64Builder>(
                {1, 3, 2, 2},
                {true, true, true, true});

        auto min = create_array<int64_t, arrow::Int64Builder>(
                {4, 3, 2, 1},
                {true, true, true, true});

        auto max = create_array<int64_t, arrow::Int64Builder>(
                {4, 7, 6, 8},
                {true, true, true, true});

        auto sum = create_array<int64_t, arrow::Int64Builder>(
                {4, 15, 8, 9},
                {true, true, true, true});

        auto avg = create_array<double_t, arrow::DoubleBuilder>(
                {4.0, 5.0, 4.0, 4.5},
                {true, true, true, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("lat", arrow::float64()),
                arrow::field("count", arrow::uint64()),
                arrow::field("min", arrow::int64()),
                arrow::field("max", arrow::int64()),
                arrow::field("sum", arrow::int64()),
                arrow::field("avg", arrow::float64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, lat->length(),{lat, count, min, max, sum, avg});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateInt64Grp_IntOverflowArgFuncs() {
        std::vector<std::string> groupby_cols({"id"});
        std::vector<std::string> agg_cols({"id"});
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::SUM, "int_64", "sum_1"},
                 AggFuncDef{AggFuncType::SUM, "uint_64", "sum_2"},
                 AggFuncDef{AggFuncType::AVG, "int_64", "avg_1"},
                 AggFuncDef{AggFuncType::AVG, "uint_64", "avg_2"},
         });

        auto grp = create_array<int64_t, arrow::Int64Builder>(
                {1, 2},
                {true, true});

        std::shared_ptr<arrow::Array> array;
        arrow::Decimal128Builder decimalBuilder(arrow::decimal128(arrow::Decimal128Type::kMaxPrecision, 0));

        // -- SUM(int64) --
        RAISE_ON_ARROW_FAILURE(decimalBuilder.Append(arrow::Decimal128("36893488147419103215")));
        RAISE_ON_ARROW_FAILURE(decimalBuilder.Append(arrow::Decimal128("18446744073709551608")));
        RAISE_ON_ARROW_FAILURE(decimalBuilder.Finish(&array));
        auto sum_1 = array;
        decimalBuilder.Reset();

        // -- SUM(uint64) --
        RAISE_ON_ARROW_FAILURE(decimalBuilder.Append(arrow::Decimal128("73786976294838206448")));
        RAISE_ON_ARROW_FAILURE(decimalBuilder.Append(arrow::Decimal128("36893488147419103224")));
        RAISE_ON_ARROW_FAILURE(decimalBuilder.Finish(&array));
        auto sum_2 = array;
        decimalBuilder.Reset();

        auto avg_1 = create_array<double_t, arrow::DoubleBuilder>(
                {9.223372036854776e+18, 9.223372036854776e+18},
                {true, true});

        auto avg_2 = create_array<double_t, arrow::DoubleBuilder>(
                {1.8446744073709552e+19, 1.8446744073709552e+19},
                {true, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("grp", arrow::int64()),
                arrow::field("sum_1", arrow::decimal128(arrow::Decimal128Type::kMaxPrecision, 0)),
                arrow::field("sum_2", arrow::decimal128(arrow::Decimal128Type::kMaxPrecision, 0)),
                arrow::field("avg_1", arrow::float64()),
                arrow::field("avg_2", arrow::float64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, grp->length(), {grp, sum_1, sum_2, avg_1, avg_2});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateInt64Grp_StringArgFuncs() {
        std::vector<std::string> groupby_cols({"id"});
        std::vector<std::string> agg_cols({"id"});
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT, "date", "count_2"},
                 AggFuncDef{AggFuncType::MIN, "date", "min_2"},
                 AggFuncDef{AggFuncType::MAX, "date", "max_2"},
         });

        auto id = create_array<int64_t, arrow::Int64Builder>(
                {1, 2, 3, 4, 5, 6, 7, 8},
                {true, true, true, true, true, true, true, true});

        auto count_date = create_array<uint64_t, arrow::UInt64Builder>(
                {0, 1, 1, 1, 1, 1, 0, 1},
                {true, true, true, true, true, true, true, true});

        auto min = create_flat_array<std::string, arrow::StringBuilder>(
                {"", "2020-10-09T04:26:53", "2020-10-10T04:26:52",
                 "2020-10-11T04:26:51", "2020-10-12T04:26:50",
                 "2020-10-13T04:26:49", "", "2020-10-15T04:26:47"},
                {false, true, true, true, true, true, false, true});

        auto max = create_flat_array<std::string, arrow::StringBuilder>(
                {"", "2020-10-09T04:26:53", "2020-10-10T04:26:52",
                 "2020-10-11T04:26:51", "2020-10-12T04:26:50",
                 "2020-10-13T04:26:49", "", "2020-10-15T04:26:47"},
                {false, true, true, true, true, true, false, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("id", arrow::int64()),
                arrow::field("count", arrow::uint64()),
                arrow::field("min", arrow::utf8()),
                arrow::field("max", arrow::utf8()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, id->length(),{id, count_date, min, max});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateInt8Grp_DoubleArgFuncs() {
        std::vector<std::string> groupby_cols({"grp_int8"});
        std::vector<std::string> agg_cols({"grp_int8"});
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT_STAR, "", "count"},
                 AggFuncDef{AggFuncType::COUNT, "total", "count_9"},
                 AggFuncDef{AggFuncType::MIN, "lat", "min_6"},
                 AggFuncDef{AggFuncType::MAX, "lat", "max_6"},
                 AggFuncDef{AggFuncType::SUM, "lat", "sum_6"},
                 AggFuncDef{AggFuncType::AVG, "lat", "avg_6"},
         });

        auto grp_int8 = create_array<int8_t, arrow::Int8Builder>(
                {1, 2, 3, 0},
                {true, true, true, false});

        auto count = create_array<uint64_t, arrow::UInt64Builder>(
                {3, 2, 1, 2},
                {true, true, true, true});

        auto count_total = create_array<uint64_t, arrow::UInt64Builder>(
                {1, 1, 1, 1},
                {true, true, true, true});

        auto min = create_array<double_t, arrow::DoubleBuilder>(
                {44.89, 48.51, 42.89, 44.89},
                {true, true, true, true});

        auto max = create_array<double_t, arrow::DoubleBuilder>(
                {52.51, 48.51, 42.89, 52.51},
                {true, true, true, true});

        auto sum = create_array<double_t, arrow::DoubleBuilder>(
                {142.29, 97.02, 42.89, 97.4},
                {true, true, true, true});

        auto avg = create_array<double_t, arrow::DoubleBuilder>(
                {47.43, 48.51, 42.89, 48.7},
                {true, true, true, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("grp", arrow::int8()),
                arrow::field("count", arrow::uint64()),
                arrow::field("count_total", arrow::uint64()),
                arrow::field("min", arrow::float64()),
                arrow::field("max", arrow::float64()),
                arrow::field("sum", arrow::float64()),
                arrow::field("avg", arrow::float64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, grp_int8->length(), {grp_int8, count, count_total, min, max, sum, avg});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateMultiIntGrp_DateArgFuncs() {
        std::vector<std::string> groupby_cols({"grp_neg_int8", "date64", "time32", "timestamp"});
        std::vector<std::string> agg_cols({"grp_neg_int8", "date64", "time32", "timestamp"});
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT_STAR, "", "count"},
                 AggFuncDef{AggFuncType::MIN, "date64", "min_12"},
                 AggFuncDef{AggFuncType::MAX, "timestamp", "max_14"},
                 AggFuncDef{AggFuncType::SUM, "time32", "sum_13"},
         });

        auto grp8_neg_int = create_array<int8_t, arrow::Int8Builder>(
                {-1, -1, 1, 1, 1, 3, 0, 0},
                {true, true, true, true, true, true, false, false});

        auto date64 = create_array<int64_t, arrow::Date64Builder>(
                {1611664426386, 1611664426519, 1611664416382, 1611664426386,
                 0, 1611664416382, 1611664426519, 0},
                {true, true, true, true, false, true, true, false});

        auto time32 = create_array<int32_t, arrow::Time32Builder>(
                {7, 130, 0, 130, 41, 7, 0, 0},
                {true, true, false, true, true, true, false, false},
                arrow::time32(arrow::TimeUnit::MILLI));

        auto ts_grp = create_array<int64_t, arrow::TimestampBuilder>(
                {1611663913570, 0, 0, 1611664414385,
                 1611664420588, 1611664414385, 0, 1611664420588},
                {true, false, false, true, true, true, false, true},
                arrow::timestamp(arrow::TimeUnit::MILLI));

        auto count = create_array<uint64_t, arrow::UInt64Builder>(
                {1, 1, 1, 1, 1, 1, 1, 1},
                {true, true, true, true, true, true, true, true});

        auto min_date64 = create_array<int64_t, arrow::Date64Builder>(
                {1611664426386, 1611664426519, 1611664416382, 1611664426386,
                 0, 1611664416382, 1611664426519, 0},
                {true, true, true, true, false, true, true, false});

        auto max_ts = create_array<int64_t, arrow::TimestampBuilder>(
                {1611663913570, 0, 0, 1611664414385,
                 1611664420588, 1611664414385, 0, 1611664420588},
                {true, false, false, true, true, true, false, true},
                arrow::timestamp(arrow::TimeUnit::MILLI));

        auto sum_time32 = create_array<int32_t, arrow::Time32Builder>(
                {7, 130, 0, 130, 41, 7, 0, 0},
                {true, true, false, true, true, true, false, false},
                arrow::time32(arrow::TimeUnit::MILLI));

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("grp_neg_int8", arrow::int8()),
                arrow::field("date64", arrow::date64()),
                arrow::field("time32", arrow::time32(arrow::TimeUnit::MILLI)),
                arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("count", arrow::uint64()),
                arrow::field("min_date64", arrow::date64()),
                arrow::field("max_timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("sum_time32", arrow::time32(arrow::TimeUnit::MILLI)),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema,
                                              date64->length(),
                                      {grp8_neg_int, date64, time32, ts_grp,
                                               count, min_date64, max_ts, sum_time32});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateBooleanGrp_DateArgFuncs() {
        std::vector<std::string> groupby_cols({"is_vendor"});
        std::vector<std::string> agg_cols({"is_vendor"});
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT_STAR, "", "count"},
                 AggFuncDef{AggFuncType::MIN, "time32", "min_12"},
                 AggFuncDef{AggFuncType::MAX, "time32", "max_14"},
                 AggFuncDef{AggFuncType::SUM, "time32", "sum_13"},
                 AggFuncDef{AggFuncType::AVG, "time32", "sum_13"},
         });

        auto is_vendor = create_array<uint8_t, arrow::BooleanBuilder>(
                {false, true, false},
                {true, true, false});

        auto count = create_array<uint64_t, arrow::UInt64Builder>(
                {1, 3, 4},
                {true, true, true});

        auto min_time32 = create_array<int32_t, arrow::Time32Builder>(
                {0, 7, 7},
                {false, true, true},
                arrow::time32(arrow::TimeUnit::MILLI));

        auto max_time32 = create_array<int32_t, arrow::Time32Builder>(
                {0, 41, 130},
                {false, true, true},
                arrow::time32(arrow::TimeUnit::MILLI));

        auto sum_time32 = create_array<int32_t, arrow::Time32Builder>(
                {0, 48, 267},
                {false, true, true},
                arrow::time32(arrow::TimeUnit::MILLI));

        auto avg_time32 = create_array<double_t, arrow::DoubleBuilder>(
                {0, 24.0, 89.0},
                {false, true, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("vendor_id", arrow::boolean()),
                arrow::field("count", arrow::uint64()),
                arrow::field("min_time32", arrow::time32(arrow::TimeUnit::MILLI)),
                arrow::field("max_time32", arrow::time32(arrow::TimeUnit::MILLI)),
                arrow::field("sum_time32", arrow::time32(arrow::TimeUnit::MILLI)),
                arrow::field("avg_time32", arrow::float64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, is_vendor->length(), {is_vendor, count, min_time32, max_time32, sum_time32, avg_time32});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateNegInt64Grp_TimestampArgFuncs() {
        std::vector<std::string> groupby_cols({"grp_neg_int64"});
        std::vector<std::string> agg_cols({"grp_neg_int64"});
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT_STAR, "", "count"},
                 AggFuncDef{AggFuncType::COUNT, "timestamp", "count_ts"},
                 AggFuncDef{AggFuncType::MIN, "timestamp", "min_14"},
                 AggFuncDef{AggFuncType::MAX, "timestamp", "max_14"},
                 AggFuncDef{AggFuncType::AVG, "grp_int8", "avg_10"},
                 AggFuncDef{AggFuncType::AVG, "grp_neg_int8", "avg_11"},
         });

        auto grp_neg_int64 = create_array<int64_t, arrow::Int64Builder >(
                {-9223372036854775807, -9223372036854775806, 9223372036854775806, 9223372036854775807},
                {true, true, true, true});

        auto count = create_array<uint64_t, arrow::UInt64Builder >(
                {2, 2, 2, 2},
                {true, true, true, true});

        auto count_ts = create_array<uint64_t, arrow::UInt64Builder >(
                {2, 2, 1, 0},
                {true, true, true, true});

        auto min_ts = create_array<int64_t, arrow::TimestampBuilder>(
                {1611664414385, 1611663913570, 1611664420588, 0},
                {true, true, true, false},
                arrow::timestamp(arrow::TimeUnit::MILLI));

        auto max_ts = create_array<int64_t, arrow::TimestampBuilder>(
                {1611664420588, 1611664414385, 1611664420588, 0},
                {true, true, true, false},
                arrow::timestamp(arrow::TimeUnit::MILLI));

        auto avg_grp_int8 = create_array<float_t, arrow::FloatBuilder>(
                {3.0, 1.5, 1.5, 1.0},
                {true, true, true, true});

        auto avg_grp_neg_int8 = create_array<float_t, arrow::FloatBuilder>(
                {3.0, 0, 0, 1.0},
                {true, true, true, true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("grp_neg_int64", arrow::int64()),
                arrow::field("count", arrow::uint64()),
                arrow::field("count_ts", arrow::uint64()),
                arrow::field("min_ts", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("max_ts", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("avg_grp_int8", arrow::float32()),
                arrow::field("avg_grp_neg_int8", arrow::float32()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, min_ts->length(),
                                              {grp_neg_int64, count, count_ts,
                                               min_ts, max_ts, avg_grp_int8, avg_grp_neg_int8});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateNoGrp_AggFuncs() {
        std::vector<std::string> groupby_cols{};
        std::vector<std::string> agg_cols{};
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT_STAR, "", "count_star"},
                 AggFuncDef{AggFuncType::COUNT, "timestamp_int64", "count_int64"},
                 AggFuncDef{AggFuncType::MIN, "timestamp_int64", "min_int64"},
                 AggFuncDef{AggFuncType::MAX, "timestamp_int64", "max_int64"},
                 AggFuncDef{AggFuncType::SUM, "timestamp_int64", "sum_int64"},
                 AggFuncDef{AggFuncType::AVG, "timestamp_int64", "avg_int64"},
         });

        auto count_star = create_array<uint64_t, arrow::UInt64Builder >(
                {8},
                {true});

        auto count_int64 = create_array<uint64_t, arrow::UInt64Builder >(
                {6},
                {true});

        auto min_int64 = create_array<int64_t, arrow::Int64Builder>(
                {1602127614},
                {true});

        auto max_int64 = create_array<int64_t, arrow::Int64Builder>(
                {1602736007},
                {true});

        auto sum_int64 = create_array<int64_t, arrow::Int64Builder>(
                {9614338866},
                {true});

        auto avg_int64 = create_array<double_t, arrow::DoubleBuilder>(
                {1602389811.0},
                {true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("count_star", arrow::uint64()),
                arrow::field("count_int64", arrow::uint64()),
                arrow::field("min_int64", arrow::int64()),
                arrow::field("max_int64", arrow::int64()),
                arrow::field("sum_int64", arrow::int64()),
                arrow::field("avg_int64", arrow::float64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, count_star->length(),
                                        {count_star, count_int64, min_int64, max_int64,
                                         sum_int64, avg_int64});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }

    AggTestDef CreateEmptyTable_AggFuncs() {
        std::vector<std::string> groupby_cols{};
        std::vector<std::string> agg_cols{};
        std::vector<AggFuncDef> agg_funcs({
                 AggFuncDef{AggFuncType::COUNT_STAR, "", "count_star"},
         });

        auto count_star = create_array<uint64_t, arrow::UInt64Builder >(
                {0},
                {true});

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("count_star", arrow::uint64()),
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto batch = arrow::RecordBatch::Make(schema, count_star->length(), {count_star});
        return AggTestDef{groupby_cols, agg_cols, agg_funcs, batch};
    }


    std::shared_ptr<arrow::Table> test_table;
    std::shared_ptr<arrow::Table> overflow_test_table;
    std::shared_ptr<arrow::RecordBatch> empty_test_batch;
    AggTestDef string_grp__double_arg_funcs;
    AggTestDef double_grp__int_arg_funcs;
    AggTestDef int64_grp__int_overflow_arg_funcs;
    AggTestDef int64_grp__string_arg_funcs;
    AggTestDef int8_grp__double_arg_funcs;
    AggTestDef multi_int_grp__date_arg_funcs;
    AggTestDef boolean_grp__date_arg_funcs;
    AggTestDef neg_int64_grp__timestamp_arg_funcs;
    AggTestDef no_grp__agg_funcs;
    AggTestDef empty_table__agg_funcs;
};


std::shared_ptr<arrow::Table> readCsv() {
    const char *csv_filename = "train.csv";

    std::cout << "* Reading CSV file '" << csv_filename << "' into table" << std::endl;
    auto input_file = arrow::io::ReadableFile::Open(csv_filename);
    auto csv_reader_res = arrow::csv::TableReader::Make(arrow::default_memory_pool(),
                                                        input_file.ValueOrDie(),
                                                        arrow::csv::ReadOptions::Defaults(),
                                                        arrow::csv::ParseOptions::Defaults(),
                                                        arrow::csv::ConvertOptions::Defaults());
    auto csv_reader = csv_reader_res.ValueOrDie();
    auto table_res = csv_reader->Read();
    auto table = table_res.ValueOrDie();

    std::cout << "* Read table:" << table << std::endl;
    return table;
}


TEST_F(HashAggTestFixture, Generic_StringGrp_DoubleArgFuncs) {
    auto test_def = this->string_grp__double_arg_funcs;
    GenericHashAggregate agg(test_def.groupby_cols,
                             test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Single_DoubleGrp_IntArgFuncs) {
    auto test_def = this->double_grp__int_arg_funcs;
    SingleNumericalHashAggregate agg(test_def.groupby_cols,
                                     test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Multi_DoubleGrp_IntArgFuncs) {
    auto test_def = this->double_grp__int_arg_funcs;
    MultiNumericalHashAggregate agg(test_def.groupby_cols,
                                    test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Generic_DoubleGrp_IntArgFuncs) {
    auto test_def = this->double_grp__int_arg_funcs;
    GenericHashAggregate agg(test_def.groupby_cols,
                             test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Single_Int64Grp_IntOverflowArgFuncs) {
    auto test_def = this->int64_grp__int_overflow_arg_funcs;
    SingleNumericalHashAggregate agg(test_def.groupby_cols,
                                     test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, overflow_test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Single_Int64Grp_StringArgFuncs) {
    auto test_def = this->int64_grp__string_arg_funcs;
    SingleNumericalHashAggregate agg(test_def.groupby_cols,
                                     test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Multi_Int64Grp_StringArgFuncs) {
    auto test_def = this->int64_grp__string_arg_funcs;
    MultiNumericalHashAggregate agg(test_def.groupby_cols,
                                    test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Generic_Int64Grp_StringArgFuncs) {
    auto test_def = this->int64_grp__string_arg_funcs;
    GenericHashAggregate agg(test_def.groupby_cols,
                             test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Single_Int8Grp_DoubleArgFuncs) {
    auto test_def = this->int8_grp__double_arg_funcs;
    SingleNumericalHashAggregate agg(test_def.groupby_cols,
                                     test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Multi_Int8Grp_DoubleArgFuncs) {
    auto test_def = this->int8_grp__double_arg_funcs;
    MultiNumericalHashAggregate agg(test_def.groupby_cols,
                                    test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Generic_Int8Grp_DoubleArgFuncs) {
    auto test_def = this->int8_grp__double_arg_funcs;
    GenericHashAggregate agg(test_def.groupby_cols,
                             test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Multi_MultiIntGrp_DateArgFuncs) {
    auto test_def = this->multi_int_grp__date_arg_funcs;
    MultiNumericalHashAggregate agg(test_def.groupby_cols,
                                    test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table, {0, 1, 2, 3});

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Generic_MultiIntGrp_DateArgFuncs) {
    auto test_def = this->multi_int_grp__date_arg_funcs;
    GenericHashAggregate agg(test_def.groupby_cols,
                             test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table, {0, 1, 2, 3});

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, BooleanGrp_DateArgFuncs) {
    auto test_def = this->boolean_grp__date_arg_funcs;
    GenericHashAggregate agg(test_def.groupby_cols,
                             test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table, {1});

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Single_NegInt64Grp_TimestampArgFuncs) {
    auto test_def = this->neg_int64_grp__timestamp_arg_funcs;
    SingleNumericalHashAggregate agg(test_def.groupby_cols,
                                     test_def.agg_cols, test_def.agg_funcs);

    auto sorted_table = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_table);
}

TEST_F(HashAggTestFixture, Multi_NegInt64Grp_TimestampArgFuncs) {
    auto test_def = this->neg_int64_grp__timestamp_arg_funcs;
    MultiNumericalHashAggregate agg(test_def.groupby_cols,
                                    test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, Generic_NegInt64Grp_TimestampArgFuncs) {
    auto test_def = this->neg_int64_grp__timestamp_arg_funcs;
    GenericHashAggregate agg(test_def.groupby_cols,
                             test_def.agg_cols, test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, NoGrp_AggFuncs) {
    auto test_def = this->no_grp__agg_funcs;
    OneGroupAggregate agg(test_def.agg_funcs);

    auto sorted_batch = aggregate_and_sort(agg, test_table);

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *sorted_batch);
}

TEST_F(HashAggTestFixture, EmptyTable_AggFuncs) {
    auto test_def = this->empty_table__agg_funcs;
    OneGroupAggregate agg(test_def.agg_funcs);

    agg.Next(empty_test_batch);
    std::shared_ptr<arrow::RecordBatch> res_batch = agg.Result();

    ASSERT_BATCHES_EQUAL(*test_def.result_batch, *res_batch);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}