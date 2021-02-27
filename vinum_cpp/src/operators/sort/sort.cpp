#include "sort.h"

#include <iostream>

namespace vinum::operators::sort {

Sort::Sort(const std::vector<std::string>& cols,
           const std::vector<SortOrder>& order) :
        sort_cols(cols), sort_order(order) {}

void Sort::Next(const std::shared_ptr<arrow::RecordBatch>& batch) {
    this->batches.push_back(batch);
}

std::shared_ptr<arrow::RecordBatch> Sort::Sorted() {
    auto create_tbl_res = arrow::Table::FromRecordBatches(this->batches);
    if (!create_tbl_res.ok()) {
        throw std::runtime_error("Failed to create table from record batches." + create_tbl_res.status().ToString());
    }
    auto table = create_tbl_res.ValueOrDie();

    std::vector<arrow::compute::SortKey> sort_keys;
    for (size_t i = 0, len = this->sort_cols.size(); i < len; i++) {
        const auto& col_name = this->sort_cols[i];
        const auto& order = this->sort_order[i] == SortOrder::ASC
                ? arrow::compute::SortOrder::Ascending
                : arrow::compute::SortOrder::Descending;
        sort_keys.emplace_back(col_name, order);
    }

    auto table_datum = arrow::Datum{*table};
    arrow::compute::SortOptions sort_options(sort_keys);
    auto sort_result = arrow::compute::SortIndices(table_datum, sort_options);
    if (!sort_result.ok()) {
        throw std::runtime_error("Failed to sort table.");
    }
    auto sorted_indices = sort_result.ValueOrDie();

    arrow::Datum sorted_datum;
    auto take_res = arrow::compute::Take(table_datum, arrow::Datum(sorted_indices));
    if (!take_res.ok()) {
        throw std::runtime_error("Failed to take table in sorted order.");
    }
    auto tbl = take_res.ValueOrDie().table();

    // There should only be one chunk, just an extra check.
    assert(tbl->column(0)->num_chunks() <= 1);
    auto comb_res = tbl->CombineChunks();
    if (!comb_res.ok()) {
        throw std::runtime_error("Failed to combine table's chunks.");
    }
    auto sorted_table = comb_res.ValueOrDie();

    auto reader = arrow::TableBatchReader(*sorted_table);
    // Let's ensure that the entire table is converted into a single RecordBatch
    reader.set_chunksize(sorted_table->num_rows() + 1);
    auto batch_res = reader.Next();
    if (!comb_res.ok()) {
        throw std::runtime_error("Failed to convert table to a record batch.");
    }

    return batch_res.ValueOrDie();
}


}  // namespace vinum::operators::sort
