#include "base_aggregate.h"
#include "agg_func_factory.h"
#include "agg_funcs.h"

#include <arrow/api.h>

#include <unordered_map>
#include <utility>
#include <vector>


namespace vinum::operators::aggregate {

BaseAggregate::BaseAggregate(const std::vector<std::string>& groupby_cols,
                             const std::vector<std::string>& agg_cols,
                             const std::vector<AggFuncDef>& agg_funcs)
                 : groupby_col_names(groupby_cols),
                   agg_col_names(agg_cols),
                   input_agg_specs(agg_funcs) {}

BaseAggregate::~BaseAggregate() = default;

void BaseAggregate::Next(const std::shared_ptr<arrow::RecordBatch>& batch) {
    this->EnsureInitAggFuncs(batch->schema());
    this->SetBatchArrays(batch);

    auto num_rows = batch->column(0)->length();
    for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
        bool is_new_entry;
        auto& entry = this->GetOrCreateEntry(batch, row_idx, is_new_entry);
        if (is_new_entry) {
            for (const auto &agg_func : this->agg_funcs) {
                entry.push_back(
                    agg_func->Init(row_idx)
                );
            }
        } else {
            for (auto agg_idx = this->agg_col_indices.size(), size = this->agg_funcs.size();
                        agg_idx < size; agg_idx++) {
                const auto &agg_func = this->agg_funcs[agg_idx];
                agg_func->Update(entry[agg_idx]);
            }
        }
    }
}

std::shared_ptr<arrow::RecordBatch> BaseAggregate::Result() {
    this->SummarizeGroups();

    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    std::vector<std::shared_ptr<arrow::Array>> table_cols;
    for (int agg_idx = 0, size = this->agg_funcs.size(); agg_idx < size; agg_idx++) {
        const auto &agg_func = this->agg_funcs[agg_idx];
        const auto &ag_func_spec = this->agg_func_specs[agg_idx];

        schema_vector.push_back(arrow::field(ag_func_spec.out_col_name, agg_func->DataType()));

        auto array = agg_func->Result();
        table_cols.push_back(std::move(array));
    }

    int num_rows = 0;
    if (!table_cols.empty() && table_cols[0]->length()) {
        num_rows = table_cols[0]->length();
    }
    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    return std::shared_ptr<arrow::RecordBatch>(arrow::RecordBatch::Make(schema, num_rows, table_cols));
}

void BaseAggregate::SetBatchArrays(const std::shared_ptr<arrow::RecordBatch> &batch) {
    for (size_t agg_idx = 0, size = this->agg_func_specs.size(); agg_idx < size; agg_idx++) {
        const AggFuncDef &func_def = this->agg_func_specs[agg_idx];
        const auto &agg_func = this->agg_funcs[agg_idx];

        auto type_id =
                !func_def.column_name.empty()
                ? batch->schema()->GetFieldByName(func_def.column_name)->type()->id()
                : arrow::Type::NA;
        auto array_iter = common::array_iter_factory(type_id);

        std::shared_ptr<arrow::Array> array =
                !func_def.column_name.empty()
                ? batch->GetColumnByName(func_def.column_name)
                : batch->column(0);

        array_iter->SetArray(array);
        agg_func->SetArrayIter(std::move(array_iter));
    }
}

void BaseAggregate::EnsureInitAggFuncs(const std::shared_ptr<arrow::Schema>& schema) {
    if (!this->agg_funcs.empty()) {
        return;
    }

    lookup_col_indices(this->groupby_col_names, this->groupby_col_indices, schema);
    lookup_col_indices(this->agg_col_names, this->agg_col_indices, schema);

    std::vector<AggFuncDef> all_func_specs;
    for (const auto& col_name : this->agg_col_names) {
        AggFuncDef func {
                AggFuncType::GROUP_BUILDER,
                col_name,
                col_name
        };
        all_func_specs.push_back(func);
        const auto func_inst = agg_func_factory(func, schema);
        this->agg_funcs.push_back(func_inst);
    }

    for (const AggFuncDef &func : this->input_agg_specs) {
        const auto func_inst = agg_func_factory(func, schema);
        this->agg_funcs.push_back(func_inst);
    }
    all_func_specs.insert(all_func_specs.end(),
                          this->input_agg_specs.begin(),
                          this->input_agg_specs.end());
    this->agg_func_specs = all_func_specs;
}

void lookup_col_indices(const std::vector<std::string>& col_names,
                        std::vector<int>& col_indices,
                        const std::shared_ptr<arrow::Schema>& table_schema) {
    for (const auto& col_name : col_names) {
        auto col_idx = table_schema->GetFieldIndex(col_name);
        if (col_idx == -1) {
            throw std::runtime_error("Column not found: " + col_name);
        }
        col_indices.push_back(col_idx);
    }
}


}  // namespace vinum::operators::aggregate
