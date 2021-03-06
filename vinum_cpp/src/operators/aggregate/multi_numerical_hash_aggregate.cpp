#include "multi_numerical_hash_aggregate.h"


namespace vinum::operators::aggregate {


void MultiNumericalHashAggregate::SetBatchArrays(const std::shared_ptr<arrow::RecordBatch>& batch) {
    BaseAggregate::SetBatchArrays(batch);

    for (size_t col_idx = 0, size = this->groupby_col_indices.size(); col_idx < size; col_idx++) {
        auto col = batch->column(this->groupby_col_indices[col_idx]);
        this->iters[col_idx]->SetArray(col);
    }
}


std::vector<std::shared_ptr<void>> &
MultiNumericalHashAggregate::GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                                              const int& row_idx,
                                              bool& is_new_entry) {
    static const auto num_groupby_cols = this->groupby_col_indices.size();

    std::vector<IntKeyValue> key;
    key.reserve(this->iters.size());
    for (const auto& iter : this->iters) {
        // Need to get the null bit before call to Next(), otherwise next elem's null value will be used.
        bool is_null = iter->IsNull();
        key.emplace_back(IntKeyValue{iter->NextAsUInt64(), is_null});
    }

    const auto &entry_pair = this->groups.find(key);

    if (entry_pair == this->groups.end()) {
        is_new_entry = true;

        auto entry = std::make_unique<std::vector<std::shared_ptr<void>>>();
        entry->reserve(num_groupby_cols);
        return *(this->groups[key] = std::move(entry));
    } else {
        is_new_entry = false;
        return *entry_pair->second;
    }
}

void MultiNumericalHashAggregate::SummarizeGroups() {
    const auto num_groups = this->groups.size();
    for (const auto &agg_func : this->agg_funcs) {
        agg_func->Reserve(num_groups);
    }

    for (const auto &group : this->groups) {
        auto &entry = *group.second;
        for (int agg_idx = 0, size = this->agg_funcs.size(); agg_idx < size; agg_idx++) {
            const auto &agg_func = this->agg_funcs[agg_idx];
            agg_func->Summarize(entry[agg_idx]);
        }
    }
}

void MultiNumericalHashAggregate::EnsureInitAggFuncs(const shared_ptr<arrow::Schema>& table_schema) {
    BaseAggregate::EnsureInitAggFuncs(table_schema);

    if (!this->iters.empty()) {
        return;
    }

    for (int col_idx : this->groupby_col_indices) {
        this->iters.push_back(
                std::move(common::array_iter_factory(table_schema->field(col_idx)->type()->id()))
        );
    }
}


}  // namespace vinum::operators::aggregate
