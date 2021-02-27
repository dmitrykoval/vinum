#include "single_numerical_hash_aggregate.h"


namespace vinum::operators::aggregate {


void SingleNumericalHashAggregate::SetBatchArrays(const std::shared_ptr<arrow::RecordBatch>& batch) {
    BaseAggregate::SetBatchArrays(batch);

    auto col = batch->column(this->groupby_col_indices[0]);
    this->iter->SetArray(col);
}


std::vector<std::shared_ptr<void>> &
SingleNumericalHashAggregate::GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                                               const int& row_idx,
                                               bool& is_new_entry) {
    static const auto num_groupby_cols = this->groupby_col_indices.size();
    bool is_null = this->iter->IsNull();
    auto key = this->iter->NextAsUInt64();

    // Check NULL group first
    if (is_null) {
        is_new_entry = false;
        if (this->null_group == nullptr) {
            this->null_group = std::move(std::make_unique<std::vector<std::shared_ptr<void>>>());
            this->null_group->reserve(num_groupby_cols);
            is_new_entry = true;
        }
        return *this->null_group;
    }

    const auto& entry_pair = this->groups.find(key);

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

void SingleNumericalHashAggregate::SummarizeGroups() {
    const auto num_groups = this->groups.size() + ((this->null_group != nullptr) ? 1 : 0);
    for (const auto &agg_func : this->agg_funcs) {
        agg_func->Reserve(num_groups);
    }

    for (const auto &group : this->groups) {
        SummarizeAggFunc(*group.second);
    }

    if (this->null_group != nullptr) {
        SummarizeAggFunc(*this->null_group);
    }
}

void SingleNumericalHashAggregate::SummarizeAggFunc(const std::vector<std::shared_ptr<void>>& entry) {
    for (int agg_idx = 0, size = this->agg_funcs.size(); agg_idx < size; agg_idx++) {
        const auto &agg_func = this->agg_funcs[agg_idx];
        agg_func->Summarize(entry[agg_idx]);
    }
}

void SingleNumericalHashAggregate::EnsureInitAggFuncs(const shared_ptr<arrow::Schema>& table_schema) {
    BaseAggregate::EnsureInitAggFuncs(table_schema);

    if (this->iter == nullptr) {
        this->iter = std::move(common::array_iter_factory(table_schema->field(groupby_col_indices[0])->type()->id()));
    }
}


}  // namespace vinum::operators::aggregate
