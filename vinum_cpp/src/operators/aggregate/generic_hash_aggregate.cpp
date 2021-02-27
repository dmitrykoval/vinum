#include "generic_hash_aggregate.h"


namespace vinum::operators::aggregate {

std::vector<std::shared_ptr<void>> &
GenericHashAggregate::GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                                       const int &row_idx,
                                       bool &is_new_entry) {
    static const auto num_groupby_cols = this->groupby_col_indices.size();

    auto key = std::vector<std::shared_ptr<arrow::Scalar>>();
    key.reserve(num_groupby_cols);

    for (int col_idx : this->groupby_col_indices) {
        arrow::Result<std::shared_ptr<arrow::Scalar>> scalar_res =
                batch->column(col_idx)->GetScalar(row_idx);
        if (!scalar_res.ok()) {
            throw std::runtime_error("Failed to execute array::GetScalar().");
        }
        key.emplace_back(scalar_res.ValueOrDie());
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

void GenericHashAggregate::SummarizeGroups() {
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


}  // namespace vinum::operators::aggregate
