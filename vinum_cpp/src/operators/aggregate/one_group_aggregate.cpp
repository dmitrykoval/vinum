#include "one_group_aggregate.h"

namespace vinum::operators::aggregate {

OneGroupAggregate::OneGroupAggregate(const std::vector<AggFuncDef>& agg_funcs)
        : BaseAggregate({}, {}, agg_funcs) {}


void OneGroupAggregate::Next(const std::shared_ptr<arrow::RecordBatch>& batch) {
    this->EnsureInitAggFuncs(batch->schema());
    this->SetBatchArrays(batch);

    if (this->group->empty()) {
        for (const auto &agg_func : this->agg_funcs) {
            this->group->push_back(
                agg_func->InitBatch()
            );
        }
    }

    for (auto agg_idx = this->agg_col_indices.size(), size = this->agg_funcs.size();
         agg_idx < size; agg_idx++) {
        const auto &agg_func = this->agg_funcs[agg_idx];
        agg_func->UpdateBatch((*this->group)[agg_idx]);
    }
}

void OneGroupAggregate::SummarizeGroups() {
    for (const auto &agg_func : this->agg_funcs) {
        agg_func->Reserve(1);
    }

    for (int agg_idx = 0, size = this->agg_funcs.size(); agg_idx < size; agg_idx++) {
        const auto &agg_func = this->agg_funcs[agg_idx];
        agg_func->Summarize((*this->group)[agg_idx]);
    }
}

vector<std::shared_ptr<void>> &
OneGroupAggregate::GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                                    const int& row_idx,
                                    bool& is_new_entry) {
    throw std::runtime_error("GetOrCreateEntry() is not implemented for OneGroupAggregate.");
}

}  // namespace vinum::operators::aggregate
