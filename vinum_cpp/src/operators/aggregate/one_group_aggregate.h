# pragma once

#include "base_aggregate.h"
#include "common/array_iterators.h"


namespace vinum::operators::aggregate {

class OneGroupAggregate : public BaseAggregate {
public:

    explicit OneGroupAggregate(const std::vector<AggFuncDef>& agg_funcs);

    void Next(const std::shared_ptr<arrow::RecordBatch>& batch) override;

protected:
    std::unique_ptr<std::vector<std::shared_ptr<void>>> group =
            std::make_unique<std::vector<std::shared_ptr<void>>>();

    void SummarizeGroups() override;

private:
    vector<std::shared_ptr<void>> &
    GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                     const int& row_idx,
                     bool& is_new_entry) override;
};

}  // namespace vinum::operators::aggregate
