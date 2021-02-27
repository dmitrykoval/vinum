#pragma once

#include "base_aggregate.h"
#include "common/array_iterators.h"

#include "common/robin_hood.h"


namespace vinum::operators::aggregate {

class SingleNumericalHashAggregate : public BaseAggregate {
public:

    using BaseAggregate::BaseAggregate;


protected:
    typedef uint64_t KEY_TYPE;

    void SetBatchArrays(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    void EnsureInitAggFuncs(const shared_ptr<arrow::Schema>& table_schema) override;


private:
    robin_hood::unordered_map <
            KEY_TYPE,
            std::unique_ptr<std::vector<std::shared_ptr<void>>> > groups;

    std::unique_ptr<std::vector<std::shared_ptr<void>>> null_group = nullptr;
    std::unique_ptr<common::ArrayIter> iter = nullptr;


    std::vector <std::shared_ptr<void>> &GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                          const int& row_idx,
                                                          bool& is_new_entry) override;
    void SummarizeGroups() override;

private:
    inline void SummarizeAggFunc(const std::vector<std::shared_ptr<void>>& entry);

};

}  // namespace vinum::operators::aggregate
