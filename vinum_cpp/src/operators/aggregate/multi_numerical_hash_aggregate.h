#pragma once

#include "base_aggregate.h"
#include "common/array_iterators.h"

#include "common/robin_hood.h"


namespace vinum::operators::aggregate {

struct IntKeyValue {
    uint64_t value;
    bool is_null;

    bool operator==(const IntKeyValue& second) const {
        return is_null == second.is_null && (is_null || value == second.value);
    }
};

class IntVectorHasher {
public:
    std::size_t operator()(const std::vector<IntKeyValue> &vec) const {
        std::size_t seed = vec.size();
        for (const auto& val : vec) {
            std::size_t hash_val;
            if (!val.is_null) {
                hash_val = int_hash(val.value);
            } else {
                hash_val = 0;
            }
            seed ^= hash_val + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }

private:
    std::hash<uint64_t> int_hash;
};


class MultiNumericalHashAggregate : public BaseAggregate {
public:

    using BaseAggregate::BaseAggregate;

protected:
    typedef std::vector<IntKeyValue> KEY_TYPE;

    void SetBatchArrays(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    void EnsureInitAggFuncs(const shared_ptr<arrow::Schema>& table_schema) override;


private:
    robin_hood::unordered_map <
            KEY_TYPE,
            std::unique_ptr<std::vector<std::shared_ptr<void>>>,
            IntVectorHasher > groups;

    std::vector<std::unique_ptr<common::ArrayIter>> iters;

    std::vector <std::shared_ptr<void>> &GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                          const int& row_idx,
                                                          bool& is_new_entry) override;
    void SummarizeGroups() override;

};

}  // namespace vinum::operators::aggregate
