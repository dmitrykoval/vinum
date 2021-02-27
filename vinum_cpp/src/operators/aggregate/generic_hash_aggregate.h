#pragma once

#include "base_aggregate.h"

#include "common/robin_hood.h"

namespace vinum::operators::aggregate {

class ScalarsVectorHasher {
public:
    std::size_t operator()(const std::vector<std::shared_ptr<arrow::Scalar>> &vec) const {
        std::size_t seed = vec.size();
        for (const std::shared_ptr<arrow::Scalar> &scalar : vec) {
            size_t hash_val;
            if (scalar->is_valid) {
                hash_val = scalar_hash(*scalar);
            } else {
                hash_val = 0;
            }
            seed ^= hash_val + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }

private:
    arrow::Scalar::Hash scalar_hash;
};

class ScalarsVectorEqualsFn {
public:
    bool operator()(const std::vector<std::shared_ptr<arrow::Scalar>> &v_one,
                    const std::vector<std::shared_ptr<arrow::Scalar>> &v_two) const {
        auto iter_one = v_one.begin();
        auto end_one = v_one.end();
        auto iter_two = v_two.begin();
        for (; iter_one != end_one; ++iter_one, ++iter_two) {
            if (!(*iter_one)->Equals(*iter_two)) {
                return false;
            }
        }
        return true;
    }
};


class GenericHashAggregate : public BaseAggregate {
public:
    using BaseAggregate::BaseAggregate;

protected:
    typedef std::vector<std::shared_ptr<arrow::Scalar>> KEY_TYPE;


private:
    robin_hood::unordered_map<
            KEY_TYPE,
            std::unique_ptr<std::vector<std::shared_ptr<void>>>,
            ScalarsVectorHasher,
            ScalarsVectorEqualsFn> groups;

    std::vector<std::shared_ptr<void>> &GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                         const int& row_idx,
                                                         bool& is_new_entry) override;

    void SummarizeGroups() override;

};


}  // namespace vinum::operators::aggregate
