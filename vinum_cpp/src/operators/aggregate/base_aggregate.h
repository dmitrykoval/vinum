#pragma once

#include "agg_funcs.h"

#include <arrow/api.h>

#include <iostream>
#include <vector>
#include <unordered_set>
#include <unordered_map>

namespace vinum::operators::aggregate {


class BaseAggregate {
public:
    BaseAggregate(const std::vector<std::string>& groupby_cols,
                  const std::vector<std::string>& agg_cols,
                  const std::vector<AggFuncDef>& agg_funcs);

    virtual ~BaseAggregate();

    virtual void Next(const std::shared_ptr<arrow::RecordBatch>& batch);

    std::shared_ptr<arrow::RecordBatch> Result();

protected:
    const std::vector<AggFuncDef> input_agg_specs;
    const std::vector<std::string> groupby_col_names; // Names of groupby columns
    const std::vector<std::string> agg_col_names;   // Names of aggregate columns (subset of groupby columns)

    std::vector<int> groupby_col_indices; // Indices of groupby columns
    std::vector<int> agg_col_indices;   // Indices of aggregate columns (subset of groupby columns)
    std::vector<AggFuncDef> agg_func_specs; // Vector of aggregate function specs

    std::vector<std::shared_ptr<AbstractAggFunc>> agg_funcs;

    virtual void SetBatchArrays(const std::shared_ptr<arrow::RecordBatch>& batch);
    virtual void EnsureInitAggFuncs(const std::shared_ptr<arrow::Schema>& schema);

private:

    virtual std::vector<std::shared_ptr<void>>& GetOrCreateEntry(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                 const int& row_idx,
                                                                 bool& is_new_entry) = 0;
    virtual void SummarizeGroups() = 0;
};

void lookup_col_indices(const std::vector<std::string>& col_names,
                        std::vector<int>& col_indices,
                        const std::shared_ptr<arrow::Schema>& table_schema);


}  // namespace vinum::operators::aggregate
