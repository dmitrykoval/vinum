#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <memory>

namespace vinum::operators::sort {

enum class SortOrder {
    ASC, DESC
};

class Sort {
public:
    explicit Sort(const std::vector<std::string>& sort_cols,
                  const std::vector<SortOrder>& sort_order);

    void Next(const std::shared_ptr<arrow::RecordBatch>& batch);

    std::shared_ptr<arrow::RecordBatch> Sorted();


private:
    std::vector<std::string> sort_cols;
    std::vector<SortOrder> sort_order;

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};


}  // namespace vinum::operators::sort
