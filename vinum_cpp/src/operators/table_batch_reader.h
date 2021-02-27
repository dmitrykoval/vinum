#pragma once

#include <arrow/api.h>

#include <memory>

namespace vinum::operators {

class TableBatchReader {
public:
    explicit TableBatchReader(const std::shared_ptr<arrow::Table>& table);

    std::shared_ptr<arrow::RecordBatch> Next();

    void SetBatchSize(int64_t batch_size);

private:
    std::shared_ptr<arrow::Table> table;
    std::unique_ptr<arrow::TableBatchReader> reader;

};

}  // namespace vinum::operators
