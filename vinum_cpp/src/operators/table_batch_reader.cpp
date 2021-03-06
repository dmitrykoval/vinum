#include "table_batch_reader.h"

namespace vinum::operators {

TableBatchReader::TableBatchReader(const std::shared_ptr<arrow::Table>& in_table) : table(in_table) {
    reader = std::make_unique<arrow::TableBatchReader>(*this->table);
}

std::shared_ptr<arrow::RecordBatch> TableBatchReader::Next() {
    auto res = this->reader->Next();
    return res.ValueOrDie();
}

void TableBatchReader::SetBatchSize(int64_t batch_size) {
    this->reader->set_chunksize(batch_size);
}

}  // namespace vinum::operators
