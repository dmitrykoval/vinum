#pragma once

#include "common/util.h"
#include "common/array_iterators.h"

#include <arrow/api.h>

#include <iostream>
#include <limits>
#include <common/huge_int.h>
#include <unordered_set>


namespace vinum::operators::aggregate {

enum class AggFuncType {
    COUNT, COUNT_STAR, MIN, MAX, SUM, AVG, GROUP_BUILDER
};

struct AggFuncDef {
    AggFuncType func;
    std::string column_name;
    std::string out_col_name;
};


template <typename BUILDER, typename T>
inline void AppendToBuilder(std::unique_ptr<BUILDER>& builder, const std::shared_ptr<T>& val);

template <typename BUILDER, typename T = std::string>
inline void AppendToBuilder(std::unique_ptr<BUILDER>& builder, const std::shared_ptr<std::string>& val) {
    RAISE_ON_ARROW_FAILURE(builder->Append(arrow::util::string_view(val->data(), val->length())));
}

template <typename BUILDER, typename T>
inline void AppendToBuilder(std::unique_ptr<BUILDER>& builder, const std::shared_ptr<T>& val) {
    builder->UnsafeAppend(*val);
}


class AbstractAggFunc {
public:
    virtual void SetArrayIter(std::unique_ptr<common::ArrayIter> iter) = 0;

    virtual std::shared_ptr<void> Init(int row_idx) = 0;

    virtual void Update(std::shared_ptr<void> &current_val) = 0;

    virtual std::shared_ptr<void> InitBatch() = 0;

    virtual void UpdateBatch(std::shared_ptr<void> &group) = 0;

    virtual void Reserve(int64_t capacity) = 0;

    virtual void Summarize(const std::shared_ptr<void> &current_val) = 0;

    virtual std::shared_ptr<arrow::Array> Result() = 0;

    virtual std::shared_ptr<arrow::DataType> DataType() = 0;
};

template<typename T_OUT, typename BUILDER>
class AggFuncTemplate : public AbstractAggFunc {

public:
    explicit AggFuncTemplate(const std::shared_ptr<arrow::DataType>& builder_type) {
        this->builder = std::make_unique<BUILDER>(builder_type, arrow::default_memory_pool());
    }

    void Reserve(int64_t capacity) override {
        RAISE_ON_ARROW_FAILURE(this->builder->Resize(capacity));
    }

    void Summarize(const std::shared_ptr<void> &current_val) override {
        if (current_val != nullptr) {
            auto val = std::static_pointer_cast<T_OUT>(current_val);
            ::vinum::operators::aggregate::AppendToBuilder<BUILDER, T_OUT>(this->builder, val);
        } else {
            this->builder->UnsafeAppendNull();
        }
    }

    std::shared_ptr<arrow::Array> Result() override {
        std::shared_ptr<arrow::Array> array;
        RAISE_ON_ARROW_FAILURE(this->builder->Finish(&array));
        return array;
    }

    std::shared_ptr<arrow::DataType> DataType() override {
        return builder->type();
    }

protected:
    std::unique_ptr<BUILDER> builder;
};

class CountStarFunc : public AggFuncTemplate<uint64_t, arrow::UInt64Builder> {

public:
    CountStarFunc() : AggFuncTemplate<uint64_t, arrow::UInt64Builder>::AggFuncTemplate(arrow::uint64()) {}

    void SetArrayIter(std::unique_ptr<common::ArrayIter> iter) override {
        this->array_iter = std::move(iter);
    }

    std::shared_ptr<void> Init(int row_idx) override {
        return std::make_shared<uint64_t>(1);
    }

    void Update(std::shared_ptr<void> &current_val) override {
        auto count = std::static_pointer_cast<uint64_t>(current_val);
        *count += 1;
    }

    shared_ptr<void> InitBatch() override {
        return std::make_shared<uint64_t>(0);
    }

    void UpdateBatch(shared_ptr<void> &group) override {
        auto count = std::static_pointer_cast<uint64_t>(group);
        *count += this->array_iter->Length();
    }

protected:
    std::unique_ptr<common::ArrayIter> array_iter = nullptr;

};

class CountFunc : public AggFuncTemplate<uint64_t, arrow::UInt64Builder> {

public:
    explicit CountFunc()
            : AggFuncTemplate<uint64_t, arrow::UInt64Builder>::AggFuncTemplate(arrow::uint64()) {}

    void SetArrayIter(std::unique_ptr<common::ArrayIter> iter) override {
        this->array_iter = std::move(iter);
    }

    std::shared_ptr<void> Init(int row_idx) override {
        auto initial = this->array_iter->NextNull() ? 0 : 1;
        return std::make_shared<uint64_t>(initial);
    }

    void Update(std::shared_ptr<void> &current_val) override {
        auto incr = this->array_iter->NextNull() ? 0 : 1;
        auto count = std::static_pointer_cast<uint64_t>(current_val);
        *count += incr;
    }

    shared_ptr<void> InitBatch() override {
        return std::make_shared<uint64_t>(0);
    }

    void UpdateBatch(shared_ptr<void> &group) override {
        auto count = std::static_pointer_cast<uint64_t>(group);
        *count += this->array_iter->NonNullCount();
    }

private:
    std::unique_ptr<common::ArrayIter> array_iter = nullptr;
};


template<typename T_IN, typename BUILDER>
class MinMaxFunc : public AggFuncTemplate<T_IN, BUILDER> {

public:
    using AggFuncTemplate<T_IN, BUILDER>::AggFuncTemplate;

    explicit MinMaxFunc(bool is_max,
                        const std::shared_ptr<arrow::DataType>& builder_type = nullptr
    ) : AggFuncTemplate<T_IN, BUILDER>(builder_type), is_max(is_max) {}

    void SetArrayIter(std::unique_ptr<common::ArrayIter> iter) override {
        this->array_iter = std::unique_ptr<common::TypedValueArrayIter<T_IN>>{
                static_cast<common::TypedValueArrayIter<T_IN>*>(iter.release())
        };
    }

    std::shared_ptr<void> Init(int row_idx) override {
        if (this->array_iter->NextIfNull()) {
            return std::shared_ptr<T_IN>(nullptr);
        } else {
            return std::make_shared<T_IN>(this->array_iter->Next());
        }
    }

    inline void Update(std::shared_ptr<void> &current_val) override {
        if (this->array_iter->NextIfNull()) {
            return;
        } else if (current_val == nullptr) {
            current_val = std::make_shared<T_IN>(this->array_iter->Next());
            return;
        }

        auto row_val = this->array_iter->Next();
        auto last = std::static_pointer_cast<T_IN>(current_val);
        if ((row_val < *last) ^ this->is_max) {
            *last = row_val;
        }
    }

    shared_ptr<void> InitBatch() override {
        return this->Init(0);
    }

    void UpdateBatch(shared_ptr<void> &group) override {
        while (this->array_iter->HasMore()) {
            this->Update(group);
        }
    }

protected:
    std::unique_ptr<common::TypedValueArrayIter<T_IN>> array_iter = nullptr;
    bool is_max;
};


template<typename BUILDER>
class StringMinMaxFunc : public MinMaxFunc<std::string, BUILDER> {

public:
    using MinMaxFunc<std::string, BUILDER>::MinMaxFunc;

    void SetArrayIter(std::unique_ptr<common::ArrayIter> iter) override {
        this->str_iter = std::unique_ptr<common::TypedValueArrayIter<arrow::util::string_view>>{
                dynamic_cast<common::TypedValueArrayIter<arrow::util::string_view>*>(iter.release())
        };
    }

    std::shared_ptr<void> Init(int row_idx) override {
        if (this->str_iter->NextIfNull()) {
            return std::shared_ptr<std::string>(nullptr);
        } else {
            return this->create_shared_entry(this->str_iter->Next());
        }
    }

    inline void Update(std::shared_ptr<void>& current_val) override {
        if (this->str_iter->NextIfNull()) {
            return;
        } else if (current_val == nullptr) {
            current_val = this->create_shared_entry(this->str_iter->Next());
            return;
        }

        auto row_val = this->str_iter->Next();
        auto last = std::static_pointer_cast<std::string>(current_val);
        auto last_view = arrow::util::string_view(last->data(), last->length());
        if ((row_val < last_view) ^ this->is_max) {
            *last = std::string(row_val.data(), row_val.length());
        }
    }

protected:
    std::unique_ptr<common::TypedValueArrayIter<arrow::util::string_view>> str_iter = nullptr;

    inline std::shared_ptr<std::string> create_shared_entry(const arrow::util::string_view& view) {
        return std::make_shared<std::string>(view.data(), view.length());
    }
};


template<typename T_IN, typename T_OUT, typename BUILDER>
class NumericAggFunc : public AggFuncTemplate<T_OUT, BUILDER> {
public:
    using AggFuncTemplate<T_OUT, BUILDER>::AggFuncTemplate;

    void SetArrayIter(std::unique_ptr<common::ArrayIter> iter) override {
        this->array_iter = std::unique_ptr<common::NumericArrayIter<T_IN>>{
                static_cast<common::NumericArrayIter<T_IN>*>(iter.release())
        };
    }

protected:
    std::unique_ptr<common::NumericArrayIter<T_IN>> array_iter = nullptr;
};


template<typename T_IN, typename T_OUT, typename BUILDER>
class SumFunc : public NumericAggFunc<T_IN, T_OUT, BUILDER> {

public:
    using NumericAggFunc<T_IN, T_OUT, BUILDER>::NumericAggFunc;

    std::shared_ptr<void> Init(int row_idx) override {
        if (this->array_iter->NextIfNull()) {
            return std::shared_ptr<T_OUT>(nullptr);
        } else {
            return std::make_shared<T_OUT>(this->array_iter->Next());
        }
    }

    inline void Update(std::shared_ptr<void> &current_val) override {
        if (this->array_iter->NextIfNull()) {
            return;
        } else if (current_val == nullptr) {
            current_val = std::make_shared<T_OUT>(this->array_iter->Next());
            return;
        }

        auto row_val = this->array_iter->Next();
        auto last = std::static_pointer_cast<T_OUT>(current_val);
        *last += row_val;
    }

    shared_ptr<void> InitBatch() override {
        return this->Init(0);
    }

    void UpdateBatch(shared_ptr<void> &group) override {
        while (this->array_iter->HasMore()) {
            this->Update(group);
        }
    }

};

template<typename T_IN, typename T_OUT, typename BUILDER>
class SumOverflowFunc : public NumericAggFunc<T_IN, T_OUT, BUILDER> {

public:
    using DType = typename T_IN::c_type;

    using NumericAggFunc<T_IN, T_OUT, BUILDER>::NumericAggFunc;

    std::shared_ptr<void> Init(int row_idx) override {
        if (this->array_iter->NextIfNull()) {
            return std::shared_ptr<common::hugeint_t>(nullptr);
        } else {
            return std::make_shared<common::hugeint_t>(common::Hugeint::Convert<DType>(this->array_iter->Next()));
        }
    }

    inline void Update(std::shared_ptr<void> &current_val) override {
        if (this->array_iter->NextIfNull()) {
            return;
        } else if (current_val == nullptr) {
            current_val = std::make_shared<common::hugeint_t>(common::Hugeint::Convert<DType>(this->array_iter->Next()));
            return;
        }

        auto row_val = common::Hugeint::Convert<DType>(this->array_iter->Next());
        auto last = std::static_pointer_cast<common::hugeint_t>(current_val);
        *last += row_val;
    }

    shared_ptr<void> InitBatch() override {
        return this->Init(0);
    }

    void UpdateBatch(shared_ptr<void> &group) override {
        while (this->array_iter->HasMore()) {
            this->Update(group);
        }
    }

    void Summarize(const shared_ptr<void> &current_val) override {
        if (current_val != nullptr) {
            auto hugeint_val = std::static_pointer_cast<common::hugeint_t>(current_val);

            if (this->is_overflow_mode) {
                this->overflow_builder_->UnsafeAppend(
                        this->HugeintToDecimal(*hugeint_val));
            } else {
                T_OUT int_res;
                bool cast_ok = common::Hugeint::TryCast<T_OUT>(*hugeint_val, int_res);

                if (cast_ok) {
                    // Value can be cast to the output type, appending casted value.
                    this->builder->UnsafeAppend(int_res);
                } else {
                    // Sum would overflow on 64 bit types, we need to resort Decimal128.
                    this->is_overflow_mode = true;

                    // Instantiate new Decimal128Builder
                    this->overflow_builder_ = std::make_unique<arrow::Decimal128Builder>(
                            arrow::decimal128(arrow::Decimal128Type::kMaxPrecision, 0)
                            );
                    RAISE_ON_ARROW_FAILURE(this->overflow_builder_->Resize(this->builder->capacity()));

                    // Copy values from the current builder
                    this->CopyBuilder();

                    // Append Decimal Value
                    this->overflow_builder_->UnsafeAppend(
                            this->HugeintToDecimal(*hugeint_val));
                }
            }
        } else {
            if (this->is_overflow_mode) {
                this->overflow_builder_->UnsafeAppendNull();
            } else {
                this->builder->UnsafeAppendNull();
            }
        }
    }

    shared_ptr<arrow::DataType> DataType() override {
        if (this->is_overflow_mode) {
            return this->overflow_builder_->type();
        } else {
            return this->builder->type();
        }
    }

    shared_ptr<arrow::Array> Result() override {
        std::shared_ptr<arrow::Array> array;
        if (this->is_overflow_mode) {
            RAISE_ON_ARROW_FAILURE(this->overflow_builder_->Finish(&array));
        } else {
            RAISE_ON_ARROW_FAILURE(this->builder->Finish(&array));
        }
        return array;
    }

private:
    bool is_overflow_mode = false;
    std::unique_ptr<arrow::Decimal128Builder> overflow_builder_ = nullptr;

    inline arrow::Decimal128 HugeintToDecimal(common::hugeint_t& hugeint) const {
        return arrow::Decimal128{hugeint.upper, hugeint.lower};
    }

    void CopyBuilder() {
        for (int64_t i = 0, sz = this->builder->length(); i < sz; i++) {
            if (!this->array_iter->IsNull(i)) {
                this->overflow_builder_->UnsafeAppend(
                        arrow::Decimal128{(*this->builder)[i]});
            } else {
                this->overflow_builder_->UnsafeAppendNull();
            }
        }
    }
};



template<typename T_IN, typename T_SUM, typename T_OUT, typename BUILDER>
class AvgFunc : public NumericAggFunc<T_IN, T_OUT, BUILDER> {

public:
    using DType = typename T_IN::c_type;

    using NumericAggFunc<T_IN, T_OUT, BUILDER>::NumericAggFunc;

    std::shared_ptr<void> Init(int row_idx) override {
        if (this->array_iter->NextIfNull()) {
            return std::shared_ptr<std::shared_ptr<std::pair<T_SUM, uint64_t>>>(nullptr);
        } else {
            return this->InitPair<DType>(this->array_iter->Next());
        }
    }

    void Update(std::shared_ptr<void> &current_val) override {
        if (this->array_iter->NextIfNull()) {
            return;
        } else if (current_val == nullptr) {
            current_val = this->InitPair<DType>(this->array_iter->Next());
            return;
        }

        auto row_val = this->array_iter->Next();
        auto pair = std::static_pointer_cast<std::pair<T_SUM, uint64_t>>(current_val);
        this->Add<T_SUM>(pair->first, row_val);
        pair->second += 1;
    }

    shared_ptr<void> InitBatch() override {
        return this->Init(0);
    }

    void UpdateBatch(shared_ptr<void> &group) override {
        while (this->array_iter->HasMore()) {
            this->Update(group);
        }
    }


    void Summarize(const std::shared_ptr<void> &current_val) override {
        if (current_val != nullptr) {
            auto pair = std::static_pointer_cast<std::pair<T_SUM, uint64_t>>(current_val);
            T_OUT val = this->ComputeAvg<T_SUM>(pair->first, pair->second);
            this->builder->UnsafeAppend(val);
        } else {
            this->builder->UnsafeAppendNull();
        }
    }

private:

    template<typename T>
    inline std::shared_ptr<std::pair<T_SUM, uint64_t>> InitPair(const T &val) {
        return std::make_shared<std::pair<T_SUM, uint64_t>>(
                std::make_pair(val, 1)
        );
    }

    template<typename T=common::hugeint_t>
    inline std::shared_ptr<std::pair<common::hugeint_t, uint64_t>> InitPair(const uint64_t &val) {
        auto sum = common::Hugeint::Convert(val);
        return std::make_shared<std::pair<common::hugeint_t, uint64_t>>(
                std::make_pair(sum, 1)
        );
    }

    template<typename T>
    inline void Add(T &add_to, DType val) const {
        add_to += val;
    }

    template<typename T=common::hugeint_t>
    inline void Add(common::hugeint_t &add_to, DType val) const {
        auto hugeint_val = common::Hugeint::Convert<DType>(val);
        add_to += hugeint_val;
    }


    template<typename T>
    inline T_OUT ComputeAvg(T sum, uint64_t count) {
        return sum / static_cast<double>(count);
    }

    template<typename T=common::hugeint_t>
    inline T_OUT ComputeAvg(common::hugeint_t sum, uint64_t count) {
        T_OUT avg = 0;
        // Whole part
        auto quotient = sum / count;
        auto cast_status = common::Hugeint::TryCast(quotient, avg);
        assert(cast_status);

        // Fractional part
        T_OUT rem_double;
        auto remainder = sum % count;
        cast_status = common::Hugeint::TryCast(remainder, rem_double);
        assert(cast_status);
        avg += rem_double / count;

        return avg;
    }

};

template<typename T_IN, typename BUILDER>
class GroupBuilder : public AggFuncTemplate<T_IN, BUILDER> {

public:
    using AggFuncTemplate<T_IN, BUILDER>::AggFuncTemplate;

    void SetArrayIter(std::unique_ptr<common::ArrayIter> iter) override {
        this->array_iter = std::unique_ptr<common::TypedValueArrayIter<T_IN>>{
                static_cast<common::TypedValueArrayIter<T_IN>*>(iter.release())
        };
    }

    std::shared_ptr<void> Init(int row_idx) override {
        if (this->array_iter->IsNull(row_idx)) {
            return std::shared_ptr<T_IN>(nullptr);
        } else {
            return std::make_shared<T_IN>(this->array_iter->GetValue(row_idx));
        }
    }

    void Update(std::shared_ptr<void> &current_val) override {
        throw std::runtime_error("Calling Update method of GroupBuilder - assertion error.");
    }

    shared_ptr<void> InitBatch() override {
        throw std::runtime_error("Calling InitBatch method of GroupBuilder - assertion error.");
    }

    void UpdateBatch(shared_ptr<void> &group) override {
        throw std::runtime_error("Calling UpdateBatch method of GroupBuilder - assertion error.");
    }

private:
    std::unique_ptr<common::TypedValueArrayIter<T_IN>> array_iter = nullptr;
};

template<typename ARRAY, typename BUILDER>
class StringGroupBuilder : public GroupBuilder<arrow::util::string_view, BUILDER> {
public:
    using GroupBuilder<arrow::util::string_view, BUILDER>::GroupBuilder;

    void SetArrayIter(std::unique_ptr<common::ArrayIter> iter) override {
        this->array_iter = std::unique_ptr<common::StringArrayIter<ARRAY>>{
                static_cast<common::StringArrayIter<ARRAY>*>(iter.release())
        };
    }


    std::shared_ptr<void> Init(int row_idx) override {
        if (this->array_iter->IsNull(row_idx)) {
            return std::shared_ptr<std::string>(nullptr);
        } else {
            return std::make_shared<std::string>(this->array_iter->GetString(row_idx));
        }
    }

    void Summarize(const std::shared_ptr<void>& current_val) override {
        if (current_val != nullptr) {
            auto val = std::static_pointer_cast<std::string>(current_val);
            ::vinum::operators::aggregate::AppendToBuilder<BUILDER, std::string>(this->builder, val);
        } else {
            this->builder->UnsafeAppendNull();
        }
    }

private:
    std::unique_ptr<common::StringArrayIter<ARRAY>> array_iter = nullptr;
};

}  // namespace vinum::operators::aggregate
