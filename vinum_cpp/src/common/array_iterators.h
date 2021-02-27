#pragma once

#include <arrow/api.h>

#include <memory>
#include <cstdint>

#include <iostream>


namespace vinum::common {

class ArrayIter {
public:
    ArrayIter() = default;

    virtual ~ArrayIter() = default;

    virtual void SetArray(const std::shared_ptr<void>& arr) = 0;

    [[nodiscard]] virtual bool HasMore() const = 0;

    [[nodiscard]] virtual int64_t Length() const = 0;

    [[nodiscard]] virtual int64_t NonNullCount() const = 0;

    [[nodiscard]] inline bool IsNull() const {
        return (nulls_ptr != nullptr) && !arrow::BitUtil::GetBit(nulls_ptr, nulls_idx);
    }

    [[nodiscard]] inline virtual bool IsNull(int64_t idx) const = 0;

    bool NextNull() {
        bool is_null = this->IsNull();
        this->MoveNext();
        return is_null;
    }
    bool NextIfNull() {
        bool is_null = this->IsNull();
        if (is_null) {
            this->MoveNext();
        }
        return is_null;
    }

    virtual uint64_t NextAsUInt64() = 0;

protected:
    const uint8_t *nulls_ptr = nullptr;
    int64_t nulls_idx = 0;
    int64_t current_idx = 0;
    int64_t length = 0;

    inline virtual void MoveNext() = 0;
};


class GenericArrayIter : public ArrayIter {
public:

    void SetArray(const std::shared_ptr<void>& arr) override {
        this->array = std::static_pointer_cast<arrow::Array>(arr);
        this->nulls_ptr = this->array->null_bitmap_data();
        this->nulls_idx = this->array->offset();
        this->current_idx = 0;
        this->length = this->array->length();
    }

    [[nodiscard]] bool HasMore() const override {
        return this->current_idx < this->length;
    }

    [[nodiscard]] int64_t Length() const override {
        return this->length;
    }

    [[nodiscard]] int64_t NonNullCount() const override {
        return this->length - this->array->null_count();
    }

    [[nodiscard]] inline bool IsNull(int64_t idx) const override {
        return this->array->IsNull(idx);
    }

    uint64_t NextAsUInt64() override {
        throw std::runtime_error("NextAsUInt64() is not supported by GenericArrayIter.");
    }

protected:
    std::shared_ptr<arrow::Array> array = nullptr;

    inline void MoveNext() override {
        this->current_idx++;
        this->nulls_idx++;
    }
};


template<typename T>
class TypedValueArrayIter : public ArrayIter {
public:

    using ArrayIter::ArrayIter;

    virtual T Next() = 0;

    virtual T GetValue(int64_t idx) = 0;
};


template<typename T, typename ARRAY>
class GetViewArrayIter : public TypedValueArrayIter<T> {
public:

    void SetArray(const std::shared_ptr<void>& arr) override {
        this->array = std::static_pointer_cast<ARRAY>(arr);
        this->nulls_ptr = this->array->null_bitmap_data();
        this->nulls_idx = this->array->offset();
        this->current_idx = 0;
        this->length = this->array->length();
    }

    [[nodiscard]] bool HasMore() const override {
        return this->current_idx < this->length;
    }

    [[nodiscard]] int64_t Length() const override {
        return this->length;
    }

    [[nodiscard]] int64_t NonNullCount() const override {
        return this->length - this->array->null_count();
    }

    [[nodiscard]] inline bool IsNull(int64_t idx) const override {
        return this->array->IsNull(idx);
    }

    T Next() override {
        auto idx = this->current_idx;
        this->MoveNext();
        return this->array->GetView(idx);
    }

    T GetValue(int64_t idx) override {
        return this->array->GetView(idx);
    }

    uint64_t NextAsUInt64() override {
        throw std::runtime_error("NextAsUInt64() is not supported by GetViewArrayIter.");
    }


protected:
    std::shared_ptr<ARRAY> array = nullptr;

    inline void MoveNext() override {
        this->current_idx++;
        this->nulls_idx++;
    }
};


template<typename ARRAY>
class StringArrayIter : public GetViewArrayIter<arrow::util::string_view, ARRAY> {
public:
    std::string GetString(int64_t idx) {
        return this->array->GetString(idx);
    }
};


template<typename T>
class NumericArrayIter : public TypedValueArrayIter<typename T::c_type> {
public:
    using DType = typename T::c_type;

    using TypedValueArrayIter<typename T::c_type>::TypedValueArrayIter;

    void SetArray(const std::shared_ptr<void>& arr) override {
        this->array = std::static_pointer_cast<arrow::NumericArray<T>>(arr);
        native_arr_ptr = (DType *) this->array->raw_values();
        this->nulls_ptr = this->array->null_bitmap_data();
        this->nulls_idx = this->array->offset();
        this->current_idx = 0;
        this->length = this->array->length();
    }

    [[nodiscard]] bool HasMore() const override {
        return this->current_idx < this->length;
    }

    [[nodiscard]] int64_t Length() const override {
        return this->length;
    }

    [[nodiscard]] int64_t NonNullCount() const override {
        return this->length - this->array->null_count();
    }

    [[nodiscard]] inline bool IsNull(int64_t idx) const override {
        return this->array->IsNull(idx);
    }

    DType Next() override {
        auto val = *native_arr_ptr;
        this->MoveNext();
        return val;
    }

    DType GetValue(int64_t idx) override {
        return this->array->GetView(idx);
    }

    uint64_t NextAsUInt64() override {
        return static_cast<uint64_t>(this->Next());
    }

protected:
    std::shared_ptr<arrow::NumericArray<T>> array = nullptr;
    const DType *native_arr_ptr = nullptr;

    inline void MoveNext() override {
        this->current_idx++;
        this->nulls_idx++;
        native_arr_ptr++;
    }
};

template<typename T>
class FloatArrayIter : public NumericArrayIter<T> {
public:
    using DType = typename T::c_type;

    FloatArrayIter() : NumericArrayIter<T>::NumericArrayIter() {
        assert(sizeof(DType) <= sizeof(uint64_t));
    }

    uint64_t NextAsUInt64() override {
        return this->floatToInt(this->Next());
    }

private:
    inline uint64_t floatToInt(DType v) const {
        uint64_t r = 0;
        memcpy(&r, &v, sizeof(DType));
        return r;
    }
};


std::unique_ptr<ArrayIter> array_iter_factory(arrow::Type::type type);


}  // namespace vinum::common
