// Copied and adapted from DuckDB. duckdb/common/types.hpp
// https://github.com/cwida/duckdb/blob/master/src/include/duckdb/common/types.hpp

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>

using namespace std;

namespace vinum::common {


struct hugeint_t {
public:
    uint64_t lower;
    int64_t upper;

public:
    hugeint_t() = default;

    hugeint_t(int64_t value);

    hugeint_t(const hugeint_t &rhs) = default;

    hugeint_t(hugeint_t &&rhs) = default;

    hugeint_t &operator=(const hugeint_t &rhs) = default;

    hugeint_t &operator=(hugeint_t &&rhs) = default;

    std::string ToString() const;

    // comparison operators
    bool operator==(const hugeint_t &rhs) const;

    bool operator!=(const hugeint_t &rhs) const;

    bool operator<=(const hugeint_t &rhs) const;

    bool operator<(const hugeint_t &rhs) const;

    bool operator>(const hugeint_t &rhs) const;

    bool operator>=(const hugeint_t &rhs) const;

    // arithmetic operators
    hugeint_t operator+(const hugeint_t &rhs) const;

    hugeint_t operator-(const hugeint_t &rhs) const;

    hugeint_t operator*(const hugeint_t &rhs) const;

    hugeint_t operator/(const hugeint_t &rhs) const;

    hugeint_t operator%(const hugeint_t &rhs) const;

    hugeint_t operator-() const;

    // bitwise operators
    hugeint_t operator>>(const hugeint_t &rhs) const;

    hugeint_t operator<<(const hugeint_t &rhs) const;

    hugeint_t operator&(const hugeint_t &rhs) const;

    hugeint_t operator|(const hugeint_t &rhs) const;

    hugeint_t operator^(const hugeint_t &rhs) const;

    hugeint_t operator~() const;

    // in-place operators
    hugeint_t &operator+=(const hugeint_t &rhs);

    hugeint_t &operator-=(const hugeint_t &rhs);

    hugeint_t &operator*=(const hugeint_t &rhs);

    hugeint_t &operator/=(const hugeint_t &rhs);

    hugeint_t &operator%=(const hugeint_t &rhs);

    hugeint_t &operator>>=(const hugeint_t &rhs);

    hugeint_t &operator<<=(const hugeint_t &rhs);

    hugeint_t &operator&=(const hugeint_t &rhs);

    hugeint_t &operator|=(const hugeint_t &rhs);

    hugeint_t &operator^=(const hugeint_t &rhs);
};

} // namespace vinum::common
