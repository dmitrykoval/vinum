#pragma once


#define RAISE_ON_ARROW_FAILURE(expr)               \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      throw std::runtime_error(status_.message()); \
    }                                              \
  } while (0);
