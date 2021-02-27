set(CMAKE_VERBOSE_MAKEFILE ON)

SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}")

add_library(vinum
        common/huge_int.cpp
        common/array_iterators.cpp
        operators/aggregate/agg_func_factory.cpp
        operators/aggregate/base_aggregate.cpp
        operators/aggregate/one_group_aggregate.cpp
        operators/aggregate/single_numerical_hash_aggregate.cpp
        operators/aggregate/multi_numerical_hash_aggregate.cpp
        operators/aggregate/generic_hash_aggregate.cpp
        operators/sort/sort.cpp
        operators/table_batch_reader.cpp)
target_link_libraries(vinum PRIVATE arrow_shared)
