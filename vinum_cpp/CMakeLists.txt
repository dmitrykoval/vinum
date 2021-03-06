cmake_minimum_required(VERSION 3.13)
project(vinum)

set(CMAKE_CXX_STANDARD 17)

include_directories(src)

add_subdirectory(src)

if(ENABLE_TESTS)
    add_subdirectory(test EXCLUDE_FROM_ALL)
endif()
