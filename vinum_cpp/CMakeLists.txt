cmake_minimum_required(VERSION 3.10)
project(vinum)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

#find_package(Arrow REQUIRED)

include_directories(src)

add_subdirectory(src)

if(ENABLE_TESTS)
    add_subdirectory(test EXCLUDE_FROM_ALL)
endif()
