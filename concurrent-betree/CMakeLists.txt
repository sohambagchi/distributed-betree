cmake_minimum_required(VERSION 3.26)
project(concurrent_betree)

set(CMAKE_CXX_STANDARD 11)
include_directories(include)

add_executable(concurrent_betree main.cpp
        include/betree.hpp
        include/lock.hpp
        src/lock.cpp)
