cmake_minimum_required(VERSION 3.14)

PROJECT(rocksdb_interop)

set(CMAKE_VERBOSE_MAKEFILE on)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_VERBOSE_MAKEFILE on)

add_library(rocksdb_interop SHARED rocksdb_interop.cpp rocksdb_interop.h)

find_library(rocksdb NAMES librocksdb.so PATHS ../../rocksdb)
target_link_libraries(rocksdb_interop ${rocksdb})

