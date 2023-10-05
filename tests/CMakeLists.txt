cmake_minimum_required(VERSION 3.15.0)
project(zset-on-rocksdb-test)

set(CMAKE_CXX_STANDARD 17)

enable_testing()
add_subdirectory(googletest-release-1.12.0)
include_directories(
    ..
    ../third_party
    googletest-release-1.12.0/googletest/include
)

add_executable(test_zset test_zset.cc)
target_link_libraries(
    test_zset
    gtest gtest_main
    -L/usr/local/lib -lrocksdb -lpthread -lz -llz4 -lsnappy -lbz2
)

add_test(NAME test_zset COMMAND test_zset)
