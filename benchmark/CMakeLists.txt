cmake_minimum_required(VERSION 3.15.0)
project(zset-on-rocksdb-benchmark)

set(CMAKE_CXX_STANDARD 17)

include_directories(
    ..
    ../third_party
)

foreach(exec benchmark)
add_executable(${exec} ${exec}.cc)
target_link_libraries(
    ${exec}
    -L/usr/local/lib -lrocksdb -lpthread -lz -llz4 -lsnappy -lbz2
    -O3
)
endforeach(exec)
