# Set core dump file path
#   sysctl -w kernel.core_pattern=/var/log/%e.core.%p
#   ulimit -c 1024

# Build
rm -rf build && mkdir build && cd build && cmake .. && make

# Run
./benchmark
