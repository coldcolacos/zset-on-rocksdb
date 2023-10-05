# Build
rm -rf build
mkdir build
cd build
cmake ..
make

# Run
./simple_example
./custom_score_type_example
./recover_example
