
if [ ! -d "googletest-release-1.12.0" ]; then
  wget https://codeload.github.com/google/googletest/zip/refs/tags/release-1.12.0
  unzip release-1.12.0
  rm -f release-1.12.0
fi

rm -rf build && mkdir build && cd build

cmake .. && make && ./test_zset
