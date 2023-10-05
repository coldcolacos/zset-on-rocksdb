# git clone
rm -rf rocksdb
git clone git@github.com:facebook/rocksdb.git
cd rocksdb

# build static lib
git checkout tags/v6.11.4
make static_lib -j 4        # fixed 2 warnings here

# install
rm -rf /usr/local/include/rocksdb
cp -r include/rocksdb/ /usr/local/include/
cp librocksdb.a /usr/local/lib

# clean
cd ..
rm -rf rocksdb
