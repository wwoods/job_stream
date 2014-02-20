set -e
cd ../build
cmake .. && make -j4
cd ../example/build
cmake .. && make -j4 $1

