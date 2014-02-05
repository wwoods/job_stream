set -e
cd ../build
cmake .. && make
cd ../example/build
cmake .. && make

