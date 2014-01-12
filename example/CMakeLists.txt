cmake_minimum_required (VERSION 2.8)
project(job_stream_example)

include_directories("../")
include_directories("../include")

# Have to use the OpenMPI compiler...
set(CMAKE_CXX_COMPILER mpicxx)

# Enable c++11
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

add_executable(job_stream_example
        main.cpp)

target_link_libraries(job_stream_example
        "${CMAKE_SOURCE_DIR}/../build/libjob_stream.a")

# FUTURE - It sure would be nice if everything below this point could go away.
# But, for now, not worth the time.
SET(BOOST_LIB "${CMAKE_SOURCE_DIR}/../lib/boost")
SET(YAML_LIB "${CMAKE_SOURCE_DIR}/../lib/yaml-cpp")

target_link_libraries(job_stream_example ${BOOST_LIB}/libboost_system.a)
target_link_libraries(job_stream_example ${BOOST_LIB}/libboost_thread.a)
target_link_libraries(job_stream_example ${BOOST_LIB}/libboost_serialization.a)
target_link_libraries(job_stream_example ${BOOST_LIB}/libboost_mpi.a)
target_link_libraries(job_stream_example ${YAML_LIB}/libyaml-cpp.a)

# rt must be last
target_link_libraries(job_stream_example rt)
