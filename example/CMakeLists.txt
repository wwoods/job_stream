cmake_minimum_required (VERSION 2.8)
project(job_stream_example)

include_directories("../")
include_directories("../include")

# Have to use the OpenMPI compiler...
set(CMAKE_CXX_COMPILER mpicxx)

# Enable c++11
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

# Enable gprof
#add_definitions(-g)
#add_definitions(-pg)
#SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")

add_executable(job_stream_example
        main.cpp)

target_link_libraries(job_stream_example
        "${CMAKE_SOURCE_DIR}/../build/libjob_stream.a")

# FUTURE - It sure would be nice if everything below this point could go away.
# But, for now, not worth the time.
SET(BOOST_LIB "${CMAKE_SOURCE_DIR}/../lib/boost")
SET(YAML_LIB "${CMAKE_SOURCE_DIR}/../lib/yaml-cpp")
SET(JS_LIB 
        ${CMAKE_SOURCE_DIR}/../build/libjob_stream.a
        ${BOOST_LIB}/libboost_system.a
        ${BOOST_LIB}/libboost_thread.a
        ${BOOST_LIB}/libboost_serialization.a
        ${BOOST_LIB}/libboost_mpi.a
        ${YAML_LIB}/libyaml-cpp.a
        # rt must be last
        rt)

target_link_libraries(job_stream_example ${JS_LIB})

## Bonus neuron library for demo purposes...
add_executable(job_stream_neuron
        neuron.cpp)
target_link_libraries(job_stream_neuron ${JS_LIB})

## Stable timing exe
add_executable(job_stream_stable
        stable.cpp)

target_link_libraries(job_stream_stable ${JS_LIB})

## Inheritance test
add_executable(job_stream_inherit inherit.cpp)
target_link_libraries(job_stream_inherit ${JS_LIB})
