cmake_minimum_required (VERSION 2.8)
project(job_stream)

# Note - run "cmake .." from the build/ directory.

# Note -- I had to compile boost by editing project-config.jam to include:
#
# using mpi : mpicxx.openmpi ;
# 
# And then running ./b2 --with-mpi to build the library.

# Have to use the OpenMPI compiler...
set(CMAKE_CXX_COMPILER mpicxx)

# Enable c++11
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

# Enable gprof
#add_definitions(-g)
#add_definitions(-pg)
#SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")

# Set up paths
SET(BOOST_INC "${CMAKE_SOURCE_DIR}/include/boost")
SET(BOOST_LIB "${CMAKE_SOURCE_DIR}/lib/boost")
SET(YAML_INC "${CMAKE_SOURCE_DIR}/include/yaml-cpp")
SET(YAML_LIB "${CMAKE_SOURCE_DIR}/lib/yaml-cpp")

# Check boost headers
if(NOT EXISTS "${BOOST_INC}/mpi.hpp")
    message(FATAL_ERROR "include/boost not configured; couldn't find "
            "include/boost/mpi.hpp")
endif()
# Check boost libs
if(NOT EXISTS "${BOOST_LIB}/libboost_mpi.a")
    message(FATAL_ERROR "lib/boost not configured; couldn't find "
            "lib/boost/libboost_mpi.a")
endif()
# Check yaml-cpp headers
if(NOT EXISTS "${YAML_INC}/yaml.h")
    message(FATAL_ERROR "include/yaml-cpp not configured; couldn't find "
            "include/yaml-cpp/yaml.h")
endif()
# Check yaml-cpp libs
if(NOT EXISTS "${YAML_LIB}/libyaml-cpp.a")
    message(FATAL_ERROR "lib/yaml-cpp not configured; couldn't find "
            "lib/yaml-cpp/build/lib-yaml.cpp.a")
endif()

include_directories("include/")

add_library(job_stream 
        job_stream/job_stream.cpp
        job_stream/job.cpp
        job_stream/message.cpp
        job_stream/module.cpp
        job_stream/processor.cpp)
target_link_libraries(job_stream ${BOOST_LIB}/libboost_system.a)
target_link_libraries(job_stream ${BOOST_LIB}/libboost_thread.a)
target_link_libraries(job_stream ${BOOST_LIB}/libboost_serialization.a)
target_link_libraries(job_stream ${BOOST_LIB}/libboost_mpi.a)
target_link_libraries(job_stream ${YAML_LIB}/libyaml-cpp.a)

# rt must be last
target_link_libraries(job_stream rt)
