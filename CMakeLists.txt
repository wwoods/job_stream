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
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -ftemplate-backtrace-limit=0")

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
include_directories("job_stream/boost_process/")

SET(REQ_LIB
        ${BOOST_LIB}/libboost_filesystem.a
        ${BOOST_LIB}/libboost_system.a
        ${BOOST_LIB}/libboost_thread.a
        ${BOOST_LIB}/libboost_serialization.a
        ${BOOST_LIB}/libboost_mpi.a
        ${YAML_LIB}/libyaml-cpp.a
        # rt must be last
        rt)

add_library(job_stream
        job_stream/invoke.cpp
        job_stream/job_stream.cpp
        job_stream/job.cpp
        job_stream/message.cpp
        job_stream/module.cpp
        job_stream/processor.cpp
        job_stream/serialization.cpp
        job_stream/workerThread.cpp
        job_stream/death_handler/death_handler.cc)
target_link_libraries(job_stream ${REQ_LIB})

# Examples
add_executable(example/job_stream_example example/main.cpp)
add_custom_command(TARGET example/job_stream_example
        PRE_BUILD COMMAND mkdir -p example)
target_link_libraries(example/job_stream_example job_stream ${REQ_LIB})
target_include_directories(example/job_stream_example PUBLIC .)

add_executable(example/job_stream_neuron example/neuron.cpp)
add_custom_command(TARGET example/job_stream_neuron
        PRE_BUILD COMMAND mkdir -p example)
target_link_libraries(example/job_stream_neuron job_stream ${REQ_LIB})
target_include_directories(example/job_stream_neuron PUBLIC .)

add_executable(example/job_stream_stable example/stable.cpp)
add_custom_command(TARGET example/job_stream_stable
        PRE_BUILD COMMAND mkdir -p example)
target_link_libraries(example/job_stream_stable job_stream ${REQ_LIB})
target_include_directories(example/job_stream_stable PUBLIC .)

add_custom_target(example)
add_dependencies(example
        example/job_stream_example
        example/job_stream_neuron
        example/job_stream_stable)

# Tests
FILE(GLOB TEST_SOURCES "job_stream/test/*.cpp" "test/*.cpp")
add_executable(test_job_stream
        ${TEST_SOURCES})
target_link_libraries(test_job_stream
        job_stream
        ${REQ_LIB}
        # std::regex is horribly, horribly broken in gcc 4.8.  @&#^%&
        ${BOOST_LIB}/libboost_regex.a)
target_include_directories(test_job_stream PUBLIC
        "include/"
        "./"
        "test/libexecstream/")
add_dependencies(test_job_stream example)

add_custom_target(test DEPENDS test_job_stream)
add_custom_command(TARGET test
        POST_BUILD COMMAND test_job_stream "\${ARGS}")
