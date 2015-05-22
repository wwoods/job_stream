cmake_minimum_required (VERSION 2.8)
project(job_stream)

# Note - run "cmake .." from the build/ directory.

# Library can be built and tested with "cmake .. && make test"
# Since some tests, specifically the checkpoint ones, fail transiently when
# something is wrong, there is also the test-long target, which runs the tests
# over and over until something fails

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

include_directories(job_stream/boost_process/ yaml-cpp-0.5.1/include/)
find_library(BOOST_FILESYSTEM_LIBRARY
        NAMES boost_filesystem
        HINTS ENV LD_LIBRARY_PATH ENV LIBRARY_PATH)
find_library(BOOST_SYSTEM_LIBRARY
        NAMES boost_system
        HINTS ENV LD_LIBRARY_PATH ENV LIBRARY_PATH)
find_library(BOOST_THREAD_LIBRARY
        NAMES libboost_thread.so
        HINTS ENV LD_LIBRARY_PATH ENV LIBRARY_PATH)
find_library(BOOST_SERIALIZATION_LIBRARY
        NAMES boost_serialization
        HINTS ENV LD_LIBRARY_PATH ENV LIBRARY_PATH)
find_library(BOOST_MPI_LIBRARY
        NAMES boost_mpi
        HINTS ENV LD_LIBRARY_PATH ENV LIBRARY_PATH)
find_library(BOOST_REGEX_LIBRARY
        NAMES boost_regex
        HINTS ENV LD_LIBRARY_PATH ENV LIBRARY_PATH)

# CMake bug?  If we reference libraries in target_link_libraries as in their
# resolved variable form above (${BOOST_MPI_LIBRARY}), then this doesn't
# build.
add_library(boost_filesystem UNKNOWN IMPORTED)
set_property(TARGET boost_filesystem PROPERTY IMPORTED_LOCATION 
        "${BOOST_FILESYSTEM_LIBRARY}")
add_library(boost_system UNKNOWN IMPORTED)
set_property(TARGET boost_system PROPERTY IMPORTED_LOCATION 
        "${BOOST_SYSTEM_LIBRARY}")
add_library(boost_thread UNKNOWN IMPORTED)
set_property(TARGET boost_thread PROPERTY IMPORTED_LOCATION 
        "${BOOST_THREAD_LIBRARY}")
add_library(boost_serialization UNKNOWN IMPORTED)
set_property(TARGET boost_serialization PROPERTY IMPORTED_LOCATION 
        "${BOOST_SERIALIZATION_LIBRARY}")
add_library(boost_mpi UNKNOWN IMPORTED)
set_property(TARGET boost_mpi PROPERTY IMPORTED_LOCATION 
        "${BOOST_MPI_LIBRARY}")
add_library(boost_regex UNKNOWN IMPORTED)
set_property(TARGET boost_regex PROPERTY IMPORTED_LOCATION 
        "${BOOST_REGEX_LIBRARY}")

SET(REQ_LIB
        boost_filesystem
        boost_system
        boost_thread
        boost_serialization
        boost_mpi)

add_library(job_stream
        job_stream/invoke.cpp
        job_stream/job_stream.cpp
        job_stream/job.cpp
        job_stream/message.cpp
        job_stream/module.cpp
        job_stream/processor.cpp
        job_stream/pythonType_noPython.cpp
        job_stream/serialization.cpp
        job_stream/types.cpp
        job_stream/workerThread.cpp
        job_stream/death_handler/death_handler.cc

        # yaml-cpp-0.5.1 - Packaged with job_stream for convenience
        yaml-cpp-0.5.1/src/binary.cpp
        yaml-cpp-0.5.1/src/convert.cpp
        yaml-cpp-0.5.1/src/directives.cpp
        yaml-cpp-0.5.1/src/emit.cpp
        yaml-cpp-0.5.1/src/emitfromevents.cpp
        yaml-cpp-0.5.1/src/emitter.cpp
        yaml-cpp-0.5.1/src/emitterstate.cpp
        yaml-cpp-0.5.1/src/emitterutils.cpp
        yaml-cpp-0.5.1/src/exp.cpp
        yaml-cpp-0.5.1/src/memory.cpp
        yaml-cpp-0.5.1/src/nodebuilder.cpp
        yaml-cpp-0.5.1/src/node.cpp
        yaml-cpp-0.5.1/src/node_data.cpp
        yaml-cpp-0.5.1/src/nodeevents.cpp
        yaml-cpp-0.5.1/src/null.cpp
        yaml-cpp-0.5.1/src/ostream_wrapper.cpp
        yaml-cpp-0.5.1/src/parse.cpp
        yaml-cpp-0.5.1/src/parser.cpp
        yaml-cpp-0.5.1/src/regex.cpp
        yaml-cpp-0.5.1/src/scanner.cpp
        yaml-cpp-0.5.1/src/scanscalar.cpp
        yaml-cpp-0.5.1/src/scantag.cpp
        yaml-cpp-0.5.1/src/scantoken.cpp
        yaml-cpp-0.5.1/src/simplekey.cpp
        yaml-cpp-0.5.1/src/singledocparser.cpp
        yaml-cpp-0.5.1/src/stream.cpp
        yaml-cpp-0.5.1/src/tag.cpp
        yaml-cpp-0.5.1/src/contrib/graphbuilder.cpp
        yaml-cpp-0.5.1/src/contrib/graphbuilderadapter.cpp
        )
target_link_libraries(job_stream ${REQ_LIB})

# Examples
add_executable(example/job_stream_example example/main.cpp)
add_custom_command(TARGET example/job_stream_example
        PRE_BUILD COMMAND mkdir -p example)
target_link_libraries(example/job_stream_example job_stream)
target_include_directories(example/job_stream_example PUBLIC .)

add_executable(example/job_stream_neuron example/neuron.cpp)
add_custom_command(TARGET example/job_stream_neuron
        PRE_BUILD COMMAND mkdir -p example)
target_link_libraries(example/job_stream_neuron job_stream)
target_include_directories(example/job_stream_neuron PUBLIC .)

add_executable(example/job_stream_stable example/stable.cpp)
add_custom_command(TARGET example/job_stream_stable
        PRE_BUILD COMMAND mkdir -p example)
target_link_libraries(example/job_stream_stable job_stream)
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
        boost_regex)
target_include_directories(test_job_stream PUBLIC
        "./"
        "test/libexecstream/")
add_dependencies(test_job_stream example)

add_custom_target(test DEPENDS test_job_stream)
add_custom_command(TARGET test
        POST_BUILD COMMAND test_job_stream "\${ARGS}")

# Since some tests may intermittently fail, test-long just re-runs the tests
# forever, until something fails.
add_custom_target(test-long DEPENDS test_job_stream)
add_custom_command(TARGET test-long
        POST_BUILD COMMAND /bin/sh -c 'echo \"Starting tests\" && while [ $$? -eq 0 ]\; do ./test_job_stream "\${ARGS}"\; done')

add_custom_target(job_stream_python)
add_custom_command(TARGET job_stream_python
        POST_BUILD COMMAND /bin/sh -c 'cd .. && pip install --user --upgrade . || pip install --upgrade .')

# Test the python module
add_custom_target(test-python DEPENDS job_stream_python)
add_custom_command(TARGET test-python
        POST_BUILD COMMAND /bin/sh -c 'cd ../python && nosetests \${ARGS}')
