Job Stream
==========

A tiny C library based on OpenMPI for distributing streamed batch processing.


Requirements
------------

boost (mpi, serialization, thread)
yaml-cpp

Building
--------

Create a build/ folder, cd into it, and run:

    cmake .. && make

This will instruct you on how to configure the build environment, and then will
build the library.


Basics
------

The following example is fully configured in the "example" subdirectory.

Essentially, you code some jobs, and optionally a reducer for combining results:

    #include <job_stream/job_stream.h>

    /** Add one to any integer we receive */
    class AddOneJob : public job_stream::Job<int> {
    public:
        static AddOneJob* make() { return new AddOneJob(); }

        void handleWork(int& work) {
            this->emit(work + 1);
        }
    };


    class DuplicateJob : public job_stream::Job<int> {
    public:
        static DuplicateJob* make() { return new DuplicateJob(); }

        void handleWork(int& work) {
            this->emit(work);
            this->emit(work);
        }
    };


    class SumReducer : public job_stream::Reducer<int> {
    public:
        static SumReducer* make() { return new SumReducer(); }

        void handleInit(int& current) {
            //Must be callable multiple times.  That is, T_accum might be 
            //instantiated on different hosts, and later merged via handleMore.
            current = 0;
        }

        void handleMore(int& current, int& more) {
            current += more;
        }

        void handleDone(int& current) {
            this->emit(current);
        }
    };

Register them in your main, and call up a processor:

    int main(int argc, char* argv []) {
        job_stream::addJob("addOne", AddOneJob::make);
        job_stream::addJob("duplicate", DuplicateJob::make);
        job_stream::addReducer("sum", SumReducer::make);
        job_stream::runProcessor(argc, argv);
        return 0;
    }

Define a pipeline / configuration:

    # example1.yaml
    reducer: sum
    jobs:
        - type: addOne
        - type: addOne

And run it!

    # This will compute 45 + 2 and 7 + 2 separately, then sum them, returning
    # one number (because of the reducer).
    $ mpirun -np 4 ./job_stream_example example1.yaml <<!
        45
        7
        !
    56
    $ 

Want to get a little more complicated?  You can embed modules:

    # example2.yaml
    jobs:
        - type: addOne
        # Not defining type (or setting it to "module") starts a new module
        # that can have its own reducer and job chain
        -   reducer: sum
            jobs:
                - type: duplicate

That pipeline will, individually for each input row, add one and double it:

    $ mpirun -np 4 ./job_stream_example example2.yaml <<!
        1
        2
        3
        !
    4
    6
    8
    $

Does your program have more complex flow?  The emit() function can take a second
argument, which is the name of the target to route to.  For instance, if we add
to main.cpp:

    class GetToTenJob : public job_stream::Job<int> {
    public:
        static GetToTenJob* make() { return new GetToTenJob(); }

        void handleWork(int& work) {
            if (work < 10) {
                this->emit(work, "keep_going");
            }
            else {
                this->emit(work, "done");
            }
        }
    };

    //Remember to register it in main...

And then you set up example3.yaml:

    # example3.yaml
    # Note that our module now has an "input" field - this determines the first
    # job to receive work.  Our "jobs" field is now a map instead of a list, 
    # with the key being the id of each job.  "to" determines where emitted
    # work goes - if "to" is a mapping, the job uses "emit" with a second 
    # argument to guide each emitted work.
    input: checkValue
    jobs:
        addOne:
            type: addOne
            to: checkValue
        checkValue:
            type: getToTen
            to:
                keep_going: addOne
                done: output

Run it:

    $ mpirun -np 4  ./job_stream_example example3.yaml <<!
        1
        8
        12
        !
    12
    10
    10
    $

Note that the "12" is output first, since it got routed to output almost 
immediately rather than having to pass through many AddOneJobs.

Roadmap
-------

* Reductions should always happen locally; a dead ring should merge them.
* Doxygen documentation
* Tests
