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


    class SumReducer : public job_stream::Reducer<int> {
    public:
        static SumReducer* make() { return new SumReducer(); }

        /** Called to initialize the accumulator for this reduce.  May be called
            several times on different hosts, whose results will later be merged
            in handleJoin(). */
        void handleInit(int& current) {
            current = 0;
        }

        /** Used to add a new output to this Reducer */
        void handleAdd(int& current, int& work) {
            current += work;
        }

        /** Called to join this Reducer with the accumulator from another */
        void handleJoin(int& current, int& other) {
            current += other;
        }

        /** Called when the reduction is complete, or nearly - recur() may be used
            to keep the reduction alive (inject new work into this reduction). */
        void handleDone(int& current) {
            this->emit(current);
        }
    };


    //Another way to write SumReducer, relying on defaults (operator+):
    class Sum2Reducer : public job_stream::Reducer<int> {
        static Sum2Reducer* make() { return new Sum2Reducer(); }

        /** Init is needed just because int does not have an initializer.  User
            classes should specify an initializer rather than overloading 
            handleInit. */
        void handleInit(int& current) { current = 0; }
    };


    class GetToValueReducer : public job_stream::Reducer<int> {
    public:
        static GetToValueReducer* make() { return new GetToValueReducer(); }

        void handleInit(int& current) {
            current = 0;
        }

        void handleAdd(int& current, int& work) {
            //Everytime we get an output less than 2, we'll need to run it through
            //the system again.
            printf("Adding %i\n", work);
            if (work < 3) {
                this->recur(3);
            }
            current += work;
        }

        void handleJoin(int& current, int& other) {
            current += other;
        }

        void handleDone(int& current) {
            printf("Maybe done at %i\n", current);
            if (current >= this->config["value"].as<int>()) {
                this->emit(current);
            }
            else {
                //Not really done, put work back in as our accumulated value.
                this->recur(current);
            }
        }
    };

Register them in your main, and call up a processor:

    int main(int argc, char* argv []) {
        job_stream::addJob("addOne", AddOneJob::make);
        job_stream::addJob("duplicate", DuplicateJob::make);
        job_stream::addJob("getToTen", GetToTenJob::make);
        job_stream::addReducer("sum", SumReducer::make);
        job_stream::addReducer("getToValue", GetToValueReducer::make);
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

You can also have recurrence in your reducers - that is, if a reduction finishes
but the results do not match a criteria yet, you can put more tuples through
in the same reduction:

    # example4.yaml
    # Reducer recurrence
    reducer: 
        type: getToValue
        value: 100
    jobs:
        - type: duplicate
        - type: addOne

Running this with 1 will yield 188 - essentially, since handleAdd() calls recur
for each value less than 3, two additional "3" works get added into the system
early on.  So handleDone() gets called with 20, 62, and finally 188.


Words of Warning
----------------

Reducers get a non-const reference to an object.  This object is still 
temporary!  If you are adding it to a collection, you must copy the object.  Do
not store a pointer to that object.


Roadmap
-------

* Multiples is sometimes crashing with input stream error in Neurontest?????
* Reductions should always happen locally; a dead ring should merge them.  
    * Issue - would need a merge() function on the templated reducer base class.  Also, recurrence would have to re-initialize those rings.  Might be better to hold off on this one until it's a proven performance issue.
    * Unless, of course, T_accum == T_input always and I remove the second param.  Downsides include awkwardness if you want other components to feed into the reducer in a non-reduced format... but, you'd have to write a converter anyway (current handleMore).  So...
    * Though, if T_accum == T_input, it's much more awkward to make generic, modular components.  For instance, suppose you have a vector calculation.  Sometimes you just want to print the vectors, or route them to a splicer or whatever.  If you have to form them as reductions, that's pretty forced...
    * Note - decided to go with handleJoin(), which isn't used currently, but will be soon (I think this will become a small issue)
* Doxygen documentation
* Tests
* Subproject - executable integrated with python, for compile-less / easier work
