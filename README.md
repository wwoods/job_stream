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

    using std::unique_ptr;

    /** Add one to any integer we receive */
    class AddOneJob : public job_stream::Job<int> {
    public:
        static AddOneJob* make() { return new AddOneJob(); }

        void handleWork(unique_ptr<int> work) {
            this->emit(*work + 1);
        }
    };


    class DuplicateJob : public job_stream::Job<int> {
    public:
        static DuplicateJob* make() { return new DuplicateJob(); }

        void handleWork(unique_ptr<int> work) {
            this->emit(*work);
            this->emit(*work);
        }
    };


    class GetToTenJob : public job_stream::Job<int> {
    public:
        static GetToTenJob* make() { return new GetToTenJob(); }

        void handleWork(unique_ptr<int> work) {
            if (*work < 10) {
                this->emit(*work, "keep_going");
            }
            else {
                this->emit(*work, "done");
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
        void handleAdd(int& current, unique_ptr<int> work) {
            current += *work;
        }

        /** Called to join this Reducer with the accumulator from another */
        void handleJoin(int& current, unique_ptr<int> other) {
            current += *other;
        }

        /** Called when the reduction is complete, or nearly - recur() may be used
            to keep the reduction alive (inject new work into this reduction). */
        void handleDone(int& current) {
            this->emit(current);
        }
    };


    class GetToValueReducer : public job_stream::Reducer<int> {
    public:
        static GetToValueReducer* make() { return new GetToValueReducer(); }

        void handleInit(int& current) {
            current = 0;
        }

        void handleAdd(int& current, unique_ptr<int> work) {
            //Everytime we get an output less than 2, we'll need to run it through
            //the system again.
            printf("Adding %i\n", *work);
            if (*work < 3) {
                this->recur(3);
            }
            current += *work;
        }

        void handleJoin(int& current, unique_ptr<int> other) {
            current += *other;
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

        void handleWork(unique_ptr<int> work) {
            if (*work < 10) {
                this->emit(*work, "keep_going");
            }
            else {
                this->emit(*work, "done");
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

Sometimes, passing -bind-to-core to mpirun can have a profoundly positive impact
on performance.


Roadmap
-------

* Make a checkpoint, convert to server / worker model in a single process
* Sleep 0 or 1 when we don't get any messages for awhile...
* Merge job\_stream\_inherit into job\_stream\_example (and test it)
* TIME\_COMM should not include initial isend request, since we're not using
  primitive objects and that groups in the serialization time
* Frame probably shouldn't need handleJoin (behavior would be wrong, since
  the first tuple would be different in each incarnation)
* Replace to: output with to: parent; input: output to input: reducer
* Consider replacing "reducer" keyword with "frame" to automatically rewrite
  recurTo as input and input as reducer
* Consider attachToNext() paired w/ emit and recur; attachments have their own
  getAttached<type>("label") retriever that returns a modifiable version of the
  attachment.  removeAttached("label").  Anyway, attachments go to all child
  reducers but are not transmitted via emitted() work from reducers.  Would 
  greatly simplify trainer / maximize code... though, if something is required,
  passing it in a struct is probably a better idea as it's a compile-time error.
  Then again, it wouldn't work for return values, but it would work for
  attaching return values to a recur'd tuple and waiting for it to come back
  around.
* Update README with serialization changes, clean up code.  Note that unique\_ptr
  serialize() is specified in serialization.h.
* Idle time tracking - show how much time is spent e.g. waiting on a reducer
* Solve config problem - if e.g. all jobs need to fill in some globally shared
  information (tests to run, something not in YAML)
* Python embedded bindings / application
* Reductions should always happen locally; a dead ring should merge them.  
    * Issue - would need a merge() function on the templated reducer base class.  Also, recurrence would have to re-initialize those rings.  Might be better to hold off on this one until it's a proven performance issue.
    * Unless, of course, T_accum == T_input always and I remove the second param.  Downsides include awkwardness if you want other components to feed into the reducer in a non-reduced format... but, you'd have to write a converter anyway (current handleMore).  So...
    * Though, if T_accum == T_input, it's much more awkward to make generic, modular components.  For instance, suppose you have a vector calculation.  Sometimes you just want to print the vectors, or route them to a splicer or whatever.  If you have to form them as reductions, that's pretty forced...
    * Note - decided to go with handleJoin(), which isn't used currently, but will be soon (I think this will become a small issue)
* Tests
* Subproject - executable integrated with python, for compile-less / easier work

Recent Changelog
----------------
* 2014-2-19 - Added Frame specialization of Reducer.  Expects a different
  first work than subsequent.  Usage pattern is to do some initialization work
  and then recur() additional work as needed.
* 2014-2-12 - Serialization is now via pointer, and supports polymorphic classes
  completely unambiguously via dynamic_cast and 
  job_stream::serialization::registerType.  User cpu % updated to be in terms of
  user time (quality measure) for each processor, and cumulative CPUs for 
  cumulative time.  
* 2014-2-5 - In terms of user ticks / wall clock ms, less_serialization is on
  par with master (3416 vs 3393 ticks / ms, 5% error), in addition
  to all of the other fixes that branch has.  Merged in.
* 2014-2-4 - Got rid of needed istream specialization; use an if and a 
  runtime\_exception.
* 2014-2-4 - handleWork, handleAdd, and handleJoin all changed to take a 
  unique\_ptr rather than references.  This allows preventing more memory 
  allocations and copies.  Default implementation with += removed.

