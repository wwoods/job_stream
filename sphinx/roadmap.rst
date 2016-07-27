Roadmap
=======

(These notes may be old)

* Memory management helper - psutil.get_memory_info().rss,
  psutil.phymem_usage().available, inline multiprocessing disable, etc.  Use
  statistical probabilities to determine the memory consumption per job,
  INCLUDING job_stream.invoke memory.  Assume one (or two) std deviations (or
  high watermark) of memory are required above average for job allocation.
  How to do this?
  Low watermark before jobs are handled.  Periodically sample memory usage,
  and ...?  Assume used memory is evenly distributed amongst jobs?  Python
  does multiprocessing... this affects these stats.  Hmm..

  Maybe what would be best is add a "memoryRequired" to SharedBase override.  This
  much RAM is required FOR THE WHOLE DURATION of the job.  E.g., it will double
  count memory.  Eventually, tracking avg time to completion + std dev, can fade
  out memory bias on running jobs.  But initially, naive is OK.

  Also, needs to be nice to other people's experiments.  That is, collaborate
  across job_stream instances (since I've got most of the lab using job_stream).
  Goals:
  - Maximize resources available to all job_streams (use all cores amongst
      job_streams, and all memory).
  - Distribute those resources evenly, but also greedily.  That is, not all
      job_streams will use 100% of what is available.  Ones that need more should
      expand into the gap.

  So...
  Distributed arbitration?  Requests and
  yields?  E.g., each job_stream status file has two fields: allocated
  cores & ram, and desired.  Allocated is moved towards desired based on
  capacity (cores, mb) minus sum of allocated.  Allocated is moved down when
  desired is lower.  Balanced across user, then jobs.

  Traverse parent pid chain to count all memory usage.  Allocated memory should
  probably be a virtual figure - that is, the allocated memory is IN ADDITION
  to actual allocations.  Although, that has a 50% error margin.  Other way
  to do it would be to have allocated memory be a minimum of sorts...
  memory = max(baseline + allocation, actual)

  We now have hard limits and soft limits.  Make sure we have a concept of
  running vs desired running, too.

* to: Should be a name or YAML reference, emit() or recur() should accept an
  argument of const YAML::Node& so that we can use e.g. stepTo: *priorRef as
  a normal config.  DO NOT overwrite to!  Allow it to be specified in pipes, e.g.

      - to: *other
        needsMoreTo: *next
      - &next
        type: ...
        to: output
      - &other
        type: ...

  In general, allow standard YAML rather than a specially split "to" member.
* Smarter serialization....... maybe hash serialized entities, and store a dict
  of hashes, so as to only write the same data once even if it is NOT a
  duplicated pointer.
* depth-first iteration as flag
* Ability to let job_stream optimize work size.  That is, your program says
  something like this->getChunk(__FILE__, __LINE__, 500) and then job_stream
  tracks time spent on communicating vs processing and optimizes the size of
  the work a bit...
* Fix timing statistics in continue'd runs from checkpoints
* Errors during a task should push the work back on the stack and trigger a
  checkpoint before exiting.  That would be awesome.  Should probably be an
  option though, since it would require "checkpointing" reduce accumulations
  and holding onto emitted data throughout each work's processing
* Prevent running code on very slow systems... maybe make a CPU / RAM sat
  metric by running a 1-2 second test and see how many cycles of computation
  we get, then compare across systems.  If we also share how many contexts each
  machine has, then stealing code can balance such that machines 1/2 as capable
  only keep half their cores busy maximum according to stealing.
* Progress indicator, if possible...
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
  serialize() is specified in serialization.h.  Also Frame needs doc.
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


