Changelog
=========

* 2018-7-30 - Fixed compilation with Boost 1.67.0.  Now using Boost's official boost::process implementation rather than the unofficial one that was bundled with job\_stream previously.

  Python version bumped to 0.1.32.
* 2017-3-4 - Allowed :meth:`job_stream.inline.Work.job` to be passed keyword argument ``useMultiprocessing`` to disable multiprocessing on a single-job basis.
* 2017-2-9 - Added stability for checkpoints and errors.  There was an issue with the Jan. 31st commit.  Version bump to 0.1.30.
* 2017-1-31 - Fixed checkpointInfo from python.  Slightly more rigorous preservation of data with checkpoints in the face of exceptions.
* 2016-10-27 - Fixed bug where requested a checkpoint file whose parent does not exist would cause the whole job_stream to hang.  Python version bump to 0.1.29.
* 2016-10-20 - Modified Processor behavior to automatically call MPI_Abort when an exception is thrown by a job.  This fixes some hangs when errors were raised in Python code, and should make job_stream behave more robustly in general.

  The job_stream binary was also modified to pass the ``mca`` flag ``orte_abort_on_non_zero_status``, to make sure that an arbitrary death of a process also causes MPI_Abort to be called.
* 2016-10-18 - Hostfile lines may now specify e.g. ``cpus=8`` after a hostname to
  manually set the number of cores to be used by job_stream on a given machine.
* 2016-10-12 - Added ``--map-by node`` to ``job_stream`` binary's flags for ``mpirun``.
  Apparently, newer versions of MPI restrict spawned processes to a single core.
  Since job_stream does its own parallelization, this prevented any
  parallelization at all on a single machine, which was disastrous.
* 2016-10-11 - :any:`job_stream.invoke` is now more intelligent about raising
  errors for non-existent executables.
* 2016-8-8 - Fixes for :meth:`job_stream.baked.sweep` as well as checkpoint
  continuations.

  :meth:`sweep` no longer erroneously produces negative standard deviations.
  Additionally, :meth:`sweep` logs to stderr to show that progress is
  occurring.

  Checkpoint continuations now correctly handle cpuCount.  Python bindings do
  not eagerly fork for multiprocessing tasks, leading to correct handling of
  cpuCount on continuation (vital for :meth:`sweep`).

  Minor C++ compilation errors fixed.

  Python version bumped to 0.1.24.
* 2016-7-28 - Minor change to :meth:`job_stream.baked.sweep`; now allows
  specifying minimum and maximum number of trials in trialsParms.  Should not
  be needed for most use cases, but for e.g. calculating pi naively, it is good
  to be able to raise the minimum number of trials above 3.

  Python version bumped to 0.1.23.
* 2016-7-27 - :mod:`job_stream.baked` module.  Vastly improved documentation.
  Python bump to 0.1.22.  job_stream.__version__ added in Python.
* 2016-7-13 - Minor fix for bin/job_stream; hosts now remain sorted in their
  original order, fixing the server with rank 0 to a specific host.
* 2016-7-07 - mpirun doesn't automatically forward environment variables; this
  has been fixed for the job_stream binary.  Python version bump to 0.1.19.
* 2016-7-05 - Checkpoints now make a .done file to prevent accidental results
  overwriting.  Updated job_stream binary to be able to specify checkpoints
  and to have a slightly improved interface.  Python version to 0.1.18.
* 2016-6-28 - Added job_stream binary to stop users from needing to know how
  to use mpirun, and more importantly, to open the way for uses like maxCpu
  or flagging other resources.
* 2016-6-27 - A fix and additional testing for multiprocessing timing code.
  Streamlined to be more effective to boot.  Version to 0.1.14.
* 2016-6-24 - Fixed python timing code for multiprocessing.  Reported CPU
  efficiencies are now valid for python code.  Version bump to 0.1.13.
* 2016-6-23 - job_stream now processes work in a depth-first fashion rather
  than breadth-first.  The utility of this change is for progress bars; no
  functionality should be altered as an effect of this change.

  Updated multiprocessing in Python driver to work with new code organization,
  multiprocessing was not effective before.  Stats still need to be fixed there.
  Version bump to 0.1.12.
* 2016-6-22 - inline.Multiple() in Python now ignores None results.
  Version 0.1.11.
* 2016-6-14 - Python 3 support and embedded two of the boost libraries that
  are not typically associated with boost-python.  In other words, a
  `conda install boost` preceding a `pip install job_stream` now works with
  both Python 2 and 3.  System boost libraries should still work as before
  (although job_stream might use an outdated version of boost-mpi).

  Version bump to 0.1.8, 0.1.9 and 0.1.10 (build fix).
* 2016-4-14 - Added a `map()` function that is compatible with the builtin
  `map()` function.
* 2015-5-26 - README warning about Frames and Reducers that store a list of
  objects.  Python inline frames can specify useMultiprocessing=False separate
  from the work.  Minor efficiency improvement to copy fewer Python object
  strings.
* 2015-5-21 - Build on Mac OS X is now fixed.
* 2015-3-26 - `job_stream.inline.Work` can now be used in `with` blocks and has
  a `finish()` method.  Args for `Work.run()` were moved to `Work`'s
  initializer.
* 2015-2-6 - job_stream.inline python module can disable multiprocessing by
  passing useMultiprocessing = False in the `Work` object's initializer.
* 2015-1-30 - Updated README to include job_stream.invoke, and exposed
  checkpointInfo function for debugging.
* 2015-1-29 - Added inline.result() function, which lets users write code that
  is executed exactly once per result, and always on the main host.
* 2015-1-28 - Added inline.init() function, which ensures code is only executed
  once regardless of checkpoint or host status.
* 2015-1-7 - Added job_stream.invoke to the python module.  Useful for launching
  an external process, e.g. Xyce.
* 2014-12-26 - Finished up job_stream.inline, the more intuitive way to
  parallelize using job_stream.  Minor bug fixes, working on README.  Need
  to curate everything and fix the final test_pipes.py test that is failing
  before redeploying to PyPI
* 2014-12-23 - Embedded yaml-cpp into job_stream's source to ease compilation.
  Bumped PyPI to 0.1.3.
* 2014-12-22 - Finished python support (initial version, anyway).  Supports
  config, multiprocessing, proper error reporting.  Pushed version 0.1.2 to
  PyPI :)
* 2014-12-18 - Python support.  Frame methods renamed for clarity
  (handleWork -> handleNext).  Frames may now be specified as a string for
  type, just like reducers.
* 2014-12-04 - Checkpoints no longer are allowed for interactive mode.  All input must be spooled into the system before a checkpoint will be allowed.
* 2014-11-14 - Fixed job_stream checkpoints to be continuous.  That is, a checkpoint no longer needs current work to finish in order to complete.  This
  cuts the runtime for checkpoints from several hours in some situations down
  to a couple of seconds.  Also, added test-long to cmake, so that tests can
  be run repeatedly for any period of time in order to track down transient
  failures.

  Fixed a bug with job_stream::invoke which would lock up if a program wrote
  too much information to stderr or stdout.

  Re-did steal ring so that it takes available processing power into account.
* 2014-11-06 - Fixed invoke::run up so that it supported retry on user-defined
  transient errors (For me, Xyce was having issues creating a sub directory
  and would crash).
* 2014-11-03 - Added --checkpoint-info for identifying what makes checkpoint
  files so large sometimes.  Miscellaneous cleanup to --help functionality.
  Serialization will refuse to serialize a non-pointer version of a polymorphic
  class, since it takes a long time to track down what's wrong in that
  situation.
* 2014-10-17 - Apparently yaml-cpp is not thread safe.  Wtf.  Anyway, as a
  "temporary" solution, job_stream now uses some custom globally locked classes
  as a gateway to yaml-cpp.  All functionality should still work exactly like
  vanilla yaml-cpp.

  Also, no work happens during a checkpoint now.  That was causing corrupted
  checkpoint files with duplicated ring tests.
* 2014-9-10 - Fixed up duplicated and end-of-job-sequence (output) submodules.
  Host name is now used in addition to MPI rank when reporting results.
* 2014-6-13 - Finalized checkpoint code for initial release.  A slew of new
  tests.
* 2014-4-24 - Fixed up shared_ptr serialization.  Fixed synchronization issue
  in reduction rings.
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
