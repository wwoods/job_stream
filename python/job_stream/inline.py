r"""An intuitive way of specifying data flow using imperative programming
techniques.

For example, to count the average word length in a string:

.. code-block:: python

    from job_stream import inline

    # Begin a new job_stream, treating each word in the string as a piece
    # of work.
    with inline.Work("Hello there world, how are you?".split()) as w:
        @w.job
        def getLen(w):
            # Transform each word into its length.
            return len(w)

        # A reducer combines the above answers; the result from the reducer
        # will be the stored cumulative length divided by the number of words,
        # or in other words the mean length.
        @w.reduce(emit = lambda store: store.len / store.count)
        def findAverage(store, inputs, others):
            # Initialize the data store if it hasn't been.
            if not hasattr(store, 'init'):
                store.init = True
                store.count = 0
                store.len = 0.0

            # Add each word's length to ours, and increment count.
            for i in inputs:
                store.len += i
                store.count += 1
            # Merge with another reducer, adding their count and length to
            # our own.
            for o in others:
                store.len += o.len
                store.count += o.count

        @w.result
        def printer(result):
            # Print out the emit'd average.
            print(result)


The main class interacted with in ``inline`` code is
:class:`job_stream.inline.Work`, which provides methods for decorating Python
functions and remembers how the code was organized so that things are executed
in the right order.  See :class:`job_stream.inline.Work`'s documentation for
more information.


Hierarchy of ``job_stream.inline`` Pipes
========================================

Sometimes, code follows a common skeleton with some parallel code in the
middle.  The :mod:`job_stream.baked` library is an example of this;
:meth:`job_stream.baked.sweep` returns a context manager that yields a
:class:`job_stream.inline.Work` object used to tell ``sweep`` what operations
the user wishes to perform:

.. code-block:: python

    from job_stream.baked import sweep
    import numpy as np

    with sweep({ 'a': np.arange(10) }) as w:
        @w.job
        def square(id, trial, a):
            return { 'value': a*a + np.random.random() }

It is recommended that if a user class has functionality that can be added
within a ``job_stream`` pipe, it should expose that functionality in an
``@classmethod`` as ``jobs_{name}``:

.. code-block:: python

    from job_stream.inline import Args, Work

    class MyClass:
        def classify(inputs):
            return inputs[0]


        @classmethod
        def jobs_classify(w):
            @w.job
            def _classify(classifier, inputs):
                return classifier.classify(inputs)

    with Work([ Args(MyClass(), [i]) for i in range(10) ]) as w:
        MyClass.jobs_classify(w)

"""

import job_stream.common
import pickle
import inspect
import os

# Imports from job_stream
Object = job_stream.Object
getCpuCount = job_stream.getCpuCount
getRank = job_stream.getRank

# Used to register new classes so they can be pickle'd appropriately
_moduleSelf = globals()

_typeCount = [ 0 ]

class _ForceCheckpointJob(job_stream.common.Job):
    """Used for tests; raises exception after checkpoint."""
    def handleWork(self, work):
        self._forceCheckpoint(True)
        self.emit(work)



class _ForceCheckpointAndGoJob(job_stream.common.Job):
    """Used for tests; execution continues after checkpoint."""
    def handleWork(self, work):
        self._forceCheckpoint(False)
        self.emit(work)



class _UnwrapperJob(job_stream.common.Job):
    """Job to make processing after a reducer possible and sensical for inline.
    """
    USE_MULTIPROCESSING = False

    def handleWork(self, work):
        for w in work:
            self.emit(w)


class Args(object):
    """Automatically fans-out args and kwargs to the caller.  For example, the
    below will print out "Got 1, 2, 3, 4":

    .. code-block:: python

        from job_stream.inline import Args, Work

        with Work([ Args(1, 2, d=4, c=3) ]) as w:
            @w.job
            def handle(a, b, c, d):
                print("Got {}, {}, {}, {}".format(a, b, c, d))
    """
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class Multiple(object):
    """If some inline code wants to emit or recur multiple pieces of work, an instance of
    this class is the way to do it.  For instance:

    return Multiple([ 1, 2, 3 ])
    """

    def __init__(self, workList):
        self.workList = workList


class Work(object):
    """Represents a job_stream pipeline.  Similar to traditional imperative
    coding practices, :class:`Work` passes work in the same direction as the
    source file.  In other words, the system starts with something like:

    .. code-block:: python

        import job_stream.inline as inline
        with inline.Work([1, 2, 3]) as w:
            # ...

    This code will distribute the numbers 1, 2, and 3 into the system.  Once
    in the system, any decorated methods will process the data in some way.
    *The ordering of the decorated methods matters!*  For instance, running:

    .. code-block:: python

        with inline.Work([1]) as w:
            @w.job
            def first(w):
                print("a: {}".format(w))
                return w+1
            @w.job
            def second(w):
                print("b: {}".format(w))


    Will print ``a: 1`` and ``b: 2``.  If the list to :class:`Work` were
    longer, then more ``a:`` and ``b:`` lines would be printed, one pair for
    each input to the system.

    .. note:: Multiprocessing

        Python has the `GIL <https://wiki.python.org/moin/GlobalInterpreterLock>`_
        in the default implementation, which typically limits pure-python code
        to a single thread.  To get around this, the :mod:`job_stream` module
        by default uses multiprocessing for all jobs - that is, your python
        code will run in parallel on all cores, in different processes.

        If this behavior is not desired, particularly if your application loads
        a lot of data in memory that you would rather not duplicate, passing
        ``useMultiprocessing=False`` to the :class:`Work` object's initializer
        will force all job_stream activity to happen within the original
        process:

        .. code-block:: python

            Work([ 1, 2 ], useMultiprocessing=False)

        This may also be done to individual jobs / frames / reducers:

        .. code-block:: python

            @w.job(useMultiprocessing=False)
            def doSomething(work):
                pass

    """

    def __init__(self, initialWork = [], useMultiprocessing = True,
            **runKwargs):
        """May be passed a list or other iterable of initial work, as well as
        any of the following flags:

        Args:
            initialWork (list): The token(s) or bits of work that will be
                processed.  Each will be processed independently of the others;
                results might be joined late in the pipeline depending on its
                configuration.

            useMultiprocessing (bool): If True (the default), then all
                job_stream work will be handled in child processes spawned via
                Python's multiprocessing module.  This is used to get around
                the limitations of the GIL, but if your application's setup
                (not the jobs themselves) uses a lot of memory and your
                computation is handled by either non-GIL protected code or in
                another process anyway, it is appropriate to turn this off.

            checkpointFile (str): **Obsolete; use the ``job_stream`` binary.**
                If specified, job_stream will run in checkpoint
                mode, periodically dumping progress to this file.  If this file
                exists when run() is called, then simulation will be resumed
                from the state stored in this file.

            checkpointInterval (float): **Obsolete; use the ``job_stream``
                binary.** Time, in seconds, between the completion of one
                checkpoint and the beginning of the next.

            checkpointSyncInterval (float): **Debug / test usage only.**
                Time, in seconds, to wait for sync when taking a checkpoint.
        """
        self._initialWork = list(initialWork)
        self._initialWorkDepth = 0
        self._useMultiprocessing = useMultiprocessing
        self._runKwargs = dict(runKwargs)
        # The YAML config that we're building, essentially
        self._config = { 'jobs': [] }
        # Functions ran on init
        self._inits = []
        # Function ran to handle a result.  If set, nothing else may be added
        # to the pipe!
        self._resultHandler = None
        # finish() has been called
        self._hasFinish = False
        # parent node for current scope
        self._stack = [ { 'config': self._config } ]


    def __enter__(self):
        return self


    def __exit__(self, errType, value, tb):
        if errType is None:

            # If no result was specified, we do not want to cache and return
            # results either.  That is, there is no way to get the result of a
            # with block.
            if not self._hasFinish and self._resultHandler is None:
                @self.result
                def handleResult(r):
                    pass

            self.run()


    def init(self, func = None):
        """Decorates a method that is called only once on the primary host
        before any jobs run.  The method is never run when continuing from
        a checkpoint.

        Useful for e.g. creating folders, deleting temporary files from previous
        runs, etc."""
        if func is None:
            return lambda func2: self.init(func2)

        self._assertNoResult()
        self._assertFuncArgs(func, 0)

        self._inits.append(func)
        return func


    def finish(self, func = None):
        """Decorates a method that is called only once on the primary host,
        after all jobs have finished.

        The decorated method takes a single argument, which is a list of all
        results returned from the jobs within the stream.

        Anything returned within the decorated method will be emitted from the
        job_stream, meaning it will be returned by run().  User code should not
        typically rely on this; it is used mainly for e.g. IPython notebooks.

        Cannot be used if Work.result is used.  Work.result is preferred, if
        your job stream's output can be processed one piece at a time (more
        efficient in that results can be discarded after they are processed)."""
        if func is None:
            return lambda func2: self.finish(func2)

        self._assertNoResult()
        self._assertFuncArgs(func, 1)

        @self.reduce(store = lambda: [], emit = func)
        def collectResults(store, works, others):
            store.extend(works)
            for o in others:
                store.extend(o)

        self._hasFinish = True


    def frame(self, func = None, **kwargs):
        """Decorates a function that begins a frame.  A frame is started with
        a single piece of work, and then recurses other pieces of work into
        itself until some stopping condition.

        Frames (and their cousins Reducers) are the most complicated feature in
        `job_stream`.  A frame is appropriate if:

        * A while loop would be used in non-parallelizable code
        * Individual pieces of work need fan-out and fan-in

        Frames have three parts - an "all outstanding work is finished"
        handler, an aggregator, and everything in between, which is used to
        process recurred work.

        For example, suppose we want to sum all digits between 1 and our work,
        and report the result.  The best way to design this type of system is
        with a Frame, implemented in `inline` through :meth:`frame` and
        :meth:`frameEnd`.  The easiest way to think of these is as the two ends
        of a ``while`` loop - :meth:`frame` is evaluated as a termination
        condition, and is also evaluated before anything happens.
        :meth:`frameEnd` exists to aggregate logic from within the ``while``
        loop into something that ``frame`` can look at.

        .. code-block:: python

            from job_stream.inline import Work, Multiple
            w = Work([ 4, 5, 8 ])

            @w.frame
            def sumThrough(store, first):
                # Remember, this is called like the header of a while statement: once at
                # the beginning, and each time our recurred work finishes.  Anything
                # returned from this function will keep the loop running.
                if not hasattr(store, 'value'):
                    # Store hasn't been initialized yet, meaning that this is the first
                    # evaluation
                    store.first = first
                    store.value = 0
                    return first

                # If we reach here, we're done.  By not returning anything, job_stream knows
                # to exit the loop (finish the reduction).  The default behavior of frame is
                # to emit the store object itself, which is fine.

            # Anything between an @frame decorated function and @frameEnd will be executed
            # for anything returned by the @frame or @frameEnd functions.  We could have
            # returned multiple from @frame as well, but this is a little more fun

            @w.job
            def countTo(w):
                # Generate and emit as work all integers ranging from 1 to w, inclusive
                return Multiple(range(1, w + 1))

            @w.frameEnd
            def handleNext(store, next):
                # next is any work that made it through the stream between @frame and
                # @frameEnd.  In our case, it is one of the integers between 1 and our
                # initial work.
                store.value += next

            @w.result
            def printMatchup(w):
                print("{}: {}".format(w.first, w.value))

            w.run()

        Running the above code will print:

        .. code-block:: sh

            $ python script.py
            4: 10
            8: 36
            5: 15

        Note that the original work is out of order, but the sums line up.  This is
        because a frame starts a new reduction for each individual piece of work
        entering the `@frame` decorated function.

        Args:
            store: The storage object used to remember results from the frame.
            first: The first work that began this frame.

        Any results returned are recursed into the frame.

        Decorator kwargs:
            store - The constructor for a storage object.  Defaults to inline.Object
            emit - A function taking a storage object and returning the work that
                    should be forwarded to the next member of the stream.  Defaults
                    to emitting the store itself.
        """
        if func is None:
            # Non-decorating form
            return lambda func2: self.frame(func2, **kwargs)

        self._assertNoResult()
        self._assertFuncArgs(func, 2, True)
        fargs = Object()
        fargs.store = kwargs.pop('store', Object)
        fargs.emit = kwargs.pop('emit', lambda store: store)
        fargs.useMulti = kwargs.pop('useMultiprocessing', self._useMultiprocessing)

        if kwargs:
            raise KeyError("Unrecognized arguments: {}".format(kwargs.keys()))

        fconf = { 'jobs': [] }
        self._stack[-1]['config']['jobs'].append(fconf)
        self._stack.append({ 'frameFunc': func, 'frameArgs': fargs, 'config': fconf })

        return func


    def frameEnd(self, func = None):
        """Ends a frame.  The decorated function should accept two arguments:

        Args:
            store: The storage object used to remember results from the frame
                    (same as in the :meth:`frame` decorated method).
            next: The next result object that ran through the frame.

        Any results returned are recursed into the frame.
        """
        if func is None:
            # Non-decorating
            return lambda func2: self.frameEnd(func2)

        if 'frameFunc' not in self._stack[-1]:
            raise Exception("frameEnd call does not match up with frame!")

        self._assertNoResult()
        self._assertFuncArgs(func, 2, True)
        fargs = self._stack[-1]['frameArgs']
        funcStart = self._stack[-1]['frameFunc']
        # Note that the inline version skips straight from handleFirst to handleDone, then
        # handleNext based on recurrence.
        def handleFirst(s, store, first):
            store.first = first
            store.obj = fargs.store()
        def handleNext(s, store, next):
            if isinstance(next, Args):
                results = func(store.obj, *next.args, **next.kwargs)
            else:
                results = func(store.obj, next)
            self._listCall(s.recur, results)
        def handleDone(s, store):
            if isinstance(store.first, Args):
                results = funcStart(store.obj, *store.first.args,
                        **store.first.kwargs)
            else:
                results = funcStart(store.obj, store.first)
            hadRecur = self._listCall(s.recur, results)
            if not hadRecur:
                # Frame is done, so emit our results
                self._listCall(s.emit, fargs.emit(store.obj))

        frame = self._newType(self._stack[-1]['frameFunc'].__name__,
                job_stream.common.Frame, handleFirst = handleFirst,
                handleNext = handleNext, handleDone = handleDone,
                useMultiprocessing = fargs.useMulti)
        self._stack[-1]['config']['frame'] = frame
        self._stack.pop()
        # Since we always make a stack entry to start a frame, should never be empty here
        assert self._stack

        return func


    def job(self, func = None, **kwargs):
        """Decorates a job.  The decorated function must take one argument,
        which is the work coming into the job.  Anything returned is passed
        along to the next member of the stream.

        You may also call :meth:`job` as ``w.job(useMultiprocessing=False)`` to
        disable multiprocessing for this job.

        .. warning:: I/O Safety

            It is not safe to write non-unique external i/o (such as a file)
            within a job.  This is because jobs have no parallelism guards -
            that is, two jobs executing concurrently might open and append to a
            file at the same time.  On some filesystems, this results in e.g.
            two lines of a csv being combined into a single, invalid line.  To
            work around this, see :meth:`result`.

        """
        if func is None:
            # Invocation, not decoration
            return lambda func2: self.job(func2)

        kw = kwargs.copy()
        useMulti = kw.pop('useMultiprocessing', self._useMultiprocessing)
        if kw:
            raise ValueError("Unrecognized args: {}".format(kw))

        self._assertNoResult()
        funcCls = func
        if not inspect.isclass(funcCls):
            self._assertFuncArgs(func, 1, True)
            def handle(s, work):
                if isinstance(work, Args):
                    results = func(*work.args, **work.kwargs)
                else:
                    results = func(work)
                self._listCall(s.emit, results)

            funcCls = self._newType(func.__name__, job_stream.common.Job,
                    handleWork = handle, useMultiprocessing=useMulti)
        self._stack[-1]['config']['jobs'].append(funcCls)

        # Return original function for multiplicity
        return func


    def reduce(self, func = None, **kwargs):
        """Decorates a reducer.  Reducers are distributed (function does not
        run on only one machine per reduction).  Typically this is used to
        gather and aggregate results.  Any set of work coming into a reducer
        will be emitted as a single piece of work.

        The decorated function should take three arguments:
        store - The storage object on this machine.
        works - A list of work coming into the reducer
        others - A list of storage objects being joined into this reducer from
                other sources.

        Any return value is recurred as work into the reducer.

        Decorator kwargs:
        store - Constructor for the storage element.  Defaults to inline.Object
        emit - Function that takes the store and returns work to be forwarded
                to the next element in the stream.
        """
        if func is None:
            # Invocation with kwargs vs paren-less decoration
            return lambda func2: self.reduce(func2, **kwargs)

        self._assertNoResult()
        reducerCls = func
        if not inspect.isclass(reducerCls):
            self._assertFuncArgs(func, 3)
            storeType = kwargs.pop('store', Object)
            emitValue = kwargs.pop('emit', lambda store: store)
            if kwargs:
                raise KeyError("Unknown kwargs: {}".format(kwargs.keys()))

            def init(s, store):
                store.obj = storeType()
            def add(s, store, w):
                results = func(store.obj, [ w ], [])
                self._listCall(s.recur, results)
            def join(s, store, other):
                results = func(store.obj, [], [ other ])
                self._listCall(s.recur, results)
            def done(s, store):
                self._listCall(s.emit, emitValue(store.obj))
            reducerCls = self._newType(func.__name__,
                    job_stream.common.Reducer, handleInit = init,
                    handleAdd = add, handleJoin = join, handleDone = done)
        self._stack[-1]['config']['reducer'] = reducerCls
        self._stack.pop()
        if not self._stack:
            # We popped off the last.  To allow inline jobs to still be added
            # (post processing), we ensure that we still have a stack
            self._config['jobs'].insert(0, _UnwrapperJob)
            self._initialWorkDepth += 1
            self._config = { 'jobs': [ self._config ] }
            self._stack.append({ 'config': self._config })

        # Return the original function so that the user may call it, if
        # desired
        return func


    def result(self, func = None, **kwargs):
        """Decorates a result handler, which is called only on the primary host
        exactly once for each piece of work that exits the system.  **This is
        the safest place for I/O!**

        The handler receives a single argument, which is the piece of work
        exiting the system.

        If no result handlers are decorated, then inline will use a collector
        that gathers the results and returns them in a list from the run()
        method (run() will return None on machines that are not the primary
        host)."""
        if func is None:
            return lambda func2: self.result(func2, **kwargs)

        self._assertNoResult()
        self._assertFuncArgs(func, 1, True)
        if kwargs:
            raise KeyError("Unknown kwargs: {}".format(kwargs.keys()))

        def realFunc(arg):
            if isinstance(arg, Args):
                return func(*arg.args, **arg.kwargs)
            return func(arg)

        self._resultHandler = realFunc

        # Return original function so it may be referred to as usual
        return func


    def run(self):
        """Runs the Work, executing all decorated methods in the order they
        were specified.  A few kwargs passed to Work() will be forwarded to
        job_stream.common.run().
        """

        runKwargs = dict(self._runKwargs)

        # Hack in a finish() that returns the results, if no finish or results
        # is specified
        result = [ None ]
        if not self._hasFinish and self._resultHandler is None:
            @self.finish
            def returnResults(results):
                return results

        if self._hasFinish:
            if self._resultHandler is not None:
                raise ValueError("finish() and result()?")
            def handleSingleResult(onlyResult):
                if result[0] is not None:
                    raise ValueError("Got multiple results?")
                result[0] = onlyResult
            runKwargs['handleResult'] = handleSingleResult
        else:
            if self._resultHandler is None:
                raise ValueError("No finish() nor result()?")
            runKwargs['handleResult'] = self._resultHandler

        # Run init functions, if it's the first execution of this stream
        isFirst = True

        # Is this a continuation?
        if runKwargs.get('checkpointFile'):
            if os.path.lexists(runKwargs['checkpointFile']):
                isFirst = False

        # Init?
        if isFirst and job_stream.getRank() == 0:
            # Call initial functions
            def addToInitial(w):
                self._initialWork.append(w)
            for init in self._inits:
                # Remember, anything returned from init() adds to our initial
                # work.
                self._listCall(addToInitial, init())

        # Bury our initial work appropriately
        for i in range(self._initialWorkDepth):
            self._initialWork = [ self._initialWork ]
        job_stream.common.work = self._initialWork
        job_stream.common.run(self._config, **runKwargs)
        return result[0]


    def _assertFuncArgs(self, func, i, isMin=False):
        """Ensures that the function func takes exactly i args.  Makes error
        messages more friendly."""
        spec = inspect.getargspec(func)
        numArgs = len(spec.args)
        # For job_stream purposes, varargs or kwargs count as only one
        # additional argument.  What we want to avoid is 'store' getting
        # wrapped up in *args or **kwargs.
        if spec.varargs is not None or spec.keywords is not None:
            numArgs += 1

        if numArgs != i and not isMin or isMin and numArgs < i:
            raise ValueError("Function {} must take {} {} args; takes {}"
                    .format(func, "at least" if isMin else "exactly", i,
                        numArgs))


    def _assertNoResult(self):
        """Ensures that @w.result hasn't been used yet, otherwise raises an
        error."""
        if self._hasFinish:
            raise Exception("After Work.finish is used, no other elements may "
                    "be added to the job stream.")
        if self._resultHandler is not None:
            raise Exception("After Work.result is used, no other elements may "
                    "be added to the job stream.")


    def _listCall(self, boundMethod, results):
        """Calls boundMethod (which is something like Job.emit) for each member of results,
        if results is Multiple.

        Returns True if the boundMethod was called at all (non-None result), False
        otherwise."""
        called = False
        if isinstance(results, Multiple):
            for r in results.workList:
                if r is None:
                    continue
                boundMethod(r)
                called = True
        elif results is None:
            pass
        else:
            boundMethod(results)
            called = True
        return called


    def _newType(self, nameBase, clsBase, **funcs):
        tname = "_{}_{}".format(nameBase, _typeCount[0])
        _typeCount[0] += 1
        useMulti = funcs.pop('useMultiprocessing', self._useMultiprocessing)
        clsAttrs = dict(funcs)
        if not useMulti:
            clsAttrs['USE_MULTIPROCESSING'] = False
        cls = type(tname, (clsBase,), clsAttrs)
        _moduleSelf[tname] = cls
        return cls


def map(func, *sequence, **kwargs):
    """Returns [ func(*a) for a in sequence ] in a parallel manner.  Identical
    to the builtin map(), except parallelized.

    kwargs - Passed to job_stream.inline.Work().

    This implementation differs from job_stream.map() so that it can
    demonstrate the functionality of the inline module.  The behavior is
    identical."""
    arr = list(enumerate(zip(*sequence)))
    result = [ None for _ in range(len(arr)) ]
    with Work(arr, **kwargs) as w:
        @w.job
        def mapWork(w):
            i, arg = w
            return (i, func(*arg))

        @w.result
        def storeResult(w):
            result[w[0]] = w[1]
    return result

