"""job_stream.inline provides a more intuitive way of specifying flow that relieves some
of the labor of thinking about data definitions

For example, to count the average word length in a string:

.. code-block:: python

    from job_stream import inline

    w = inline.Work("Hello there world, how are you?".split())

    @w.job
    def getLen(w):
        return len(w)

    @w.reduce(emit = lambda store: store.len / store.count)
    def findAverage(store, inputs, others):
        if not hasattr(store, 'init'):
            store.init = True
            store.count = 0
            store.len = 0.0

        for i in inputs:
            store.len += i
            store.count += 1
        for o in others:
            store.len += o.len
            store.count += o.count

    result, = w.run()

Members
=======

"""

import job_stream
import pickle
import inspect
import os

# Imports from job_stream
Object = job_stream.Object

# Used to register new classes so they can be pickle'd appropriately
_moduleSelf = globals()

_typeCount = [ 0 ]

class _ForceCheckpointJob(job_stream.Job):
    """Used for tests; raises exception after checkpoint."""
    def handleWork(self, work):
        self._forceCheckpoint(True)
        self.emit(work)



class _ForceCheckpointAndGoJob(job_stream.Job):
    """Used for tests; execution continues after checkpoint."""
    def handleWork(self, work):
        self._forceCheckpoint(False)
        self.emit(work)



class _UnwrapperJob(job_stream.Job):
    """Job to make processing after a reducer possible and sensical for inline.
    """
    USE_MULTIPROCESSING = False

    def handleWork(self, work):
        for w in work:
            self.emit(w)


class Multiple(object):
    """If some inline code wants to emit or recur multiple pieces of work, an instance of
    this class is the way to do it.  For instance:

    return Multiple([ 1, 2, 3 ])
    """

    def __init__(self, workList):
        self.workList = workList


class Work(object):
    def __init__(self, initialWork = [], useMultiprocessing = True,
            **runKwargs):
        """Represents a job_stream pipeline.  May be passed a list or other
        iterable of initial work, as well as any of the following flags:

        useMultiprocessing [True] - If True, then all job_stream work will be
            handled in child processes spawned via Python's multiprocessing
            module.  This is used to get around the limitations of the GIL,
            but if your application's setup (not the jobs themselves) uses a lot
            of memory and your computation is handled by either non-GIL
            protected code or in another process anyway, it is appropriate to
            turn this off.

        checkpointFile [str] - If specified, job_stream will run in checkpoint
            mode, periodically dumping progress to this file.  If this file
            exists when run() is called, then simulation will be resumed
            from the state stored in this file.

        checkpointInterval [float] - Time, in seconds, between the completion
            of one checkpoint and the beginning of the next.

        checkpointSyncInterval [float] - Debug / test usage only.  Time, in
            seconds, to wait for sync when taking a checkpoint.
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
        itself until some stopping condition.  The decorated function should
        accept two arguments:

        store - The storage object used to remember results from the frame
        first - The first work that began this frame.

        Any results returned are recursed into the frame.

        Decorator kwargs:
        store - The constructor for a storage object.  Defaults to inline.Object
        emit - A function taking a storage object and returning the work that
                should be forwarded to the next member of the stream.  Defaults
                to emitting the store itself."""
        if func is None:
            # Non-decorating form
            return lambda func2: self.frame(func2, **kwargs)

        self._assertNoResult()
        self._assertFuncArgs(func, 2)
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

        store - The storage object used to remember results from the frame (same
                as in the frame() decorated method).
        next - The next result object that ran through the frame.

        Any results returned are recursed into the frame."""
        if func is None:
            # Non-decorating
            return lambda func2: self.frameEnd(func2)

        if 'frameFunc' not in self._stack[-1]:
            raise Exception("frameEnd call does not match up with frame!")

        self._assertNoResult()
        self._assertFuncArgs(func, 2)
        fargs = self._stack[-1]['frameArgs']
        funcStart = self._stack[-1]['frameFunc']
        # Note that the inline version skips straight from handleFirst to handleDone, then
        # handleNext based on recurrence.
        def handleFirst(s, store, first):
            store.first = first
            store.obj = fargs.store()
        def handleNext(s, store, next):
            results = func(store.obj, next)
            self._listCall(s.recur, results)
        def handleDone(s, store):
            results = funcStart(store.obj, store.first)
            hadRecur = self._listCall(s.recur, results)
            if not hadRecur:
                # Frame is done, so emit our results
                self._listCall(s.emit, fargs.emit(store.obj))

        frame = self._newType(self._stack[-1]['frameFunc'].__name__,
                job_stream.Frame, handleFirst = handleFirst, handleNext = handleNext,
                handleDone = handleDone, useMultiprocessing = fargs.useMulti)
        self._stack[-1]['config']['frame'] = frame
        self._stack.pop()
        # Since we always make a stack entry to start a frame, should never be empty here
        assert self._stack

        return func


    def job(self, func = None):
        """Decorates a job.  The decorated function must take one argument,
        which is the work coming into the job.  Anything returned is passed
        along to the next member of the stream."""
        if func is None:
            # Invocation, not decoration
            return lambda func2: self.job(func2)

        self._assertNoResult()
        funcCls = func
        if not inspect.isclass(funcCls):
            self._assertFuncArgs(func, 1)
            def handle(s, work):
                results = func(work)
                self._listCall(s.emit, results)

            funcCls = self._newType(func.__name__, job_stream.Job, handleWork = handle)
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
                    job_stream.Reducer, handleInit = init, handleAdd = add,
                    handleJoin = join, handleDone = done)
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
        exactly once for each piece of work that exits the system.  The handler
        receives a single argument, which is the piece of work exiting the
        system.

        If no result handlers are decorated, then inline will use a collector
        that gathers the results and returns them in a list from the run()
        method (run() will return None on machines that are not the primary
        host)."""
        if func is None:
            return lambda func2: self.result(func2, **kwargs)

        self._assertNoResult()
        self._assertFuncArgs(func, 1)
        if kwargs:
            raise KeyError("Unknown kwargs: {}".format(kwargs.keys()))

        self._resultHandler = func

        # Return original function so it may be referred to as usual
        return func


    def run(self):
        """Runs the Work, executing all decorated methods in the order they
        were specified.  A few kwargs passed to Work() will be forwarded to
        job_stream.run().
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
        job_stream.work = self._initialWork
        job_stream.run(self._config, **runKwargs)
        return result[0]


    def _assertFuncArgs(self, func, i):
        """Ensures that the function func takes exactly i args.  Makes error
        messages more friendly."""
        numArgs = len(inspect.getargspec(func).args)
        if numArgs != i:
            raise ValueError("Function {} must take exactly {} args; takes {}"
                    .format(func, i, numArgs))


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

