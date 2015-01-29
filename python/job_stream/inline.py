"""job_stream.inline provides a more intuitive way of specifying flow that relieves some
of the labor of thinking about data definitions

For example, to count the average word length in a string:

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

"""

import job_stream
import cPickle as pickle
import inspect
import os

Object = job_stream.Object
_moduleSelf = globals()

_typeCount = [ 0 ]

class _ForceCheckpointJob(job_stream.Job):
    """Used for tests"""
    def handleWork(self, work):
        self._forceCheckpoint(True)
        self.emit(work)



class _UnwrapperJob(job_stream.Job):
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
    def __init__(self, initialWork = []):
        self._initialWork = list(initialWork)
        self._initialWorkDepth = 0
        # The YAML config that we're building, essentially
        self._config = { 'jobs': [] }
        # Functions ran on init
        self._inits = []
        # Function ran to handle a result.  If set, nothing else may be added
        # to the pipe!
        self._resultHandler = None
        # parent node for current scope
        self._stack = [ { 'config': self._config } ]


    def init(self, func = None):
        """Decorates a method that is called only once, including over multiple
        runs with checkpoints.  Useful for e.g. creating folders, deleting
        temporary files from previous runs, etc."""
        if func is None:
            return lambda func2: self.init(func2)

        self._assertNoResult()
        self._assertFuncArgs(func, 0)

        self._inits.append(func)
        return func


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
                handleDone = handleDone)
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
        reducer = self._newType(func.__name__,
                job_stream.Reducer, handleInit = init, handleAdd = add,
                handleJoin = join, handleDone = done)
        self._stack[-1]['config']['reducer'] = reducer
        self._stack.pop()
        if not self._stack:
            # We popped off the last.  To allow inline jobs to still be added (post
            # processing), we ensure that we still have a stack
            self._config['jobs'].insert(0, _UnwrapperJob)
            self._initialWorkDepth += 1
            self._config = { 'jobs': [ self._config ] }
            self._stack.append({ 'config': self._config })

        # Return the original function so that the user may call it, if desired
        return func


    def result(self, func = None, **kwargs):
        """Decorates a result handler, which is called only on the primary host
        exactly once for each piece of work that exits the system.

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


    def run(self, **kwargs):
        """Runs the Work, executing all decorated methods in the order they
        were specified.

        kwargs: Accepts the same arguments as job_stream.run, except for
        handleResult.  See Work.result for handleResult's replacement.
        """
        results = None
        resultsFile = None
        isFirst = True

        # Is this a continuation?
        if 'checkpointFile' in kwargs:
            if os.path.lexists(kwargs['checkpointFile']):
                isFirst = False

        if self._resultHandler is not None:
            kwargs['handleResult'] = self._resultHandler
        else:
            # Default result handling - gather and return array
            if job_stream.getRank() != 0:
                # Keep results at None, and leave the primary host as
                # responsible
                def raiser(r):
                    raise NotImplementedError("Should never be called")
                kwargs['handleResult'] = raiser
            else:
                if 'checkpointFile' in kwargs:
                    resultsFile = kwargs['checkpointFile'] + '.results'
                    resultsFileNew = resultsFile + '.new'

                    if os.path.lexists(resultsFile):
                        results = pickle.loads(open(resultsFile, 'r').read())
                    else:
                        results = []
                    def handleResult(result):
                        results.append(result)
                        with open(resultsFileNew, 'w') as f:
                            f.write(pickle.dumps(results))
                        os.rename(resultsFileNew, resultsFile)
                else:
                    results = []
                    def handleResult(result):
                        results.append(result)

                kwargs['handleResult'] = handleResult

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
        job_stream.run(self._config, **kwargs)

        # OK, delete checkpoints if any
        if resultsFile is not None:
            try:
                os.remove(resultsFile)
            except OSError, e:
                if e.errno != 2:
                    raise
            try:
                os.remove(resultsFileNew)
            except OSError, e:
                if e.errno != 2:
                    raise

        return results


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
        cls = type(tname, (clsBase,), funcs)
        _moduleSelf[tname] = cls
        return cls
