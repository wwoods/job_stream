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
moduleSelf = globals()

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
    def __init__(self, initialWork):
        self._initialWork = initialWork
        # The YAML config that we're building, essentially
        self._config = { 'jobs': [] }
        # parent node for current scope
        self._stack = [ { 'config': self._config } ]


    def frame(self, func = None, **kwargs):
        """Decorates a frame..."""
        if func is None:
            # Non-decorating form
            return lambda func2: self.frame(func2, **kwargs)

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
        """Ends a frame"""
        if func is None:
            # Non-decorating
            return lambda func2: self.frameEnd(func2)

        if 'frameFunc' not in self._stack[-1]:
            raise Exception("frameEnd call does not match up with frame!")

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
        """Decorates a job"""
        if func is None:
            # Invocation, not decoration
            return lambda func2: self.job(func2)

        funcCls = func
        if not inspect.isclass(funcCls):
            def handle(s, work):
                results = func(work)
                self._listCall(s.emit, results)
                    
            funcCls = self._newType(func.__name__, job_stream.Job, handleWork = handle)
        self._stack[-1]['config']['jobs'].append(funcCls)

        # Return original function for multiplicity
        return func


    def reduce(self, func = None, **kwargs):
        """Decorates a reducer"""
        if func is None:
            # Invocation with kwargs vs paren-less decoration
            return lambda func2: self.reduce(func2, **kwargs)

        storeType = kwargs.pop('store', job_stream.Object)
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
            self._initialWork = [ list(self._initialWork) ]
            self._config = { 'jobs': [ self._config ] }
            self._stack.append({ 'config': self._config })

        # Return the original function so that the user may call it, if desired
        return func


    def run(self, **kwargs):
        job_stream.work = self._initialWork
        results = None
        resultsFile = None
        if 'handleResult' not in kwargs:
            if 'checkpointFile' in kwargs:
                resultsFile = kwargs['checkpointFile'] + '.results'
                resultsFileNew = resultsFile + '.new'
                if not os.path.lexists(kwargs['checkpointFile']):
                    # instantiate our results file, no checkpoint
                    with open(resultsFile, 'w') as f:
                        f.write(pickle.dumps([]))

                results = pickle.loads(open(resultsFile, 'r').read())
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
        tname = "{}{}".format(nameBase, _typeCount[0])
        _typeCount[0] += 1
        cls = type(tname, (clsBase,), funcs)
        moduleSelf[tname] = cls
        return cls

