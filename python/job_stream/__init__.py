"""job_stream's python module implementation.  Example usage:

class AddOne(job_stream.Job):
    def handleWork(self, work):
        self.emit(work + 1)

job_stream.work.append(8)
job_stream.work.append(9)
job_stream.run({
    'jobs': [
        { 'type': AddOne }
    ]
})
# 9 and 10 will have been printed.
"""

import _job_stream as _j

import cPickle as pickle
import multiprocessing
import traceback

# Classes waiting for _patchForMultiprocessing.  We wait until _pool is initiated
# so that A) classes inheriting from one another are rewritten backwards so that they
# execute the original method, not the override, and B) so that their methods may be 
# updated between class definition and job_stream.run()
_classesToPatch = []
_pool = [ None ]
def _initMultiprocessingPool():
    """The multiprocessing pool is initialized lazily by default, to avoid overhead
    if no jobs are using multiprocessing"""
    if _pool[0] is None:
        _pool[0] = 'Do not re-init in multiprocessed pool'
        _pool[0] = multiprocessing.Pool()


def _decode(s):
    """Decodes an object with cPickle"""
    return pickle.loads(s)


def _encode(o):
    """Encodes an object with cPickle"""
    return pickle.dumps(o)


class Object(object):
    """A generic object with no attributes of its own."""


# Initialize the encode and decode values first so that they can be used in
# debug code (if left uninitialized, any attempt to pickle something from within
# C++ code will crash with NoneType cannot be called)
_j.registerEncoding(Object, _encode, _decode)


class _Work(list):
    """List of initial work sent into job_stream.
    If left empty, work comes from stdin."""
work = _Work()

_localJobs = {}
_localJobId = [ 0 ]
def _localJobInit(obj):
    _localJobs[obj.id] = obj
    if hasattr(obj, 'emit'):
        obj.emit = lambda *args: obj.emitters.append(args)
    if hasattr(obj, 'recur'):
        obj.recur = lambda *args: obj.recurs.append(args)
    obj._forceCheckpoint = lambda *args: obj.forceCheckpoints.append(args)
    def obj_reset():
        obj.emitters = []
        obj.recurs = []
        obj.forceCheckpoints = []
    obj._resetLocalJob = obj_reset

    # Now call postSetup.  Note that since we haven't called reset() yet, none of the
    # arrays exist and so emit(), recur(), and _forceCheckpoint() will all crash
    try:
        obj.mPostSetup()
    except:
        traceback.print_exc()
        raise Exception("Multiprocessing error raised")
def _localCallNoStore(obj, method, *args):
    if obj not in _localJobs:
        return (0, [], [], [])
    o = _localJobs[obj]
    o._resetLocalJob()
    try:
        getattr(o, method)(*args)
    except:
        traceback.print_exc()
        raise Exception("Multiprocessing error raised")
    return (1, o.emitters, o.recurs, o.forceCheckpoints)
def _localCallStoreFirst(obj, method, first, *args):
    if obj not in _localJobs:
        return (0, None, [], [], [])
    o = _localJobs[obj]
    o._resetLocalJob()
    try:
        getattr(o, method)(first, *args)
    except:
        traceback.print_exc()
        raise Exception("Multiprocessing error raised")
    return (1, first, o.emitters, o.recurs, o.forceCheckpoints)


def callNoStore(obj, method, *args):
    while True:
        r = _pool[0].apply(_localCallNoStore, args = (obj.id, method) + args)
        if r[0] == 0:
            _pool[0].apply(_localJobInit, args = (obj,))
        else:
            break
    for eArgs in r[1]:
        obj.emit(*eArgs)
    for rArgs in r[2]:
        obj.recur(*rArgs)
    for fArgs in r[3]:
        obj._forceCheckpoint(*fArgs)


def callStoreFirst(obj, method, first, *args):
    while True:
        r = _pool[0].apply(_localCallStoreFirst,
                args = (obj.id, method, first) + args)
        if r[0] == 0:
            _pool[0].apply(_localJobInit, args = (obj,))
        else:
            break
    first.__dict__ = r[1].__dict__
    for eArgs in r[2]:
        obj.emit(*eArgs)
    for rArgs in r[3]:
        obj.recur(*rArgs)
    for fArgs in r[4]:
        obj._forceCheckpoint(*fArgs)


class Job(_j.Job):
    """Base class for a standard job (starts with some work, and emits zero or
    more times).  Handles registration of job class with the job_stream
    system."""
    class __metaclass__(type(_j.Job)):
        def __init__(cls, name, bases, attrs):
            type(_j.Job).__init__(cls, name, bases, attrs)

            # Derived hierarchical name, use that in config
            fullname = cls.__module__
            if fullname == '__main__':
                fullname = name
            else:
                fullname += '.' + name

            # Metaclasses are called for their first rendition as well, so...
            if fullname == 'job_stream.Job':
                return

            _classesToPatch.append(cls)
            _j.registerJob(fullname, cls)


    USE_MULTIPROCESSING = True
    USE_MULTIPROCESSING_doc = """If True [default {}], job_stream automatically handles 
        overloading the class' methods and serializing everything so that the GIL is
        circumvented.  While this defaults to True as it is low overhead, lots of jobs
        do not need multiprocessing if they are using other python libraries or operations
        that release the GIL.""".format(USE_MULTIPROCESSING)


    @classmethod
    def _patchForMultiprocessing(cls):
        def newInit(self):
            super(cls, self).__init__()
            _initMultiprocessingPool()
            self.id = _localJobId[0]
            _localJobId[0] += 1
        cls.__init__ = newInit

        cls.mHandleWork = cls.handleWork
        cls.handleWork = lambda self, *args: callNoStore(self, "mHandleWork",
                *args)

        # We do not call postSetup when job_stream requests it.  This is because
        # our jobs must be set up in each thread, so we defer until it is called
        # in a thread.
        cls.mPostSetup = cls.postSetup
        cls.postSetup = lambda self: True


    def postSetup(self):
        """Called when self.config is set and the Job is fully ready for work,
        but before any work is accepted."""
        pass


    def handleWork(self, work):
        """Handle incoming work, maybe call self.emit() to generate more work
        for jobs further down the pipe."""
        raise NotImplementedError()



class Reducer(_j.Reducer):
    """Base class for a Reducer (starts with...
    TODO
    """
    class __metaclass__(type(_j.Reducer)):
        def __init__(cls, name, bases, attrs):
            type(_j.Reducer).__init__(cls, name, bases, attrs)

            # Derived hierarchical name, use that in config
            fullname = cls.__module__
            if fullname == '__main__':
                fullname = name
            else:
                fullname += '.' + name

            # Metaclasses are called for their first rendition as well, so...
            if fullname == 'job_stream.Reducer':
                return

            _j.registerReducer(fullname, cls)


    USE_MULTIPROCESSING = Job.USE_MULTIPROCESSING
    USE_MULTIPROCESSING_doc = Job.USE_MULTIPROCESSING_doc


    @classmethod
    def _patchForMultiprocessing(cls):
        def newInit(self):
            super(cls, self).__init__()
            _initMultiprocessingPool()
            self.id = _localJobId[0]
            _localJobId[0] += 1
        cls.__init__ = newInit

        for oldName in [ 'handleInit', 'handleAdd', 'handleJoin', 'handleDone' ]:
            newName = 'm' + oldName[0].upper() + oldName[1:]
            setattr(cls, newName, getattr(cls, oldName))
            closure = lambda newName: lambda self, *args: callStoreFirst(self,
                    newName, *args)
            setattr(cls, oldName, closure(newName))

        # We do not call postSetup when job_stream requests it.  This is because
        # our jobs must be set up in each thread, so we defer until it is called
        # in a thread.
        cls.mPostSetup = cls.postSetup
        cls.postSetup = lambda self: True


    def postSetup(self):
        """Called when self.config is set and the Job is fully ready for work,
        but before any work is accepted."""
        pass


    def handleInit(self, store):
        """Called when a reduction is started.  Store is a python object that
        should be modified to remember information between invocations."""


    def handleAdd(self, store, work):
        """Called when new work arrives at the Reducer."""
        raise NotImplementedError()


    def handleJoin(self, store, other):
        """Called to merge two stores from the same Reducer."""
        raise NotImplementedError()


    def handleDone(self, store):
        """Called when the reduction is finished.  The reduction will be marked
        as unfinished if a recur() happens."""
        raise NotImplementedError()



class Frame(_j.Frame):
    """Base class for a Frame
    TODO
    """
    class __metaclass__(type(_j.Frame)):
        def __init__(cls, name, bases, attrs):
            # Derived hierarchical name, use that in config
            fullname = cls.__module__
            if fullname == '__main__':
                fullname = name
            else:
                fullname += '.' + name

            # Metaclasses are called for their first rendition as well, so...
            if fullname == 'job_stream.Frame':
                return

            _classesToPatch.append(cls)
            _j.registerFrame(fullname, cls)


    USE_MULTIPROCESSING = Job.USE_MULTIPROCESSING
    USE_MULTIPROCESSING_doc = Job.USE_MULTIPROCESSING_doc


    @classmethod
    def _patchForMultiprocessing(cls):
        def newInit(self):
            super(cls, self).__init__()
            _initMultiprocessingPool()
            self.id = _localJobId[0]
            _localJobId[0] += 1
        cls.__init__ = newInit

        for oldName in [ 'handleFirst', 'handleNext', 'handleDone' ]:
            newName = 'm' + oldName[0].upper() + oldName[1:]
            setattr(cls, newName, getattr(cls, oldName))
            closure = lambda newName: lambda self, *args: callStoreFirst(self,
                    newName, *args)
            setattr(cls, oldName, closure(newName))

        # We do not call postSetup when job_stream requests it.  This is because
        # our jobs must be set up in each thread, so we defer until it is called
        # in a thread.
        cls.mPostSetup = cls.postSetup
        cls.postSetup = lambda self: True


    def handleFirst(self, store, work):
        """Called for the first work, which starts a reduction.  Store is an
        empty Object() to which this method may assign attributes."""
        raise NotImplementedError()


    def handleNext(self, store, work):
        """Called when finished work arrives at the Frame."""
        raise NotImplementedError()


    def handleDone(self, store):
        """Called when the reduction is finished.  The reduction will be marked
        as unfinished if a recur() happens."""
        raise NotImplementedError()


    def postSetup(self):
        """Called when self.config is set and the Frame is fully ready for work,
        but before any work is accepted."""


def _convertDictToYaml(c):
    levels = [ { 'type': dict, 'vals': c.iteritems() } ]
    result = []
    def cueLine():
        result.append("  " * (len(levels) - 1))
    while levels:
        try:
            val = levels[-1]['vals'].next()
        except StopIteration:
            levels.pop()
            continue

        cueLine()
        if levels[-1]['type'] == dict:
            key, dval = val
            result.append(key)
            result.append(": ")
            val = dval
        elif levels[-1]['type'] == list:
            result.append("- ")

        # Now, the value part
        if isinstance(val, dict):
            levels.append({ 'type': dict, 'vals': val.iteritems() })
        elif isinstance(val, list):
            levels.append({ 'type': list, 'vals': iter(val) })
        elif isinstance(val, (int, float, basestring)):
            result.append(str(val))
        elif issubclass(val, (Frame, Job, Reducer)):
            result.append(val.__name__)
        else:
            raise ValueError("Unrecognized YAML object: {}: {}".format(key,
                    val))

        result.append("\n")

    result = ''.join(result)
    return result


def run(configDictOrPath, **kwargs):
    """Runs the given YAML file or config dictionary.

    Acceptable kwargs:
        checkpointFile - (string) The file to use for checkpoint / restore
        checkpointInterval - (float) The time between the completion of one
                checkpoint and the starting of the next, in seconds.
        checkpointSyncInterval - (float) The time between all processors
                thinking they're ready to checkpoint and the actual checkpoint.
    """
    if isinstance(configDictOrPath, basestring):
        # Path to file
        config = open(configDictOrPath).read()
    elif isinstance(configDictOrPath, dict):
        config = _convertDictToYaml(configDictOrPath)
    else:
        raise ValueError("configDictOrPath was not dict or filename!")

    for cls in reversed(_classesToPatch):
        if cls.USE_MULTIPROCESSING:
            cls._patchForMultiprocessing()

    _j.runProcessor(config, work, **kwargs)
