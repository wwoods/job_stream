/** Source file for job_stream python extension.  Usage like:

from job_stream import Job, run

class AddOne(Job):
    def handleWork(self, work):
        self.emit(work + 1)

if __name__ == '__main__':
    run()


DEBUG EXECUTION::

LD_LIBRARY_PATH=/u/wwoods/dev/boost_1_55_0/stage/lib/:~/dev/yaml-cpp-0.5.1/build/:/usr/lib/openmpi/lib:/usr/lib YAML_CPP=~/dev/yaml-cpp-0.5.1/ bash -c "python setup.py install --user && python example/runAdder.py 2"
*/


#include <job_stream/debug_internals.h>
#include <job_stream/job_stream.h>
#include <job_stream/pythonType.h>

#include <boost/python.hpp>
#include <dlfcn.h>

namespace bp = boost::python;
using job_stream::python::SerializedPython;

namespace job_stream {
namespace python {

bp::object encodeObj, decodeStr;
bp::object object, repr;

} //python
} //job_stream

/** So, as per the OpenMPI FAQ, MPI plugins will fail to load if libmpi.so is
    loaded in a local namespace.  From what I can tell, that is what python is
    doing, since those are failing to load.  So, we load the dll manually, and
    publicly. */
class _DlOpener {
public:
    _DlOpener(const char* soName) {
        this->lib = dlopen(soName, RTLD_NOW | RTLD_GLOBAL);
    }
    ~_DlOpener() {
        dlclose(this->lib);
    }

private:
    void* lib;
};


/** Used to execute python code within job_stream's operations. */
class _PyGilAcquire {
public:
    _PyGilAcquire() {
        this->gilState = PyGILState_Ensure();
    }
    ~_PyGilAcquire() {
        PyGILState_Release(this->gilState);
    }
private:
    PyGILState_STATE gilState;
};


/** Used outside of our main thread to ensure the GIL is by default released for
    job_stream's operations. */
class _PyGilRelease {
public:
    _PyGilRelease() {
        this->_save = PyEval_SaveThread();
    }
    ~_PyGilRelease() {
        PyEval_RestoreThread(this->_save);
    }
private:
    PyThreadState* _save;
};


/** If an error occurred in python, print its stack trace and information, then
    raise a C++ exception.  To be used in catch blocks around python code. */
#define CHECK_PYTHON_ERROR(m) \
    if (PyErr_Occurred()) { \
        /* Print stack and move error into sys.last_type, sys.last_value, \
           sys.last_traceback.  Also clears the error. */ \
        PyErr_Print(); \
        ERROR("Python exception caught: " << m); \
    }


/** Must be called with GIL locked */
SerializedPython pythonToSerialized(const bp::object& o) {
    return SerializedPython(bp::extract<std::string>(
            job_stream::python::encodeObj(o)));
}


/** Must be called with GIL locked */
bp::object serializedToPython(const SerializedPython& sp) {
    try {
        return job_stream::python::decodeStr(sp.data);
    }
    catch (...) {
        if (PyErr_Occurred()) {
            PyErr_Print();
            throw std::runtime_error("Error deserializing");
        }
        throw;
    }
}


namespace job_stream {
namespace python {
std::istream& operator>>(std::istream& is,
        SerializedPython& sp) {
    //Happens outside the GIL!
    std::string s;
    is >> s;
    _PyGilAcquire gilLock;
    sp.data = bp::extract<std::string>(
            job_stream::python::encodeObj(s));
    return is;
}

std::ostream& operator<<(std::ostream& os,
        const SerializedPython& sp) {
    //Happens outside the GIL!
    _PyGilAcquire gilLock;
    bp::object o = job_stream::python::decodeStr(sp.data);
    std::string s = bp::extract<std::string>(job_stream::python::repr(o));
    return os << s;
}

} //python
} //job_stream

class PyJob;
class PyReducer;

/*********************************************************************/
//Job


/** Since our jobs are technically allocated from Python, but job_stream expects
    jobs to belong to it (and subsequently frees them), we use a shell around
    the python job for interacting with job_stream.
    */
class PyJobShell : public job_stream::Job<PyJobShell, SerializedPython> {
public:
    //TODO - Remove _AutoRegister for this case, so we don't need NAME() or
    //pyHandleWork != 0 in base class.
    static const char* NAME() { return "_pyJobBase"; }

    PyJobShell() : _job(0) {}
    PyJobShell(PyJob* job) : _job(job) {}
    virtual ~PyJobShell();

    void postSetup() override;
    void handleWork(std::unique_ptr<SerializedPython> work) override;

private:
    PyJob* _job;
};


class PyJob {
public:
    PyJob() {}
    virtual ~PyJob() {}


    void pyEmit(bp::object o) {
        this->pyEmit(o, "");
    }


    void pyEmit(bp::object o, const std::string& target) {
        SerializedPython obj = pythonToSerialized(o);

        //Let other threads do stuff while we're emitting
        _PyGilRelease gilLock;
        this->_shell->emit(obj, target);
    }


    void handleWork(std::unique_ptr<SerializedPython> work) {
        //Entry point to python!  Reacquire the GIL to deserialize and run our
        //code
        _PyGilAcquire gilLock;
        try {
            bp::object workObj = job_stream::python::decodeStr(work->data);
            this->pyHandleWork(workObj);
        }
        catch (...) {
            CHECK_PYTHON_ERROR("PyJob::handleWork()");
            throw;
        }
    }


    void postSetup() {
        _PyGilAcquire gilLock;
        try {
            this->pyPostSetup();
        }
        catch (...) {
            CHECK_PYTHON_ERROR("PyJob::postSetup()");
            throw;
        }
    }


    virtual void pyHandleWork(bp::object work) {
        throw std::runtime_error("Python handleWork() not implemented");
    }


    virtual void pyPostSetup() {
        throw std::runtime_error("Python postSetup() not implemented");
    }


    void setPythonObject(bp::object o) {
        this->_pythonObject = o;
    }


    void setShell(PyJobShell* shell) {
        this->_shell = shell;
    }


    void releaseShell() {
        this->_shell = 0;
        _PyGilAcquire runningPyCode;
        //We no longer need to exist, so let the python GC clean up
        this->_pythonObject = bp::object();
    }

private:
    //Prevent our derivative class from being cleaned up.
    bp::object _pythonObject;
    PyJobShell* _shell;
};


class PyJobExt : public PyJob {
public:
    PyJobExt(PyObject* p) : self(p) {}
    PyJobExt(PyObject* p, const PyJob& j) : PyJob(j), self(p) {}
    virtual ~PyJobExt() {}

    void pyHandleWork(bp::object work) override {
        bp::call_method<void>(this->self, "handleWork", work);
    }

    static void default_pyHandleWork(PyJob& self_, bp::object work) {
        self_.PyJob::pyHandleWork(work);
    }

    void pyPostSetup() override {
        bp::call_method<void>(this->self, "postSetup");
    }

    static void default_pyPostSetup(PyJob& self_) {
        self_.PyJob::pyPostSetup();
    }

private:
    PyObject* self;
};


PyJobShell::~PyJobShell() {
    if (this->_job) {
        this->_job->releaseShell();
        this->_job = 0;
    }
}
void PyJobShell::postSetup() {
    this->_job->postSetup();
}
void PyJobShell::handleWork(std::unique_ptr<SerializedPython> work) {
    this->_job->handleWork(std::move(work));
}


/*********************************************************************/
//Reducer


/** Since our reducers are technically allocated from Python, but job_stream expects
    jobs to belong to it (and subsequently frees them), we use a shell around
    the python reducer for interacting with job_stream.
    */
class PyReducerShell : public job_stream::Reducer<PyReducerShell,
        SerializedPython, SerializedPython> {
public:
    //TODO - Remove _AutoRegister for this case, so we don't need NAME() or
    //pyHandleWork != 0 in base class.
    static const char* NAME() { return "_pyReducerBase"; }

    PyReducerShell() : _reducer(0) {}
    PyReducerShell(PyReducer* reducer) : _reducer(reducer) {}
    virtual ~PyReducerShell();

    void postSetup() override;
    void handleInit(SerializedPython& current) override;
    void handleAdd(SerializedPython& current,
            std::unique_ptr<SerializedPython> work) override;
    void handleJoin(SerializedPython& current,
            std::unique_ptr<SerializedPython> other) override;
    void handleDone(SerializedPython& current) override;

private:
    PyReducer* _reducer;
};


class PyReducer {
public:
    PyReducer() {}
    virtual ~PyReducer() {}


    void pyEmit(bp::object o) {
        SerializedPython obj = pythonToSerialized(o);

        //Let other threads do stuff while we're emitting
        _PyGilRelease gilLock;
        this->_shell->emit(obj);
    }


    void pyRecur(bp::object o) {
        this->pyRecur(o, "");
    }


    void pyRecur(bp::object o, const std::string& target) {
        SerializedPython obj = pythonToSerialized(o);

        //Let other threads do stuff while we're recurring
        _PyGilRelease gilLock;
        this->_shell->recur(obj, target);
    }


    void handleAdd(SerializedPython& current,
            std::unique_ptr<SerializedPython> work) {
        //Entry point to python!  Reacquire the GIL to deserialize and run our
        //code
        _PyGilAcquire gilLock;
        try {
            bp::object stash = serializedToPython(current.data);
            bp::object workObj = serializedToPython(work->data);
            this->pyHandleAdd(stash, workObj);
            current = pythonToSerialized(stash);
        }
        catch (...) {
            CHECK_PYTHON_ERROR("PyReducer::handleAdd");
            throw;
        }
    }


    void handleDone(SerializedPython& current) {
        //Entry point to python!  Reacquire the GIL to deserialize and run our
        //code
        _PyGilAcquire gilLock;
        try {
            bp::object stash = serializedToPython(current.data);
            this->pyHandleDone(stash);
            current = pythonToSerialized(stash);
        }
        catch (...) {
            CHECK_PYTHON_ERROR("PyReducer::handleDone");
            throw;
        }
    }


    void handleInit(SerializedPython& current) {
        //Entry point to python!  Reacquire the GIL to deserialize and run our
        //code
        _PyGilAcquire gilLock;
        try {
            //Current is uninitialized; make it a python object()
            bp::object stash = job_stream::python::object();
            this->pyHandleInit(stash);
            current = pythonToSerialized(stash);
        }
        catch (...) {
            CHECK_PYTHON_ERROR("PyReducer::handleInit");
            throw;
        }
    }


    void handleJoin(SerializedPython& current,
            std::unique_ptr<SerializedPython> other) {
        //Entry point to python!  Reacquire the GIL to deserialize and run our
        //code
        _PyGilAcquire gilLock;
        try {
            bp::object stash = serializedToPython(current.data);
            bp::object otter = serializedToPython(other->data);
            this->pyHandleJoin(stash, otter);
            current = pythonToSerialized(stash);
        }
        catch (...) {
            CHECK_PYTHON_ERROR("PyReducer::handleJoin");
            throw;
        }
    }


    void postSetup() {
        _PyGilAcquire gilLock;
        try {
            this->pyPostSetup();
        }
        catch (...) {
            CHECK_PYTHON_ERROR("PyReducer::postSetup");
            throw;
        }
    }


    virtual void pyHandleAdd(bp::object stash, bp::object work) {
        throw std::runtime_error("Python handleAdd() not implemented");
    }


    virtual void pyHandleDone(bp::object stash) {
        throw std::runtime_error("Python handleDone() not implemented");
    }


    virtual void pyHandleInit(bp::object stash) {
        throw std::runtime_error("Python handleInit() not implemented");
    }


    virtual void pyHandleJoin(bp::object stash, bp::object other) {
        throw std::runtime_error("Python handleJoin() not implemented");
    }


    virtual void pyPostSetup() {
        throw std::runtime_error("Python postSetup() not implemented");
    }


    void setPythonObject(bp::object o) {
        this->_pythonObject = o;
    }


    void setShell(PyReducerShell* shell) {
        this->_shell = shell;
    }


    void releaseShell() {
        this->_shell = 0;
        _PyGilAcquire runningPyCode;
        //We no longer need to exist, so let the python GC clean up
        this->_pythonObject = bp::object();
    }

private:
    //Prevent our derivative class from being cleaned up.
    bp::object _pythonObject;
    PyReducerShell* _shell;
};


class PyReducerExt : public PyReducer {
public:
    PyReducerExt(PyObject* p) : self(p) {}
    PyReducerExt(PyObject* p, const PyReducer& j) : PyReducer(j), self(p) {}
    virtual ~PyReducerExt() {}

    void pyHandleAdd(bp::object stash, bp::object work) override {
        bp::call_method<void>(this->self, "handleAdd", stash, work);
    }

    static void default_pyHandleAdd(PyReducer& self_, bp::object stash,
            bp::object work) {
        self_.PyReducer::pyHandleAdd(stash, work);
    }

    void pyHandleDone(bp::object stash) override {
        bp::call_method<void>(this->self, "handleDone", stash);
    }

    static void default_pyHandleDone(PyReducer& self_, bp::object stash) {
        self_.PyReducer::pyHandleDone(stash);
    }

    void pyHandleInit(bp::object stash) override {
        bp::call_method<void>(this->self, "handleInit", stash);
    }

    static void default_pyHandleInit(PyReducer& self_, bp::object stash) {
        self_.PyReducer::pyHandleInit(stash);
    }

    void pyHandleJoin(bp::object stash, bp::object other) override {
        bp::call_method<void>(this->self, "handleJoin", stash, other);
    }

    static void default_pyHandleJoin(PyReducer& self_, bp::object stash,
            bp::object other) {
        self_.PyReducer::pyHandleJoin(stash, other);
    }

    void pyPostSetup() override {
        bp::call_method<void>(this->self, "postSetup");
    }

    static void default_pyPostSetup(PyReducer& self_) {
        self_.PyReducer::pyPostSetup();
    }

private:
    PyObject* self;
};


PyReducerShell::~PyReducerShell() {
    if (this->_reducer) {
        this->_reducer->releaseShell();
        this->_reducer = 0;
    }
}
void PyReducerShell::postSetup() {
    this->_reducer->postSetup();
}
void PyReducerShell::handleInit(SerializedPython& current) {
    this->_reducer->handleInit(current);
}
void PyReducerShell::handleAdd(SerializedPython& current,
        std::unique_ptr<SerializedPython> work) {
    this->_reducer->handleAdd(current, std::move(work));
}
void PyReducerShell::handleJoin(SerializedPython& current,
        std::unique_ptr<SerializedPython> other) {
    this->_reducer->handleJoin(current, std::move(other));
}
void PyReducerShell::handleDone(SerializedPython& current) {
    this->_reducer->handleDone(current);
}


/*********************************************************************/


void registerEncoding(bp::object object, bp::object encode, bp::object decode) {
    job_stream::python::object = object;
    job_stream::python::encodeObj = encode;
    job_stream::python::decodeStr = decode;

    bp::handle<> builtinH(PyEval_GetBuiltins());
    bp::object builtins(builtinH);
    job_stream::python::repr = builtins["repr"];
}


void registerJob(std::string name, bp::object cls) {
    job_stream::job::addJob(name,
            [cls, name]() -> PyJobShell* {
                _PyGilAcquire allocateInPython;
                try {
                    bp::object holder = cls();
                    //Remember, this pointer belongs to python!
                    PyJob* r = bp::extract<PyJob*>(holder);
                    r->setPythonObject(holder);
                    PyJobShell* p = new PyJobShell(r);
                    r->setShell(p);
                    return p;
                }
                catch (...) {
                    CHECK_PYTHON_ERROR("Job allocation: " << name);
                    throw;
                }
            });
}


void registerReducer(std::string name, bp::object cls) {
    job_stream::job::addReducer(name,
            [cls, name]() -> PyReducerShell* {
                _PyGilAcquire allocateInPython;
                try {
                    bp::object holder = cls();
                    //Pointer belongs to python!
                    PyReducer* r = bp::extract<PyReducer*>(holder);
                    r->setPythonObject(holder);
                    PyReducerShell* p = new PyReducerShell(r);
                    r->setShell(p);
                    return p;
                }
                catch (...) {
                    CHECK_PYTHON_ERROR("Reducer allocation: " << name);
                    throw;
                }
            });
}


void runProcessor(const std::string& yamlPath, bp::object workList) {
    std::vector<const char*> args;
    args.push_back("job_stream_python");
    args.push_back(yamlPath.c_str());

    for (int i = 0, m = bp::len(workList); i < m; i++) {
        job_stream::queueInitialWork(SerializedPython(bp::extract<std::string>(
                job_stream::python::encodeObj(boost::python::object(
                    workList[i])))));
    }

    job_stream::processor::externalControlCode = []() -> void {
    };

    PyEval_InitThreads();
    _DlOpener holdItOpenGlobally("libmpi.so");
    {
        _PyGilRelease releaser;
        job_stream::runProcessor(args.size(), const_cast<char**>(args.data()));
    }
}


BOOST_PYTHON_MODULE(_job_stream) {
    bp::scope().attr("__doc__") = "C internals for job_stream python library; "
            "see https://github.com/wwoods/job_stream for more info";

    bp::def("registerEncoding", registerEncoding, "Registers the encoding and "
            "decoding functions used by C code.");
    bp::def("registerJob", registerJob, "Registers a job");
    bp::def("registerReducer", registerReducer, "Registers a reducer");
    bp::def("runProcessor", runProcessor, "Run the given blah blah");

    { //PyJob
        void (PyJob::*emit1)(bp::object) = &PyJob::pyEmit;
        void (PyJob::*emit2)(bp::object, const std::string&) = &PyJob::pyEmit;
        bp::class_<PyJob, PyJobExt>("Job", "A basic job")
                .def(bp::init<>())
                .def("emit", emit1, "Emit to only target")
                .def("emit", emit2, "Emit to specific target out of list")
                .def("handleWork", PyJobExt::default_pyHandleWork)
                .def("postSetup", PyJobExt::default_pyPostSetup)
                ;
    }

    { //PyReducer
        void (PyReducer::*emit1)(bp::object) = &PyReducer::pyEmit;
        void (PyReducer::*recur1)(bp::object) = &PyReducer::pyRecur;
        void (PyReducer::*recur2)(bp::object, const std::string&) = &PyReducer::pyRecur;
        bp::class_<PyReducer, PyReducerExt>("Reducer", "A basic reducer")
                .def(bp::init<>())
                .def("emit", emit1, "Emit to only target (outside of reducer)")
                .def("recur", recur1, "Recur to only target")
                .def("recur", recur2, "Recur to specific target")
                .def("handleAdd", PyReducerExt::default_pyHandleAdd)
                .def("handleDone", PyReducerExt::default_pyHandleDone)
                .def("handleInit", PyReducerExt::default_pyHandleInit)
                .def("handleJoin", PyReducerExt::default_pyHandleJoin)
                .def("postSetup", PyReducerExt::default_pyPostSetup)
                ;
    }
}
