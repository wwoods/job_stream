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

bp::object repr, encodeObj, decodeStr;
PyThreadState* gilThread = 0;

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
        ASSERT(job_stream::python::gilThread != 0, "GIL not unlocked?");
        PyEval_RestoreThread(job_stream::python::gilThread);
        job_stream::python::gilThread = 0;
    }
    ~_PyGilAcquire() {
        ASSERT(job_stream::python::gilThread == 0, "GIL wasn't locked?");
        job_stream::python::gilThread = PyEval_SaveThread();
    }
};


/** Used outside of our main thread to ensure the GIL is by default released for
    job_stream's operations. */
class _PyGilRelease {
public:
    _PyGilRelease() {
        ASSERT(job_stream::python::gilThread == 0, "GIL wasn't locked?");
        job_stream::python::gilThread = PyEval_SaveThread();
    }
    ~_PyGilRelease() {
        ASSERT(job_stream::python::gilThread != 0, "GIL not unlocked?");
        PyEval_RestoreThread(job_stream::python::gilThread);
        job_stream::python::gilThread = 0;
    }
};


SerializedPython pythonToSerialized(const bp::object& o) {
    return SerializedPython(bp::extract<std::string>(
            job_stream::python::encodeObj(o)));
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


class Job : public job_stream::Job<Job, SerializedPython> {
public:
    //TODO - Remove _AutoRegister for this case, so we don't need NAME() or
    //pyHandleWork != 0 in base class.
    static const char* NAME() { return "_pyJobBase"; }

    void pyEmit(bp::object o) {
        this->pyEmit(o, "");
    }

    void pyEmit(bp::object o, const std::string& target) {
        SerializedPython obj = pythonToSerialized(o);

        //Let other threads do stuff while we're emitting
        _PyGilRelease gilLock;
        this->emit(obj, target);
    }

    void handleWork(std::unique_ptr<SerializedPython> work)
            override {
        //Entry point to python!  Reacquire the GIL to deserialize and run our
        //code
        _PyGilAcquire gilLock;
        bp::object workObj = job_stream::python::decodeStr(work->data);
        try {
            this->pyHandleWork(workObj);
        }
        catch (bp::error_already_set&) {
            PyObject* pType, *pValue, *pTraceback;
            PyErr_Fetch(&pType, &pValue, &pTraceback);
            bp::handle<> hType(pType);
            bp::object exType(hType);
            bp::handle<> hTraceback(pTraceback);
            bp::object traceback(hTraceback);

            std::string strError = bp::extract<std::string>(pValue);
            long lineno = bp::extract<long>(traceback.attr("tb_lineno"));
            std::string filename = bp::extract<std::string>(
                    traceback.attr("tb_frame").attr("f_code")
                    .attr("co_filename"));
            std::string funcname = bp::extract<std::string>(
                    traceback.attr("tb_frame").attr("f_code")
                    .attr("co_name"));
            printf("Got error: %s:%s:%u %s\n", filename.c_str(),
                    funcname.c_str(), lineno, strError.c_str());
            throw;
        }
    }

    virtual void pyHandleWork(bp::object work) {
        throw new std::runtime_error("Unimplemented handleWork");
    }

    void setPythonObject(bp::object o) {
        this->_pythonObject = o;
    }

private:
    //Prevent our derivative class from being cleaned up.
    bp::object _pythonObject;
};


class JobExt : public Job {
public:
    JobExt(PyObject* p) : self(p) {}
    JobExt(PyObject* p, const Job& j) : Job(j), self(p) {}
    virtual ~JobExt() {}

    void pyHandleWork(bp::object work) override {
        bp::call_method<void>(this->self, "handleWork", work);
    }

    static void default_pyHandleWork(Job& self_, bp::object work) {
        printf("Uh... pyHandleWork?\n");
        self_.Job::pyHandleWork(work);
    }

private:
    PyObject* self;
};


void registerEncoding(bp::object repr, bp::object encode, bp::object decode) {
    job_stream::python::repr = repr;
    job_stream::python::encodeObj = encode;
    job_stream::python::decodeStr = decode;
}


void registerJob(std::string name, bp::object cls) {
    job_stream::job::addJob(name,
            [cls]() -> Job* {
                _PyGilAcquire allocateInPython;
                bp::object holder = cls();
                Job* r = bp::extract<Job*>(holder);
                r->setPythonObject(holder);
                return r;
            });
}


void runProcessor(const std::string& yamlPath) {
    std::vector<const char*> args;
    args.push_back("job_stream_python");
    args.push_back(yamlPath.c_str());

    _DlOpener holdItOpenGlobally("libmpi.so");
    _PyGilRelease releaser;
    job_stream::runProcessor(args.size(), const_cast<char**>(args.data()));
}


BOOST_PYTHON_MODULE(_job_stream) {
    bp::scope().attr("__doc__") = "C internals for job_stream python library; "
            "see https://github.com/wwoods/job_stream for more info";

    bp::def("registerEncoding", registerEncoding, "Registers the encoding and "
            "decoding functions used by C code.");
    bp::def("registerJob", registerJob, "Registers a job");
    bp::def("runProcessor", runProcessor, "Run the given blah blah");

    void (Job::*emit1)(bp::object) = &Job::pyEmit;
    void (Job::*emit2)(bp::object, const std::string&) = &Job::pyEmit;
    bp::class_<Job, JobExt>("Job", "A basic job")
            .def(bp::init<>())
            .def("emit", emit1, "Emit to only target")
            .def("emit", emit2, "Emit to specific target out of list")
            .def("handleWork", JobExt::default_pyHandleWork)
            ;
}
