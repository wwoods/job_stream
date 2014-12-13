/** Source file for job_stream python extension.  Usage like:

from job_stream import Job, run

class AddOne(Job):
    def handleWork(self, work):
        self.emit(work + 1)

if __name__ == '__main__':
    run()


DEBUG EXECUTION::

LD_LIBRARY_PATH=/u/wwoods/dev/boost_1_55_0/stage/lib/:~/dev/yaml-cpp-0.5.1/build/:/usr/lib/openmpi/lib:/usr/lib YAML_CPP=~/dev/yaml-cpp-0.5.1/ bash -c "python setup.py install --user && python example/runAdder.py"
*/

#include <dlfcn.h>

#include <boost/python.hpp>
#include <job_stream/job_stream.h>

namespace bp = boost::python;

/** So, as per the OpenMPI FAQ, MPI plugins will fail to load if libmpi.so is
    loaded in a local namespace.  From what I can tell, that is what python is
    doing, since those are failing to load.  So, we load the dll manually, and
    publicly. */
class _DlOpener {
public:
    _DlOpener(const char* soName) {
        this->lib = dlopen(soName, RTLD_NOW | RTLD_GLOBAL);
    }
    ~DlOpener() {
        dlclose(this->lib);
    }

private:
    void* lib;
};


struct SerializedPython {
    std::string data;

    SerializedPython() {}
    SerializedPython(std::string src) : data(std::move(src)) {}

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->data;
    }
};


class Job : public job_stream::Job<Job, job_stream::serialization::AnyType> {
public:
    TODO - metaclass trickery (job_stream registration, in place of _AutoRegister)
    TODO - pyHandleWork needs to be implemented in python via override.
    Job() {}

    void emit(bp::object o) {
        this->emit(o, "");
    }

    void emit(bp::object o, const std::string& target) {
        this->emit(SerializedPython(bp::pickle(o)), target);
    }

    void handleWork(std::unique_ptr<job_stream::serialization::AnyType> work)
            override {
        this->pyHandleWork(work->as<SerializedPython>());
    }
};


void registerJob(bp::object cls) {
    TODO get long name of cls, including module(s), excepting __main__
    job_stream::job::addJob(bp::extract<std::string>(cls, "__name__"),
            []() -> { return bp::call(cls); });
}


void runProcessor(const std::string& yamlPath) {
    std::vector<const char*> args;
    args.push_back("job_stream_python");
    args.push_back(yamlPath.c_str());
    Py_BEGIN_ALLOW_THREADS;
    _DlOpener holdItOpenGlobally("libmpi.so");
    job_stream::runProcessor(args.size(), const_cast<char**>(args.data()));
    Py_END_ALLOW_THREADS;
}


BOOST_PYTHON_MODULE(_job_stream) {
    boost::python::scope().attr("__doc__") = "C internals for job_stream python library; "
            "see https://github.com/wwoods/job_stream for more info";

    boost::python::def("registerJob", registerJob, "Registers a derived job class");
    boost::python::def("runProcessor", runProcessor, "Run the given blah blah");

    void (Job::*emit1)(bp::object);
    void (Job::*emit2)(bp::object, const std::string&);
    boost::python::class_<Job>("Job", "A basic job")
            .def(boost::python::init<>())
            .def("emit", emit1, "Emit to only target")
            .def("emit", emit2, "Emit to specific target out of list")
            .def("handleWork", &Job::pyHandleWork)
            ;
}
