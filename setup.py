# Python module configuration file, used to build job_stream python module,
# which can be used if you want to use python for your tasks.  Because let's
# face it, Python is nicer than C/C++.

from distutils.core import setup, Extension
import os
import re
import subprocess
import sys

# Copy documentation into restructured text (pypi's choice)
try:
    import pandoc
    pandoc.core.PANDOC_PATH = '/path/to/pandoc/todo'
    doc = pandoc.Document()
    doc.markdown = open('README.md').read()
    long_desc = doc.rst
except:
    long_desc = open('README.md').read()

# Extra directories coming from non-system paths.
incdirs = [ '.', 'yaml-cpp-0.5.1/include',
        'boost-mpi-1.61/include', 'boost-serialization-1.61/include',
        'boost-serialization-1.61/src' ]
libdirs = []
libraries = []
linkerExtras = []
if 'LD_LIBRARY_PATH' in os.environ:
    libdirs.extend(os.environ['LD_LIBRARY_PATH'].split(':'))

# Find MPI flags
mpiCompiler = 'mpicxx'
if 'MPICXX' in os.environ:
    mpiCompiler = os.environ['MPICXX']
def checkedRun(args):
    try:
        p = subprocess.Popen(args, stdout = subprocess.PIPE,
                stderr = subprocess.PIPE)
    except OSError as e:
        if e.errno != 2:
            raise
        raise Exception("Error executing program: {}".format(args))
    stdout, stderr = p.communicate()
    r = p.wait()
    if r != 0:
        raise Exception("Program failed with {}: {}".format(r, args))
    return stdout.decode(sys.stdout.encoding or 'utf-8').strip()
mpiFlagsCompile = checkedRun([ mpiCompiler, "--showme:compile" ])
mpiFlagsLink = checkedRun([ mpiCompiler, "--showme:link" ])

for r in re.finditer(r'-I([^\s]+)', mpiFlagsCompile):
    incdirs.append(r.group(1))
for r in re.finditer(r'-L([^\s]+)', mpiFlagsLink):
    libdirs.append(r.group(1))
for r in re.finditer(r'-l([^\s]+)', mpiFlagsLink):
    libraries.append(r.group(1))
for r in re.finditer(r'-Wl([^\s]+)', mpiFlagsLink):
    linkerExtras.append(r.group(0))

# Note that for e.g. conda, where boost has been installed local to the python
# interpreter, we also want the include directory related to our python
# executable.
exeIncludes = os.path.abspath(os.path.join(
        os.path.dirname(sys.executable), "../include"))
exeLibs = os.path.abspath(os.path.join(
        os.path.dirname(sys.executable), "../lib"))
if os.path.lexists(exeIncludes):
    # Everything before is job_stream specific
    incdirs.append(exeIncludes)
if os.path.lexists(exeLibs):
    libdirs.append(exeLibs)

job_stream = Extension('_job_stream',
        define_macros = [ ('PYTHON_MAJOR', sys.version_info.major) ],
        include_dirs = incdirs + [ '/usr/local/include' ],
        libraries = libraries + [ 'boost_filesystem', 'boost_python',
            'boost_system', 'boost_thread', 'boost_serialization', 'dl'
        ],
        library_dirs = libdirs,
        sources = [
            'python/_job_stream.cpp',
            'job_stream/invoke.cpp',
            'job_stream/job_stream.cpp',
            'job_stream/job.cpp',
            'job_stream/message.cpp',
            'job_stream/module.cpp',
            'job_stream/processor.cpp',
            'job_stream/serialization.cpp',
            'job_stream/types.cpp',
            'job_stream/workerThread.cpp',
            'job_stream/death_handler/death_handler.cc',

            # boost.mpi 1.61, embedded into job_stream for compilation ease
            # especially with anaconda (conda install boost works with
            # job_stream once mpi is taken care of)
            'boost-mpi-1.61/src/broadcast.cpp',
            'boost-mpi-1.61/src/communicator.cpp',
            'boost-mpi-1.61/src/computation_tree.cpp',
            'boost-mpi-1.61/src/content_oarchive.cpp',
            'boost-mpi-1.61/src/environment.cpp',
            'boost-mpi-1.61/src/exception.cpp',
            'boost-mpi-1.61/src/graph_communicator.cpp',
            'boost-mpi-1.61/src/group.cpp',
            'boost-mpi-1.61/src/intercommunicator.cpp',
            'boost-mpi-1.61/src/mpi_datatype_cache.cpp',
            'boost-mpi-1.61/src/mpi_datatype_oarchive.cpp',
            'boost-mpi-1.61/src/packed_iarchive.cpp',
            'boost-mpi-1.61/src/packed_oarchive.cpp',
            'boost-mpi-1.61/src/packed_skeleton_iarchive.cpp',
            'boost-mpi-1.61/src/packed_skeleton_oarchive.cpp',
            'boost-mpi-1.61/src/point_to_point.cpp',
            'boost-mpi-1.61/src/request.cpp',
            'boost-mpi-1.61/src/text_skeleton_oarchive.cpp',
            'boost-mpi-1.61/src/timer.cpp',

            # boost.serialization 1.61, again embedded for compilation ease
            'boost-serialization-1.61/src/basic_archive.cpp',
            'boost-serialization-1.61/src/basic_iarchive.cpp',
            'boost-serialization-1.61/src/basic_iserializer.cpp',
            'boost-serialization-1.61/src/basic_oarchive.cpp',
            'boost-serialization-1.61/src/basic_oserializer.cpp',
            'boost-serialization-1.61/src/basic_pointer_iserializer.cpp',
            'boost-serialization-1.61/src/basic_pointer_oserializer.cpp',
            'boost-serialization-1.61/src/basic_serializer_map.cpp',
            'boost-serialization-1.61/src/basic_text_iprimitive.cpp',
            'boost-serialization-1.61/src/basic_text_oprimitive.cpp',
            'boost-serialization-1.61/src/basic_xml_archive.cpp',
            'boost-serialization-1.61/src/binary_iarchive.cpp',
            'boost-serialization-1.61/src/binary_oarchive.cpp',
            'boost-serialization-1.61/src/extended_type_info.cpp',
            'boost-serialization-1.61/src/extended_type_info_typeid.cpp',
            'boost-serialization-1.61/src/extended_type_info_no_rtti.cpp',
            'boost-serialization-1.61/src/polymorphic_iarchive.cpp',
            'boost-serialization-1.61/src/polymorphic_oarchive.cpp',
            'boost-serialization-1.61/src/stl_port.cpp',
            'boost-serialization-1.61/src/text_iarchive.cpp',
            'boost-serialization-1.61/src/text_oarchive.cpp',
            'boost-serialization-1.61/src/void_cast.cpp',
            'boost-serialization-1.61/src/archive_exception.cpp',
            'boost-serialization-1.61/src/xml_grammar.cpp',
            'boost-serialization-1.61/src/xml_iarchive.cpp',
            'boost-serialization-1.61/src/xml_oarchive.cpp',
            'boost-serialization-1.61/src/xml_archive_exception.cpp',
            'boost-serialization-1.61/src/codecvt_null.cpp',
            'boost-serialization-1.61/src/utf8_codecvt_facet.cpp',
            'boost-serialization-1.61/src/singleton.cpp',

            # yaml-cpp 0.5.1, embedded into job_stream for compilation ease
            'yaml-cpp-0.5.1/src/binary.cpp',
            'yaml-cpp-0.5.1/src/convert.cpp',
            'yaml-cpp-0.5.1/src/directives.cpp',
            'yaml-cpp-0.5.1/src/emit.cpp',
            'yaml-cpp-0.5.1/src/emitfromevents.cpp',
            'yaml-cpp-0.5.1/src/emitter.cpp',
            'yaml-cpp-0.5.1/src/emitterstate.cpp',
            'yaml-cpp-0.5.1/src/emitterutils.cpp',
            'yaml-cpp-0.5.1/src/exp.cpp',
            'yaml-cpp-0.5.1/src/memory.cpp',
            'yaml-cpp-0.5.1/src/nodebuilder.cpp',
            'yaml-cpp-0.5.1/src/node.cpp',
            'yaml-cpp-0.5.1/src/node_data.cpp',
            'yaml-cpp-0.5.1/src/nodeevents.cpp',
            'yaml-cpp-0.5.1/src/null.cpp',
            'yaml-cpp-0.5.1/src/ostream_wrapper.cpp',
            'yaml-cpp-0.5.1/src/parse.cpp',
            'yaml-cpp-0.5.1/src/parser.cpp',
            'yaml-cpp-0.5.1/src/regex.cpp',
            'yaml-cpp-0.5.1/src/scanner.cpp',
            'yaml-cpp-0.5.1/src/scanscalar.cpp',
            'yaml-cpp-0.5.1/src/scantag.cpp',
            'yaml-cpp-0.5.1/src/scantoken.cpp',
            'yaml-cpp-0.5.1/src/simplekey.cpp',
            'yaml-cpp-0.5.1/src/singledocparser.cpp',
            'yaml-cpp-0.5.1/src/stream.cpp',
            'yaml-cpp-0.5.1/src/tag.cpp',
            'yaml-cpp-0.5.1/src/contrib/graphbuilder.cpp',
            'yaml-cpp-0.5.1/src/contrib/graphbuilderadapter.cpp',
        ],
        extra_compile_args = [ '-std=c++0x' ],
        extra_link_args = linkerExtras)

exec(open('python/job_stream/version.py').read())
setup(name = 'job_stream',
        version = __version__,  # From python/job_stream/version.py
        install_requires = [ 'appdirs', 'click', 'pandas', 'six' ],
        description = 'job_stream: easy and sophisticated parallelization',
        long_description = long_desc,
        author = 'Walt Woods',
        author_email = 'woodswalben@gmail.com',
        url = 'https://github.com/wwoods/job_stream',
        license = 'MIT',
        scripts = [ 'bin/job_stream' ],
        packages = [ 'job_stream' ],
        package_dir = { '': 'python' },
        ext_modules = [ job_stream ])

