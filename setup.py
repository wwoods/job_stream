# Python module configuration file, used to build job_stream python module,
# which can be used if you want to use python for your tasks.  Because let's
# face it, Python is nicer than C/C++.

from distutils.core import setup, Extension
import os
import re
import subprocess

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
incdirs = [ '.', 'job_stream/boost_process', 'yaml-cpp-0.5.1/include' ]
libdirs = []
libraries = []
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
    except OSError, e:
        if e.errno != 2:
            raise
        raise Exception("Error executing program: {}".format(args))
    stdout, stderr = p.communicate()
    r = p.wait()
    if r != 0:
        raise Exception("Program failed with {}: {}".format(r, args))
    return stdout.strip()
mpiFlagsCompile = checkedRun([ mpiCompiler, "--showme:compile" ])
mpiFlagsLink = checkedRun([ mpiCompiler, "--showme:link" ])

for r in re.finditer(r'-I([^\s]+)', mpiFlagsCompile):
    incdirs.append(r.group(1))
for r in re.finditer(r'-L([^\s]+)', mpiFlagsLink):
    libdirs.append(r.group(1))
for r in re.finditer(r'-l([^\s]+)', mpiFlagsLink):
    libraries.append(r.group(1))

job_stream = Extension('_job_stream',
        define_macros = [ ('MAJOR_VERSION', '0'),
            ('MINOR_VERSION', '1') ],
        include_dirs = incdirs + [ '/usr/local/include' ],
        libraries = libraries + [ 'boost_filesystem', 'boost_python',
            'boost_system', 'boost_thread', 'boost_serialization', 'boost_mpi'
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
        extra_link_args = [])

setup(name = 'job_stream',
        version = '0.1.6',
        description = 'job_stream: easy and sophisticated parallelization',
        long_description = long_desc,
        author = 'Walt Woods',
        author_email = 'woodswalben@gmail.com',
        url = 'https://github.com/wwoods/job_stream',
        license = 'MIT',
        packages = [ 'job_stream' ],
        package_dir = { '': 'python' },
        ext_modules = [ job_stream ])
