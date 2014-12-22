# Python module configuration file, used to build job_stream python module,
# which can be used if you want to use python for your tasks.  Because let's
# face it, Python is nicer than C/C++.

from distutils.core import setup, Extension
import os
import re
import subprocess

# Extra directories coming from non-system paths.
incdirs = [ '.', 'job_stream/boost_process' ]
libdirs = []
libraries = []
if 'LD_LIBRARY_PATH' in os.environ:
    libdirs.extend(os.environ['LD_LIBRARY_PATH'].split(':'))

# Find MPI flags
mpiCompiler = 'mpicxx'
if 'MPICXX' in os.environ:
    mpiCompiler = os.environ['MPICXX']
def checkedRun(args):
    p = subprocess.Popen(args, stdout = subprocess.PIPE,
            stderr = subprocess.PIPE)
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
            'boost_system', 'boost_thread', 'boost_serialization', 'boost_mpi',
            'yaml-cpp', 'rt' ],
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
        ],
        extra_compile_args = [ '-std=c++0x' ],
        extra_link_args = [])

setup(name = 'job_stream',
        version = '0.1',
        description = 'job_stream: easy and sophisticated parallelization',
        author = 'Walt Woods',
        author_email = 'woodswalben@gmail.com',
        url = 'https://github.com/wwoods/job_stream',
        license = 'MIT',
        packages = [ 'job_stream' ],
        package_dir = { '': 'python' },
        ext_modules = [ job_stream ])
