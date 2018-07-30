job_stream
==========

An MPI-based C++ or Python library for easy, distributed pipeline processing, with an emphasis on running scientific simulations.  The project is hosted `on Github <https://github.com/wwoods/job_stream>`_.


.. contents::
    :depth: 2
    :local:


Introduction
------------

.. _intro-deterministic:

Deterministic Experiments
~~~~~~~~~~~~~~~~~~~~~~~~~

``job_stream`` is a straightforward and effective way to implement distributed computations, geared towards scientific applications.  How straightforward?  If we wanted to find all primes between 0 and 999:

.. code-block:: python

    # Import the main Work object that manages the computation's graph.
    from job_stream.inline import Work

    # Start by declaring work based on the list of numbers between 0 and 999 as a
    # piece of `Work`.  When the w object goes out of context, the job_stream will
    # be executed.
    with Work(range(1000)) as w:
        # For each of those numbers, see if that number is prime.
        @w.job
        def isPrime(x):
            for i in range(2, int(x ** 0.5) + 1):
                if x % i == 0:
                    return
            print(x)

If you wanted to run this script, we'll call it ``test.py``, on every machine in your cluster:

.. code-block:: sh

    $ job_stream -- python test.py

If your process is long-running, and machines in the cluster sometimes crash or have other issues that interrupt processes, using ``job_stream``'s built-in checkpointing can stop your application from having to re-process the same work over and over:

.. code-block:: sh

    $ job_stream -c -- python test.py


Stochastic Experiments
~~~~~~~~~~~~~~~~~~~~~~

Using the :meth:`job_stream.baked.sweep` method, ``job_stream`` can automatically run experiments until a given confidence interval for sampled values is met; the default is a 95% confidence interval of +/- 10% of the value's sampled mean.  For example:

.. code-block:: python

    from job_stream.baked import sweep
    import numpy as np

    # Parameters to be tested can be passed as the first argument to sweep;
    # the second argument, if specified, is the number of trials for each
    # parameter combination (negative to auto-detect with a maximum threshold).
    with sweep({ 'param': np.linspace(1, 10, 3) }) as w:
        @w.job
        def handle(id, trial, param):
            # param is a value from 1 to 10, as partitioned by np.linspace.
            # Choose noise with zero-mean and standard deviation about 0.5.
            noise = (np.random.random() - 0.5) * 1.73
            return { 'value': param + noise }

This code is run the same way as the :ref:`intro-deterministic` experiments:

.. code-block:: sh

    $ job_stream -- python test.py
        trials  param      value  value_dev  value_err
    id
    2        5   10.0  10.095083   0.522214   0.649245
    1        8    5.5   5.295684   0.519942   0.472436
    0      102    1.0   1.009054   0.509398   0.098858

The result is a nice table (which can optionally be saved directly to a CSV)
with which parameters were tried, which values were recorded, and the standard
deviation and expected error in the reported mean (with 95% confidence) of
those recorded values.

.. note:: The experiment with ``param == 1.`` required many more trials because
        10% of 1 is smaller than 10% of 10 or 5.5.  However,
        :meth:`job_stream.baked.sweep` allows the stopping criteria tolerances
        to be changed, as well as a hard limit set on the number of trials;
        see the documentation for more information.


Features
~~~~~~~~

``job_stream`` lets developers write their code in an imperative style, and does all the heavy lifting behind the scenes.  While there are a lot of task processing libraries out there, job_stream bends over backwards to make writing distributed processing tasks easy while maintaining the flexibility required to support complicated parallelization paradigms.  What all is in the box?

* **Easy python interface** to keep coding in a way that you are comfortable; often, users only need parts of :mod:`job_stream.baked`.
* **Jobs and reducers** to implement common map/reduce idioms.  However, job_stream reducers also allow *recurrence*!
* **Frames** as a more powerful, recurrent addition to map/reduce.  If the flow of your data depends on that data, for instance when running a calculation until the result fits a specified tolerance, frames are a powerful tool to get the job done.
* **Automatic checkpointing** so that you don't lose all of your progress if a multi-day computations crashes on the second day
* **Intelligent job distribution** including job stealing, so that overloaded machines receive less work than idle ones
* **Execution Statistics** so that you know exactly how effectively your code parallelizes


Installation
------------

Requirements
~~~~~~~~~~~~

* `boost <http://www.boost.org>`_ (filesystem, mpi, python, regex, serialization, system, thread).
* mpi (perhaps `OpenMPI <http://www.open-mpi.org>`_).

Note that ``job_stream`` internally uses `yaml-cpp <https://github.com/jbeder/yaml-cpp>`_, but for convenience it is packaged with ``job_stream``.

Python
~~~~~~

It is **strongly** recommended that users use a virtualenv such as `Miniconda <http://conda.pydata.org/miniconda.html>`_ to install ``job_stream``.  The primary difficulty that this helps users to avoid is any boost incompatibilities, which can happen in academic environments.  Once Miniconda is installed and the ``conda`` application is on the user's ``PATH``, installing becomes as easy as:

.. code-block:: sh

    $ conda install boost
    $ pip install job_stream

.. note::

    If not using ``conda``, run the pip install command alone.  You may need to
    specify custom include or library paths:

    .. code-block:: sh

        CPLUS_INCLUDE_PATH=~/my/path/to/boost/ \
            LD_LIBRARY_PATH=~/my/path/to/boost/stage/lib/ \
            pip install job_stream


.. note::

    If you want to use an ``mpicxx`` other than your system's default, you may also specify ``MPICXX=...`` as an environment variable passed to ``pip``.

C++ Library
~~~~~~~~~~~

Create a build/ folder, cd into it, and run:

    cmake .. && make -j8 test

.. note::

    You may need to tell the compiler where boost's libraries or include files are located.  If they are not in the system's default paths, extra paths may be specified with e.g. environment variables like this:

    .. code-block:: sh

        CPLUS_INCLUDE_PATH=~/my/path/to/boost/ \
            LD_LIBRARY_PATH=~/my/path/to/boost/stage/lib/ \
            bash -c "cmake .. && make -j8 test"

Build Paths
+++++++++++

Since ``job_stream`` uses some of the compiled boost libraries, know your platform's mechanisms of amending default build and run paths:

Linux
*****

* ``CPLUS_INCLUDE_PATH=...`` - Colon-delimited paths to include directories
* ``LIBRARY_PATH=...`` - Colon-delimited paths to static libraries for linking only
* ``LD_LIBRARY_PATH=...`` - Colon-delimited paths to shared libraries for linking and running binaries


Running Tests
+++++++++++++

Making the "test" target (with optional ARGS passed to test executable) will make and run any tests packaged with job_stream:

.. code-block:: sh

    cmake .. && make -j8 test [ARGS="[serialization]"]

Or to test the python library:

.. code-block:: sh

    cmake .. && make -j8 test-python [ARGS="../python/job_stream/test/"]


Distributed Execution
---------------------

job_stream comes bundled with a binary to help running job_stream applications: that executable is installed as ``job_stream`` in your Python distribution's ``bin`` folder.  At its simplest, ``job_stream`` is a wrapper for ``mpirun``.  It needs an MPI-like hostfile that lists machines to be used for distributing work.  For example, if your machine has a file named ``hostfile`` on it with the following contents:

.. code-block:: sh

    # This file's contents are in a file called 'hostfile'.
    machine1
    machine2
    #machine3  Commented lines will not be used
    machine4 cpus=3  # Specify number of worker threads / processes on machine4

Then ``job_stream`` may be configured to automatically run experiments on all un-commented machines in this hostfile by running:

.. code-block:: sh

    $ job_stream config hostfile=./path/to/hostfile

Afterwards, if any script is run via ``job_stream``, it will run on the machines specified by that file.  For example, running this command:

.. code-block:: sh

    $ job_stream -- python script.py

Will run ``script.py`` and distribute its work across machine1, machine2, and machine4.  If you ever want to run a script on something other than the default configured hostfile, ``job_stream`` accepts ``--host`` and ``--hostfile`` arguments (see ``job_stream --help`` for more information).

.. note:: You *must* separate any arguments to your program and ``job_stream``'s arguments with a ``--``, as seen above.

.. note:: Parameters, such as ``cpus=``, must be specified on the same line as the host!  Hosts attempt to match themselves to these lines by either shared name or shared IP address.

Checkpoints
~~~~~~~~~~~

The ``job_stream`` wrapper also allows specifying checkpoints so that you do not lose progress on a job if your machines go down:

.. code-block:: sh

    $ job_stream -c ...

The ``-c`` flag means that progress is to be stored in the working directory as
``job_stream.chkpt``; a different path and filename may be specified by using
``--checkpoint PATH`` instead.  Should the application crash, the job may be
resumed by re-executing the same command as the original invocation.


Python API
----------

See the :doc:`python` module documentation.


C++ API
-------

The most up-to-date C++ API is documented in the old README, which can be found `here <https://github.com/wwoods/job_stream/#c-basics>`_.  Note that the C++ language does not provide many of the features that make it as easy to use as in Python.  However, the C++ interface was also crafted with care so that it would not be too difficult.


