"""job_stream's python module implementation.

inline
======

Often, the :mod:`job_stream.inline` module is preferred.

Example Usage
=============

.. code-block:: python

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


Submodules
==========

.. autosummary::
    :toctree: _autosummary

    job_stream.inline

Members
=======
"""

from .common import (Frame, Job, Object, Reducer, checkpointInfo,
        getRank, invoke, map, run, _cpuThreadTime)

# This module's work global is assigned in common.py, as:
# job_stream.work = _Work()

