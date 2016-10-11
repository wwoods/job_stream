"""job_stream's python module.  The primary way to use job_stream in Python is
via the :mod:`job_stream.inline` module.  It is highly recommended that new
users start there, or by looking at the :mod:`job_stream.baked` module for
high-level paradigms.


Submodules
==========

``job_stream`` provides several different APIs, arranged below in order of easiest to most difficult.  Often, the easiest solution is the one to use.

.. autosummary::
    :toctree: _autosummary

    job_stream.baked
    job_stream.inline
    job_stream.invoke
    job_stream.common

"""

from .common import (Object, checkpointInfo, getCpuCount, getRank, invoke, map,
        _cpuThreadTime)

from .version import __version__

