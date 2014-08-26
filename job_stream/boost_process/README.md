#Why boost::process is here
We need to control the environment for spawned child processes to remove any MPI
variables, in case the child process itself is an MPI application (e.g. Xyce).
job_stream was using libexecstream before, but that does not allow environment
control.

We embed this library here as it is not part of the official boost distribution.

2014-8-26 - Also, added poll() method to child.
