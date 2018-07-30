
from .common import JobStreamTest

import contextlib
import distutils.spawn
import os
import re
import socket
import subprocess
import tempfile
import textwrap
import time

exDir = os.path.join(os.path.dirname(__file__), 'lib/')

class TestBin(JobStreamTest):
    # Tests the bin/job_stream script

    def setUp(self):
        self._jsbin = distutils.spawn.find_executable('job_stream')


    def _checkpointSearch(self, stderr):
        m = re.search(r".*Using ([^\s]+) as checkpoint file$", stderr,
                flags=re.M)
        if m is None:
            return None
        return m.group(1)


    def _checkpointRun(self, args, opts=""):
        """Runs a Python script with ``opts`` as argument to Work(), returns
        checkpoint used or None for no checkpointing.
        """
        script = textwrap.dedent(r"""
                import job_stream.inline as inline
                with inline.Work([1, 2, 3], {opts}) as w:
                    @w.job
                    def addOne(n):
                        return n+1
                    @w.result
                    def printer(n):
                        print(n)
                """).format(opts=opts)
        with tempfile.NamedTemporaryFile('w') as f:
            f.write(script)
            f.flush()

            out, err = self._runSelf(args + [ 'python', f.name ])

        self.assertLinesEqual("2\n3\n4\n", out)
        return self._checkpointSearch(err)


    def _hostname(self):
        return socket.gethostname()


    def _readConfig(self, key):
        output = self._runOk([ 'config' ])
        m = re.search("^{}=(.*)$".format(key), output, flags=re.M)
        if m is None:
            raise ValueError("No {}?\n{}".format(key, output))
        r = m.group(1).strip()
        if r.lower() == 'none':
            return ''
        return r


    def _run(self, args):
        print("Running {}...".format(args))
        p = subprocess.Popen([ self._jsbin ] + args, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        r = p.wait()
        stdout = stdout.decode('utf-8')
        stdout = self._filterMpiStdout(stdout)
        return r, stdout, stderr.decode('utf-8')


    def _runOk(self, args):
        r, stdout, stderr = self._run(args)
        if r != 0:
            raise Exception("Return non-zero.\nstdout\n{}\nstderr\n{}".format(
                    stdout, stderr))
        return stdout


    def _runSelf(self, args, nonZeroOk=False):
        r, stdout, stderr = self._run([ '--host', self._hostname() ] + args)
        if r != 0 and not nonZeroOk:
            raise Exception("Return non-zero.\nstdout\n{}\nstderr\n{}".format(
                    stdout, stderr))

        # Pring for debugging
        print("stdout\n{}\nstderr\n{}".format(stdout, stderr))
        return stdout, stderr


    def _safeRemoveChkpt(self, path):
        for p in [ path, path + '.done' ]:
            try:
                os.remove(p)
            except OSError as e:
                # Does not exist is ok
                if e.errno != 2:
                    raise


    def _saveDefault(self):
        @contextlib.contextmanager
        def _saveDefaultInner():
            old = self._readConfig('hostfile')
            try:
                yield
            finally:
                self._runOk([ 'config', 'hostfile={}'.format(old) ])
        return _saveDefaultInner()


    def test_checkpoint_nil(self):
        self.assertEqual(None, self._checkpointRun([]))


    def test_checkpoint_c(self):
        self._safeRemoveChkpt("job_stream.chkpt")
        try:
            self.assertEqual("job_stream.chkpt", self._checkpointRun(['-c']))
        finally:
            # Clean out to prevent dev clutter
            self._safeRemoveChkpt("job_stream.chkpt")


    def test_checkpoint_cWithProcessorArg(self):
        # Ensure processor arg overrides c
        self._safeRemoveChkpt("/tmp/js2.chkpt")
        self.assertEqual("/tmp/js2.chkpt", self._checkpointRun(['-c'],
                opts="checkpointFile='/tmp/js2.chkpt'"))


    def test_checkpoint_checkpoint(self):
        self._safeRemoveChkpt("/tmp/js2.chkpt")
        self.assertEqual("/tmp/js2.chkpt", self._checkpointRun(['--checkpoint',
                '/tmp/js2.chkpt']))


    def test_configHostfile(self):
        with self._saveDefault():
            name = 'test.file'
            exp = os.path.abspath(name)
            self._runOk([ 'config', 'hostfile={}'.format(name) ])
            new = self._readConfig('hostfile')
            self.assertEqual(exp, new)

            # Check unsetting it
            self._runOk([ 'config', 'hostfile=' ])
            self.assertEqual('', self._readConfig('hostfile'))


    def test_gitResultsProgress(self):
        def progress():
            out, err = self._runSelf([ '--checkpoint', '/tmp/js.chkpt',
                    'git-results-progress' ])
            return float(out.strip())
        def touch(path):
            with open(path, 'w') as f:
                f.write("\n")

        self.safeRemove(['/tmp/js.chkpt', '/tmp/js.chkpt.done'])
        self.assertEqual(-1., progress())

        now = time.time()
        time.sleep(0.01)  # Weird filesystem timing issue
        touch("/tmp/js.chkpt")
        andNow = time.time()
        self.assertLessEqual(now, progress())
        self.assertGreaterEqual(andNow, progress())

        self.safeRemove(['/tmp/js.chkpt'])
        touch("/tmp/js.chkpt.done")
        andNow = time.time()
        self.assertLessEqual(now, progress())
        self.assertGreaterEqual(andNow, progress())


    def test_help(self):
        r, stdout, stderr = self._run([ '--help' ])
        print("stdout\n{}\nstderr\n{}".format(stdout, stderr))
        self.assertEqual(0, r)
        self.assertTrue('Usage: job_stream [OPTIONS] PROG [ARGS]...'
                in stdout)


    def test_hostfile_cpus(self):
        # Check that limiting / specifying cpus via the hostfile works.
        print_cpus = ['--', 'python', '-c',
                'import job_stream as j, _job_stream as _j; '
                    'print(j.map(lambda x: _j.getHostCpuCount(), [0]))']

        for nCpu in range(1, 4):
            r, stdout, stderr = self._run([ '--host', '{} cpus={}'.format(
                self._hostname(), nCpu)] + print_cpus)
            self.assertEqual("[{}]\n".format(nCpu), stdout)

        # Check that a bad parameter raises an error
        _, _, stderr = self._run([ '--host',
                '{} cpu=1'.format(self._hostname())] + print_cpus)
        self.assertTrue('Unparseable property:' in stderr)
        self.assertTrue('cpu' in stderr)

        # Check that something that's not a parameter raises
        _, _, stderr = self._run([ '--host',
                '{} cpus'.format(self._hostname())] + print_cpus)
        self.assertTrue('Unparseable properties:' in stderr)
        self.assertTrue('cpus' in stderr)

        # Check that a comment inline is OK
        r, stdout, stderr = self._run([ '--host', '{} cpus={} #Yup'.format(
            self._hostname(), 22)] + print_cpus)
        self.assertEqual("[{}]\n".format(22), stdout)


    def test_runDuplicateHost(self):
        r, stdout, stderr = self._run([ '--host', 'a,a', 'ls' ])
        print("stdout\n{}\nstderr\n{}".format(stdout, stderr))
        self.assertNotEqual(0, r)
        self.assertTrue('Duplicate host entry? a' in stderr)


    def test_runWithDefault(self):
        with self._saveDefault():
            with tempfile.NamedTemporaryFile('w+t') as f:
                f.write("{}\n".format(self._hostname()))
                f.flush()

                self._runOk([ 'config', 'hostfile={}'.format(f.name) ])
                r = self._runOk([ 'echo', 'hi' ]).strip()
                self.assertLinesEqual('hi\n', r)


    def test_runHost(self):
        stdout = self._runOk([ '--host', self._hostname(), 'python',
                os.path.join(exDir, 'testJob.py') ])
        self.assertLinesEqual("2\n3\n5\n9\naddOne setup\n", stdout)


    def test_stderrBuffering(self):
        # Without -u, python buffers its buffers by block when spawned by
        # another process.  This makes error messages occur out of order.
        with tempfile.NamedTemporaryFile('w') as f:
            f.write(textwrap.dedent(r"""
                    import job_stream.inline as inline
                    with inline.Work([1,2,3]) as w:
                        @w.job
                        def raiser(n):
                            raise ValueError(n)
                    """))
            f.flush()

            out, err = self._runSelf([ 'python', f.name ], nonZeroOk=True)

        # We want "Traceback" to always appear before "While processing..."
        mTrace = re.search(r"^Traceback.*:$", err, flags=re.M)
        if mTrace is None:
            self.fail("No traceback?")
        mWhile = re.search(r".*While processing work with target:.*$", err,
                flags=re.M)
        if mWhile is None:
            self.fail("No while processing?")
        self.assertLess(mTrace.start(), mWhile.start())

