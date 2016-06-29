
from .common import JobStreamTest

import distutils.spawn
import os
import subprocess
import tempfile

exDir = os.path.join(os.path.dirname(__file__), 'lib/')

class TestBin(JobStreamTest):
    # Tests the bin/job_stream script

    def setUp(self):
        self._jsbin = distutils.spawn.find_executable('job_stream')


    def _hostname(self):
        return subprocess.check_output([ 'hostname' ]).decode('utf-8').strip()


    def _run(self, args):
        print("Running {}...".format(args))
        p = subprocess.Popen([ self._jsbin ] + args, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        r = p.wait()
        return r, stdout.decode('utf-8'), stderr.decode('utf-8')


    def _runOk(self, args):
        r, stdout, stderr = self._run(args)
        if r != 0:
            raise Exception("Return non-zero.\nstdout\n{}\nstderr\n{}".format(
                    stdout, stderr))
        return stdout


    def test_default(self):
        old = self._runOk([ '--default' ]).strip()
        try:
            name = 'test.file'
            exp = os.path.abspath(name)
            self._runOk([ '--default', name ])
            new = self._runOk([ '--default' ]).strip()
            self.assertEqual(exp, new)

            # Check unsetting it
            self._runOk([ '--default', '' ])
            self.assertEqual('None', self._runOk([ '--default' ]).strip())
        finally:
            self._runOk([ '--default', old ])


    def test_help(self):
        r, stdout, stderr = self._run([ '--help' ])
        print("stdout\n{}\nstderr\n{}".format(stdout, stderr))
        self.assertEqual(8, r)


    def test_runDuplicateHost(self):
        r, stdout, stderr = self._run([ '--host', 'a,a', 'ls' ])
        print("stdout\n{}\nstderr\n{}".format(stdout, stderr))
        self.assertNotEqual(0, r)
        self.assertTrue('Duplicate host entry? a' in stderr)


    def test_runWithDefault(self):
        old = self._runOk([ '--default' ]).strip()
        try:
            with tempfile.NamedTemporaryFile('w+t') as f:
                f.write("{}\n".format(self._hostname()))
                f.flush()

                self._runOk([ '--default', f.name ])
                r = self._runOk([ 'echo', 'hi' ]).strip()
                self.assertLinesEqual('hi\n', r)
        finally:
            self._runOk([ '--default', old ])


    def test_runHost(self):
        stdout = self._runOk([ '--host', self._hostname(), 'python',
                os.path.join(exDir, 'testJob.py') ])
        self.assertLinesEqual("2\n3\n5\n9\naddOne setup\n", stdout)

