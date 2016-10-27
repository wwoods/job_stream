
from .common import ExecuteError, JobStreamTest

import os
import tempfile

libPath = os.path.join(os.path.dirname(__file__), "lib")
tmpPath = os.path.join(tempfile.gettempdir(), "test.chkpt")

class TestCheckpoints(JobStreamTest):
    def runTilDone(self, args, np = 1, maxTries = 100):
        allOut = []
        allErr = []
        for t in range(maxTries):
            try:
                out, err = self.execute(args, np = np)
                allOut.append(out)
                allErr.append(err)
                break
            except ExecuteError as e:
                allOut.append(e.stdout)
                allErr.append(e.stderr)
                if (t == maxTries - 1
                        # OR if it DIDN'T exit due to forced checkpoint
                        or ', resuming computation' not in e.stderr):
                    self.fail("Failed after {} tries".format(t+1))
        return ''.join(allOut), ''.join(allErr), t


    def test_checkpoint1(self):
        args = [ os.path.join(libPath, "checkpoint_1.py"), tmpPath ]

        self.safeRemove([ tmpPath, tmpPath + '.done' ])
        allOut, allErr, trials = self.runTilDone(args, np = 1)
        self.assertEqual("4\n", allOut)

        self.safeRemove([ tmpPath, tmpPath + '.done' ])
        allOut, allErr, trials = self.runTilDone(args, np = 4)
        self.assertEqual("4\n", allOut)


    def test_checkpoint2(self):
        args = [ os.path.join(libPath, "checkpoint_2.py"), tmpPath ]

        self.safeRemove([ tmpPath, tmpPath + '.done' ])
        allOut, allErr, trials = self.runTilDone(args)
        self.assertTrue(10 <= trials)
        self.assertLinesEqual("110\n143\n143\n143\n", allOut)

        self.safeRemove([ tmpPath, tmpPath + '.done' ])
        allOut, allErr, trials = self.runTilDone(args, np = 2)
        self.assertTrue(10 <= trials)
        self.assertLinesEqual("110\n143\n143\n143\n", allOut)


    def test_checkpoint_nonexistent_parent(self):
        # Checkpoints, if the parent folder did not exist, used to lock up
        # job_stream permanently.
        cp = os.path.join(tempfile.gettempdir(), "a", "b", "c", "test.chkpt")
        args = [os.path.join(libPath, "checkpoint_1.py"), cp]

        # Initial check
        try:
            self.execute(args, np=1)
        except ExecuteError as e:
            self.assertIn('folder containing the requested checkpoint file '
                    'does not exist', e.stderr)

        # Check on writing checkpoint
        try:
            self.execute(args, np=1, env=dict(
                    JOBS_DEBUG_SKIP_INITIAL_CHECKPOINT_CHECK="true"))
        except ExecuteError as e:
            self.assertIn('Could not write checkpoint', e.stderr)

