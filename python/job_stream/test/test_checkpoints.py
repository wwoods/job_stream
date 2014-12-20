
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
            except ExecuteError, e:
                allOut.append(e.stdout)
                allErr.append(e.stderr)
                if t == maxTries - 1 or 'RuntimeError' in e.stderr:
                    self.fail("Failed after {} tries:\nstdout\n{}\n\nstderr\n{}"
                            .format(t+1, ''.join(allOut),
                                ''.join(allErr)))
        return ''.join(allOut), ''.join(allErr), t


    def test_checkpoint1(self):
        self.safeRemove(tmpPath)
        args = [ os.path.join(libPath, "checkpoint_1.py"), tmpPath ]
        allOut, allErr, trials = self.runTilDone(args, np = 4)
        self.assertEqual("4\n", allOut)
        allOut, allErr, trials = self.runTilDone(args, np = 4)
        self.assertEqual("4\n", allOut)


    def test_checkpoint2(self):
        self.safeRemove(tmpPath)
        args = [ os.path.join(libPath, "checkpoint_2.py"), tmpPath ]
        allOut, allErr, trials = self.runTilDone(args)
        self.assertEqual("HIHIH", allOut)
