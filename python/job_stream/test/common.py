
import subprocess
import sys
import unittest

class JobStreamTest(unittest.TestCase):
    def assertLinesEqual(self, a, b):
        """Asserts that strings a and b contain the same lines, albeit perhaps
        in a different order."""
        al = a.split("\n")
        bl = b.split("\n")

        al.sort()
        bl.sort()

        self.assertEqual(al, bl)


    def execute(self, args):
        if not isinstance(args, list):
            args = [ args ]
        #nargs = [ '/usr/bin/mpirun', '-np', '4', sys.executable ] + args
        nargs = [ sys.executable ] + args
        p = subprocess.Popen(nargs, stdin = subprocess.PIPE,
                stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        out, err = p.communicate()
        r = p.wait()
        if r != 0:
            self.fail("Stdout:\n{}\n\nStderr:\n{}\n\nBad exit code from {}: {}"
                    .format(out, err, args, r))
        return out
