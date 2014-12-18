
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
        p = subprocess.Popen([ sys.executable ] + args,
                stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        out, err = p.communicate()
        r = p.wait()
        if r != 0:
            self.fail("Stdout: {}\nStderr: {}\nBad exit code from {}: {}".format(
                    out, err, args, r))
        return out
