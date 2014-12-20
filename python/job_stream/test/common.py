
import os
import subprocess
import sys
import unittest

class ExecuteError(Exception):
    def __init__(self, cmd, returnCode, stdout, stderr):
        Exception.__init__(self, "Bad exit code from {}: {}\nStdout:\n{}\n\n"
                "Stderr:\n{}".format(cmd, returnCode, stdout, stderr))
        self.stdout = stdout
        self.stderr = stderr


class JobStreamTest(unittest.TestCase):
    def assertLinesEqual(self, a, b):
        """Asserts that strings a and b contain the same lines, albeit perhaps
        in a different order."""
        al = a.split("\n")
        bl = b.split("\n")

        al.sort()
        bl.sort()

        self.assertEqual(al, bl)


    def execute(self, args, np = 1):
        if not isinstance(args, list):
            args = [ args ]
        nargs = [ '/usr/bin/mpirun', '-np', str(np), sys.executable ] + args
        p = subprocess.Popen(nargs, stdin = subprocess.PIPE,
                stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        out, err = p.communicate()
        r = p.wait()
        if r != 0:
            raise ExecuteError(args, r, out, err)
        return out, err


    def safeRemove(self, path):
        try:
            os.remove(path)
        except OSError, e:
            # Doesn't exit
            if e.errno != 2:
                raise
