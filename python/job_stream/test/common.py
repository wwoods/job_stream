
import os
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest

class ExecuteError(Exception):
    def __init__(self, cmd, returnCode, stdout, stderr):
        Exception.__init__(self, "Bad exit code from {}: {}".format(cmd, returnCode))
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
        nargs = [ 'mpirun', '-np', str(np), sys.executable, '-u' ] + args
        # Yes, we COULD just use communicate(), but that hides run-forever bugs if we
        # ever need to see them.
        def tee(s, buf):
            def doTee():
                while True:
                    line = s.readline()
                    if not line:
                        break
                    print line[:-1]
                    buf.append(line)
            t = threading.Thread(target = doTee)
            t.daemon = True
            t.start()
            return t
        p = subprocess.Popen(nargs, stdout = subprocess.PIPE, 
                stderr = subprocess.PIPE)
        out = []
        err = []
        tees = [ tee(p.stdout, out), tee(p.stderr, err) ]
        r = p.wait()
        [ t.join() for t in tees ]
        out = ''.join(out)
        err = ''.join(err)
        if r != 0:
            raise ExecuteError(args, r, out, err)
        return out, err


    def executePy(self, pySrc, np = 1):
        with tempfile.NamedTemporaryFile() as f:
            f.write(textwrap.dedent(pySrc))
            f.flush()

            return self.execute(f.name, np = np)


    def safeRemove(self, path):
        try:
            os.remove(path)
        except OSError, e:
            # Doesn't exit
            if e.errno != 2:
                raise
