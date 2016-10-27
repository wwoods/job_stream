
import os
import six
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest

_stdout_encoding = "utf-8"
if getattr(sys.stdout, "encoding", ""):
    _stdout_encoding = sys.stdout.encoding


class ExecuteError(Exception):
    def __init__(self, cmd, returnCode, stdout, stderr):
        Exception.__init__(self, "Bad exit code from {}: {}".format(cmd, returnCode))
        self.stdout = stdout
        self.stderr = stderr


class JobStreamTest(unittest.TestCase):
    def assertLinesEqual(self, a, b):
        """Asserts that strings a and b contain the same lines, albeit perhaps
        in a different order."""
        al = [ p for p in textwrap.dedent(a).split("\n") if p ]
        bl = [ p for p in textwrap.dedent(b).split("\n") if p ]

        al.sort()
        bl.sort()

        self.assertEqual(al, bl)


    def execute(self, args, np=1, env=None):
        if not isinstance(args, list):
            args = [ args ]
        nargs = [ 'mpirun', '-q', '-np', str(np), sys.executable, '-u' ] + args
        # Yes, we COULD just use communicate(), but that hides run-forever bugs if we
        # ever need to see them.
        def tee(s, buf):
            def doTee():
                while True:
                    line = s.readline()
                    try:
                        dline = line.decode(_stdout_encoding)
                    except UnicodeDecodeError as e:
                        six.raise_from(Exception("For '{}'".format(line)), e)
                    if not dline:
                        break
                    print(dline[:-1])
                    buf.append(dline)
            t = threading.Thread(target = doTee)
            t.daemon = True
            t.start()
            return t
        wholeEnv = os.environ.copy()
        if env is not None:
            wholeEnv.update(env)
        p = subprocess.Popen(nargs, stdout = subprocess.PIPE,
                stderr = subprocess.PIPE, env=wholeEnv)
        out = []
        err = []
        tees = [ tee(p.stdout, out), tee(p.stderr, err) ]
        r = p.wait()
        [ t.join() for t in tees ]

        # Filter out error output in stdout
        for i in range(len(out) - 4, -1, -1):
            if (out[i].startswith(u"--------")
                    and out[i+1].startswith(u"Primary job")
                    and out[i+2].startswith(u"a non-zero exit code")
                    and out[i+3].startswith(u"--------")):
                out = out[:i] + out[i+4:]

        out = ''.join(out)
        err = ''.join(err)
        if r != 0:
            raise ExecuteError(args, r, out, err)
        return out, err


    def executePy(self, pySrc, np = 1):
        with tempfile.NamedTemporaryFile() as f:
            f.write(textwrap.dedent(pySrc).encode("utf-8"))
            f.flush()

            return self.execute(f.name, np=np)


    def safeRemove(self, pathOrPaths):
        path = pathOrPaths
        if isinstance(path, (tuple, list)):
            for p in path:
                self.safeRemove(p)
            return

        try:
            os.remove(path)
        except OSError as e:
            # Doesn't exit
            if e.errno != 2:
                raise
