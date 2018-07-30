"""This file is used to test that invoke raises proper, informative errors
when the function is misused.
"""

from .common import JobStreamTest

from job_stream import invoke

import os
import shutil
import tempfile

class TestInvoke(JobStreamTest):
    @classmethod
    def setUpClass(cls):
        cls._oldPath = os.getcwd()
        cls._newPath = os.path.join(tempfile.gettempdir(), 'job_stream_invoke')
        try:
            os.makedirs(cls._newPath)
        except OSError as e:
            # Already exists is OK
            if e.errno != 17:
                raise
        os.chdir(cls._newPath)

        with open('test_no_exe', 'w') as f:
            f.write("""#! /usr/bin/env python
print("Hello, world")
""")
        with open('test_exe', 'w') as f:
            f.write("""#! /usr/bin/env python
print("Hello, world")
""")
        os.chmod('test_exe', 0o700)


    @classmethod
    def tearDownClass(cls):
        os.chdir(cls._oldPath)
        shutil.rmtree(cls._newPath)


    def test_invoke_absolute(self):
        o, e = invoke(['/bin/ls'])
        self.assertEqual("test_exe\ntest_no_exe\n", o)
        self.assertEqual("", e)


    def test_invoke_does_not_exist(self):
        with self.assertRaisesRegex(RuntimeError,
                r"^job_stream/invoke\.cpp.*Program not found.*"):
            invoke(['ls'])


    def test_invoke_not_exe(self):
        with self.assertRaisesRegex(RuntimeError,
                r"^job_stream/invoke\.cpp.*Program specified not executable.*"
                ):
            invoke(['./test_no_exe'])


    def test_invoke_relative(self):
        o, e = invoke(['./test_exe'])
        self.assertEqual("Hello, world\n", o)
        self.assertEqual("", e)

