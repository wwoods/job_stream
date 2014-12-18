
from .common import JobStreamTest

import os

libDir = os.path.join(os.path.dirname(__file__), 'lib')

class TestPythonConfig(JobStreamTest):
    def test_job(self):
        self.assertLinesEqual("45\n",
                self.execute(os.path.join(libDir, "testPythonConfig1.py")))
