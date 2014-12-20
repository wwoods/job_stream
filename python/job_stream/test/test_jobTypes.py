
from .common import JobStreamTest

import os

libPath = os.path.join(os.path.dirname(__file__), "lib")

class TestJobTypes(JobStreamTest):
    def test_frame(self):
        r, _ = self.execute(os.path.join(libPath, "testFrame.py"))
        self.assertLinesEqual("runExperiments setup\naddOne setup\n100\n", r)


    def test_job(self):
        r, _ = self.execute(os.path.join(libPath, "testJob.py"))
        self.assertLinesEqual("addOne setup\n2\n3\n5\n9\n", r)


    def test_reducer(self):
        r, _ = self.execute(os.path.join(libPath, "testReducer.py"))
        self.assertLinesEqual("sum setup\naddOne setup\naddOne setup\n11\n", r)
