"""Uses the inline module to test timing operations.
"""

from .common import JobStreamTest

import re


class TestTimingNoMulti(JobStreamTest):
    USE_MULTI = False
    RE_TIME = re.compile(r"^C (\d+)% user time, (\d+)% user cpu, quality (\d+\.\d+) cpus, ran (\d+\.\d+)s$",
            re.MULTILINE)


    def executeAndGetTime(self, pySrc):
        r = self.executePy(pySrc)
        m = self.RE_TIME.search(r[1])
        if m is None:
            self.fail("No timing information found")

        vals = [ float(v) for v in m.groups() ]
        keys = [ 'userPct', 'userCpu', 'quality', 'runTime' ]
        vals[0] *= 0.01
        vals[1] *= 0.01
        return dict(zip(keys, vals))


    def test_job_cpu(self):
        src = """
            import time
            import job_stream
            import job_stream.inline as inline
            with inline.Work(range(4), useMultiprocessing={}) as w:
                @w.job
                def sleeper(t):
                    s = job_stream._cpuThreadTime()
                    i = 0
                    while job_stream._cpuThreadTime() - s < 0.1:
                        i += 1
            """.format(self.USE_MULTI)
        r = self.executeAndGetTime(src)
        self.assertLess(0.4, r['userPct'] * r['runTime'])
        self.assertLess(0.4, r['userPct'] * r['userCpu'] * r['runTime'])


    def test_job_sleep(self):
        src = """
            import time
            import job_stream.inline as inline
            with inline.Work(range(4), useMultiprocessing={}) as w:
                @w.job
                def sleeper(t):
                    time.sleep(0.11)
            """.format(self.USE_MULTI)
        r = self.executeAndGetTime(src)
        self.assertLess(0.4, r['userPct'] * r['runTime'])
        self.assertGreater(0.01, r['userCpu'] * r['runTime'])



class TestTimingMulti(TestTimingNoMulti):
    USE_MULTI = True


