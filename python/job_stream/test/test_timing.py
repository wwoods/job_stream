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

        # Regression: userCpu should be <= userPct (cannot have CPU time
        # without wall-clock time)
        self.assertLessEqual(vals[1], vals[0])
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
                    while job_stream._cpuThreadTime() - s < 0.011:
                        i += 1
            """.format(self.USE_MULTI)
        r = self.executeAndGetTime(src)
        self.assertLess(0.04, r['userPct'] * r['runTime'])
        self.assertGreater(0.2, r['userPct'] * r['runTime'])
        self.assertLess(0.04, r['userCpu'] * r['runTime'])
        self.assertGreater(0.05, r['userCpu'] * r['runTime'])


    def test_job_sleep(self):
        src = """
            import time
            import job_stream.inline as inline
            with inline.Work(range(4), useMultiprocessing={}) as w:
                @w.job
                def sleeper(t):
                    time.sleep(0.011)
            """.format(self.USE_MULTI)
        r = self.executeAndGetTime(src)
        self.assertLess(0.04, r['userPct'] * r['runTime'])
        self.assertGreater(0.2, r['userPct'] * r['runTime'])
        self.assertGreater(0.01, r['userCpu'] * r['runTime'])


    def test_job_cpuFrame(self):
        src = """
            import time
            import job_stream
            import job_stream.inline as inline
            with inline.Work([ 10 ], useMultiprocessing={}) as w:
                @w.frame
                def frameStart(store, n):
                    if not hasattr(store, 'init'):
                        store.init = True
                        return inline.Multiple(range(n))

                @w.job
                def doStuff(n):
                    s = job_stream._cpuThreadTime()
                    while job_stream._cpuThreadTime() - s < 0.011:
                        pass

                @w.frameEnd
                def frameEnd(store, n):
                    pass
            """.format(self.USE_MULTI)
        r = self.executeAndGetTime(src)
        self.assertLess(0.1, r['userPct'] * r['runTime'])
        self.assertGreater(0.8, r['userPct'] * r['runTime'])
        self.assertLess(0.1, r['userCpu'] * r['runTime'])
        self.assertGreater(0.2, r['userCpu'] * r['runTime'])



class TestTimingMulti(TestTimingNoMulti):
    USE_MULTI = True


