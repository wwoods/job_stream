
from .common import ExecuteError, JobStreamTest

import pandas as pd


class TestBaked(JobStreamTest):
    OUT_PATH = "/tmp/js_out.csv"

    @classmethod
    def setUpClass(cls):
        src = """
                from job_stream.inline import getCpuCount, Work
                with Work([1]) as w:
                    @w.job
                    def handle(r):
                        print(getCpuCount())
                """
        inst = TestBaked()
        cls._cpus = int(inst.executePy(src)[0].strip())


    def test_sweep(self):
        # Integration test; show that everything in general works
        src = """
                from job_stream.baked import sweep
                import numpy as np

                with sweep({{ 'a': np.arange(10) }}, -100, output='{}') as w:
                    @w.job
                    def square(id, trial, a):
                        return {{ 'value': a*a + np.random.random()*8 }}
                """.format(self.OUT_PATH)

        # Since this is a thing, ensure 5 failures to fail
        for nfail in range(5, -1, -1):
            try:
                r = self.executePy(src)

                df = pd.read_csv(self.OUT_PATH).set_index('id')
                print(df.to_string())
                self.assertEqual(10, len(df))
                self.assertTrue(
                        (
                            (df['value_dev'] > 0.5)
                            & (df['value_dev'] < 5.)
                        ).all())
                for i in range(10):
                    err = df.loc[i]['value_err']
                    self.assertLess(i*i+4 - 3.*err, df.loc[i]['value'])
                    self.assertGreater(i*i+4 + 3.*err, df.loc[i]['value'])
                self.assertLess(df.loc[0]['value_err'], df.loc[9]['value_err'])
            except:
                if not nfail:
                    raise
            else:
                break


    def test_sweep_list(self):
        # Ensure that a list of param dictionaries works
        src = """
                from job_stream.baked import sweep
                from job_stream import inline
                inline.getCpuCount = lambda: 8
                with sweep([ {{ 'a': 8 }}, {{ 'a': 9 }} ], output='{}') as s:
                    @s.job
                    def handle(id, trial, a):
                        return {{ 'value': a }}
                """.format(self.OUT_PATH)
        self.executePy(src)
        df = pd.read_csv(self.OUT_PATH).set_index('id')
        self.assertEqual(2, len(df))
        self.assertEqual(4, df.loc[0]['trials'])
        self.assertEqual(8, df.loc[0]['value'])
        self.assertEqual(9, df.loc[1]['value'])


    def test_sweep_list_mismatch(self):
        # Mismatched parameters should fail
        src = """
                from job_stream.baked import sweep
                with sweep([ { 'a': 8 }, { 'b': 9 } ]) as s:
                    @s.job
                    def handle(trial, id, a=8, b=9):
                        return { 'val': a }
                """
        try:
            self.executePy(src)
        except Exception as e:
            self.assertTrue('must have same keys; found ' in e.stderr)
            self.assertTrue(' against ' in e.stderr)
            self.assertTrue("{'a'}" in e.stderr)
            self.assertTrue("{'b'}" in e.stderr)
        else:
            self.fail("Nothing raised")


    def test_sweep_multiple(self):
        # Ensure that emitting Multiple() from a sweep pipeline fails
        src = """
                from job_stream.baked import sweep
                from job_stream.inline import Multiple
                with sweep() as s:
                    @s.job
                    def handle(id, trial):
                        return Multiple([ { 'a': 0 }, { 'a': 1 } ])
                """
        try:
            self.executePy(src)
        except Exception as e:
            self.assertTrue('cannot emit Multiple' in str(e.stderr))
        else:
            self.fail("Nothing raised")


    def test_sweep_minTrials(self):
        # Test minTrials behavior
        src = """
                # By using something that is NOT random, the minimum number of
                # trials is guaranteed to be sufficient.
                from job_stream.baked import sweep
                from job_stream import inline
                inline.getCpuCount = lambda: {cpus}
                with sweep({{ 'c': range({combos1}), 'c2': range({combos2}) }},
                        output='{out}', trials={trials}) as s:
                    @s.job
                    def handle(id, trial, c, c2):
                        return {{ 'value': 0. }}
                """
        def getTrials(trials, cpus, combos1=1, combos2=1):
            self.executePy(src.format(trials=trials, cpus=cpus,
                    out=self.OUT_PATH, combos1=combos1, combos2=combos2))
            df = pd.read_csv(self.OUT_PATH).set_index('id')
            self.assertEqual(combos1 * combos2, len(df))
            return df.loc[0]['trials']

        self.assertEqual(3, getTrials(0, 1))
        self.assertEqual(8, getTrials(0, 8))
        self.assertEqual(3, getTrials(-3, 8))
        self.assertEqual(8, getTrials(-10, 8))
        self.assertEqual(1, getTrials(1, 100))

        # With multiple parameter combinations
        self.assertEqual(3, getTrials(0, 1, 2, 2))
        self.assertEqual(8, getTrials(0, 32, 2, 2))
        self.assertEqual(2, getTrials(-2, 32, 2, 2))


    def test_sweep_trialsCount(self):
        # Ensure min/max work
        src = """
                from job_stream.baked import sweep
                from job_stream import inline
                inline.getCpuCount = lambda: 1
                trialsParms = {{}}
                if {min} > 0:
                    trialsParms['min'] = {min}
                if {max} > 0:
                    trialsParms['max'] = {max}
                with sweep(trials={trials}, trialsParms=trialsParms,
                        output='{out}') as s:
                    @s.job
                    def handle(id, trial):
                        return {{ 'v': trial if {dev} else 1. }}
                """
        def getTrials(min, max, testMax, trials=0):
            self.executePy(src.format(min=min, max=max, dev=testMax,
                    trials=trials, out=self.OUT_PATH))
            df = pd.read_csv(self.OUT_PATH).set_index('id')
            return df.loc[0]['trials']

        self.assertEqual(3, getTrials(3, 8, False))
        self.assertEqual(8, getTrials(3, 8, True))
        self.assertEqual(8, getTrials(8, 8, False))
        # One more to make sure a negative trials specification also is max
        self.assertEqual(8, getTrials(-1, -1, True, -8))

        # min cannot be > max
        with self.assertRaises(ExecuteError):
            getTrials(9, 8, False)
        # if min specified, trials cannot be positive
        with self.assertRaises(ExecuteError):
            getTrials(1, -1, True, 3)
        # but can be negative
        self.assertEqual(2, getTrials(2, -1, False, -8))

        # max cannot be specified with trials != 0
        with self.assertRaises(ExecuteError):
            getTrials(-1, 1, True, 3)
        with self.assertRaises(ExecuteError):
            getTrials(-1, 1, True, -3)

