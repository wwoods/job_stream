
from .common import JobStreamTest

import pandas as pd


class TestBaked(JobStreamTest):
    def test_sweep(self):
        tPath = '/tmp/js_out.csv'
        src = """
                from job_stream.baked import sweep
                import numpy as np

                with sweep({{ 'a': np.arange(10) }}, -100, output='{}') as w:
                    @w.job
                    def square(id, trial, a):
                        return {{ 'value': a*a + np.random.random()*8 }}
                """.format(tPath)

        # Since this is a thing, ensure 5 failures to fail
        for nfail in range(5, -1, -1):
            try:
                r = self.executePy(src)

                df = pd.read_csv(tPath).set_index('id')
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

