
from .common import JobStreamTest

import os

libDir = os.path.join(os.path.dirname(__file__), 'lib')

class TestPythonConfig(JobStreamTest):
    def test_job(self):
        self.assertLinesEqual("45\n",
                self.execute(os.path.join(libDir, "config_in_python1.py"))[0])


    def test_job2(self):
        # Note that this test being comprehensive / useful relies on the fact that the
        # python config is transcribed into YAML, and then deserialized back into python
        # by job_stream
        r = self.executePy("""
            import job_stream
            class AddN(job_stream.Job):
                def handleWork(self, w):
                    if type(self.config['valI']) != int:
                        raise Exception("valI not int?")
                    if type(self.config['valF']) != float:
                        raise Exception("valF not float?")
                    if not isinstance(self.config['valS'], str):
                        raise Exception("valS not string?")
                    self.emit(w + self.config['valI'])
                    self.emit(w + self.config['valF'])
                    self.emit(str(w) + self.config['valS'])

            job_stream.work = [ 5, 8 ]
            job_stream.run({ 'jobs': [ { 'type': AddN, 'valI': 5, 'valF': 6.0, 
                    'valS': 'hi' } ] })
            """)
        self.assertLinesEqual("10\n13\n11.0\n14.0\n'5hi'\n'8hi'\n", r[0])
