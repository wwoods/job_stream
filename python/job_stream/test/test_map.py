
from .common import JobStreamTest

import textwrap

class TestMap(JobStreamTest):
    """Tests map() functionality that is compatible with the builtin map()
    function.  MY_MODULE controls where map() comes from; None indicates that
    the builtin should be used, and is used to test compatibility."""

    MY_MODULE = None
    def test_map(self):
        r, _ = self.executePy(self._addMap("""
                r = map(lambda w: w+1, [8, 9])
                print(list(r))"""))
        self.assertLinesEqual("[9, 10]", r)


    def test_map_big(self):
        r, _ = self.executePy(self._addMap("""
                r = map(lambda w:w**2, range(100))
                print(list(r))"""))
        exp = "[" + ", ".join([ str(i**2) for i in range(100) ]) + "]"
        self.assertLinesEqual(exp, r)


    def test_map_multiple(self):
        r, _ = self.executePy(self._addMap("""
                r = map(lambda a, b: a*b, range(5), range(3, 8))
                print(list(r))"""))
        exp = "[" + ", ".join([ str(i*(i+3)) for i in range(5) ]) + "]"
        self.assertLinesEqual(exp, r)


    def _addMap(self, r):
        r = textwrap.dedent(r)
        if self.MY_MODULE is None:
            return r
        return "from {} import map\n{}".format(self.MY_MODULE, r)



class TestMapJs(TestMap):
    MY_MODULE = "job_stream"



class TestMapInline(TestMap):
    MY_MODULE = "job_stream.inline"

