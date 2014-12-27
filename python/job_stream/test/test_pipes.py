"""This file is for arbitrary pipes that may cause problems.  Edge cases, if you will.
"""

from .common import JobStreamTest

class TestPipes(JobStreamTest):
    def test_frameToGlobal(self):
        # This was failing awhile ago; outer reducer wasn't catching frame's
        # output
        r = self.executePy("""
            import job_stream
            class GlobalReducer(job_stream.Reducer):
                def handleInit(self, store):
                    print("Global init")
                def handleAdd(self, store, w):
                    print("Global add: {}".format(w))
                def handleJoin(self, store, other):
                    print("Global join: {}".format(other))
                def handleDone(self, store):
                    print("Global done")
            class InnerFrame(job_stream.Frame):
                def handleFirst(self, store, w):
                    print("Frame first: {}".format(w))
                    self.recur("Recur")
                def handleNext(self, store, w):
                    print("Frame next: {}".format(w))
                def handleDone(self, store):
                    print("Frame done")
                    self.emit("(frame end)")
            class ForwardingJob(job_stream.Job):
                def handleWork(self, w):
                    print("Forwarding: {}".format(w))
                    self.emit(w)

            job_stream.work = [ 'Apple' ]
            job_stream.run({
                    'reducer': GlobalReducer,
                    'jobs': [
                        {
                            'frame': InnerFrame,
                            'jobs': [ ForwardingJob ]
                        }
                    ]
            })
            """)
        self.assertEqual(
                "Global init\nFrame first: Apple\nForwarding: Recur\n"
                "Frame next: Recur\nFrame done\nGlobal add: (frame end)\n"
                "Global done\n", r[0])


    def test_emptyFrame(self):
        r = self.executePy("""
            from job_stream.inline import Work

            w = Work([1,2,3])

            @w.frame(emit = lambda store: store.value)
            def start(store, first):
                if not hasattr(store, 'init'):
                    store.init = True
                    return first

            @w.frameEnd
            def end(store, next):
                store.value = next

            for j in w.run():
                print("R{}".format(j))""")
        self.assertLinesEqual("R1\nR2\nR3\n", r[0])


