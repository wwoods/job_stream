import job_stream
import os

curDir = os.path.dirname(__file__)
_sum = sum

class addOne(job_stream.Job):
    def postSetup(self):
        print "Post setup"

    def handleWork(self, w):
        self.emit(w + 1)


class sum(job_stream.Reducer):
    def handleInit(self, store):
        store.value = 0

    def handleAdd(self, store, work):
        store.value += work

    def handleJoin(self, store, other):
        store.value += other.value

    def handleDone(self, store):
        self.emit(store.value)


class runExperiments(job_stream.Frame):
    def handleFirst(self, store, work):
        store.values = []
        for i in work:
            self.recur(ord(i))

    def handleNext(self, store, work):
        store.values.append(work)

    def handleDone(self, store):
        self.emit(_sum(store.values) / len(store.values))


if __name__ == '__main__':
    # TODO - Transform config into python-readable equivalent.  Is one
    # directional ok?  That is, from the C program's view, python changes would
    # be read-only.
    # TODO - Make programmatic config work:
    config = {
        'jobs': [
            { 'type': addOne },
            { 'type': addOne },
        ]
    }
    job_stream.work.append("abc")
    job_stream.run(os.path.join(curDir, "example5.yaml"))
