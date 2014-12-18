import job_stream
import os

curDir = os.path.dirname(__file__)

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
    for i in range(3):
        job_stream.work.append(i)
    job_stream.run(os.path.join(curDir, "example1.yaml"))
