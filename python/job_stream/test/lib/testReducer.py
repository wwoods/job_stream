import job_stream
import os

exDir = os.path.join(os.path.dirname(__file__), '../../../../example')

class addOne(job_stream.Job):
    USE_MULTIPROCESSING = False

    def postSetup(self):
        print("addOne setup")


    def handleWork(self, w):
        self.emit(w + 1)


class sum(job_stream.Reducer):
    USE_MULTIPROCESSING = False

    def postSetup(self):
        print("sum setup")


    def handleInit(self, store):
        store.value = 0


    def handleAdd(self, store, work):
        store.value += work


    def handleJoin(self, store, other):
        store.value += other.value


    def handleDone(self, store):
        self.emit(store.value)


if __name__ == '__main__':
    job_stream.work = [ 3, 4 ]
    job_stream.run(os.path.join(exDir, "example1.yaml"))
