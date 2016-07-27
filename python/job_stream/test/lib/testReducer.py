import job_stream.common as common
import os

exDir = os.path.join(os.path.dirname(__file__), '../../../../example')

class addOne(common.Job):
    USE_MULTIPROCESSING = False

    def postSetup(self):
        print("addOne setup")


    def handleWork(self, w):
        self.emit(w + 1)


class sum(common.Reducer):
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
    common.work = [ 3, 4 ]
    common.run(os.path.join(exDir, "example1.yaml"))
