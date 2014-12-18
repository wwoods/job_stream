import job_stream
import os

exDir = os.path.join(os.path.dirname(__file__), '../../../../example')

class addOne(job_stream.Job):
    def postSetup(self):
        print("addOne setup")


    def handleWork(self, w):
        self.emit(w + 1)


class runExperiments(job_stream.Frame):
    def postSetup(self):
        print("runExperiments setup")


    def handleFirst(self, store, work):
        store.values = []
        for i in work:
            self.recur(ord(i))


    def handleNext(self, store, work):
        store.values.append(work)


    def handleDone(self, store):
        self.emit(sum(store.values) / len(store.values))


if __name__ == '__main__':
    job_stream.work.append("aec")
    job_stream.run(os.path.join(exDir, "example5.yaml"))
