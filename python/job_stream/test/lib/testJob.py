import job_stream
import os

exDir = os.path.join(os.path.dirname(__file__), '../../../../example')

class addOne(job_stream.Job):
    USE_MULTIPROCESSING = False

    def postSetup(self):
        print("addOne setup")


    def handleWork(self, w):
        self.emit(w + 1)


if __name__ == '__main__':
    job_stream.work = [ 1, 2, 4, 8 ]
    job_stream.run(os.path.join(exDir, "adder.yaml"))
