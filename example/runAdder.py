import job_stream
import os

curDir = os.path.dirname(__file__)

class addOne(job_stream.Job):
    def handleWork(self, w):
        w = int(w)
        self.emit(w + 1)

if __name__ == '__main__':
    job_stream.run(os.path.join(curDir, "adder.yaml"))
