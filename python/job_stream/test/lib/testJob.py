import job_stream.common as common
import os

exDir = os.path.join(os.path.dirname(__file__), '../../../../example')

class addOne(common.Job):
    USE_MULTIPROCESSING = False

    def postSetup(self):
        print("addOne setup")


    def handleWork(self, w):
        self.emit(w + 1)


if __name__ == '__main__':
    common.work = [ 1, 2, 4, 8 ]
    common.run(os.path.join(exDir, "adder.yaml"))
