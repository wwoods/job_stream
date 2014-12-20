
import job_stream

import sys

class addOneAndQuit(job_stream.Job):
    def handleWork(self, w):
        self._forceCheckpoint(True)
        self.emit(w + 1)


if __name__ == '__main__':
    job_stream.work = [ 1 ]
    job_stream.run({
            'jobs': [
                { 'type': addOneAndQuit },
                { 'type': addOneAndQuit },
                { 'type': addOneAndQuit },
            ]
    }, checkpointFile = sys.argv[-1], checkpointSyncInterval = 0)
