
import job_stream.common as common

import sys

class addOneAndQuit(common.Job):
    def handleWork(self, w):
        self._forceCheckpoint(True)
        self.emit(w + 1)


if __name__ == '__main__':
    common.work = [ 1 ]
    common.run({
            'jobs': [
                { 'type': addOneAndQuit },
                { 'type': addOneAndQuit },
                { 'type': addOneAndQuit },
            ]
    }, checkpointFile = sys.argv[-1], checkpointSyncInterval = 0)
