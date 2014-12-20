
import job_stream

import sys

class addOneAndQuit(job_stream.Job):
    def handleWork(self, w):
        self._forceCheckpoint(True)
        self.emit(w + 1)


def getToFifty(job_stream.Frame):
    def handleFirst(self, stash, w):
        stash.value = w


    def handleNext(self, stash, w):
        stash.value += w


    def handleDone(self, stash):
        if stash.value < 50:
            self.recur(stash.value / 2)
        else:
            self.emit(stash.value)


if __name__ == '__main__':
    job_stream.work = [ 1 ]
    job_stream.run({
            'jobs': [
                {
                    'frame': getToFifty,
                    'jobs': [
                        { 'type': addOneAndQuit },
                    ]
                },
                { 'type': addOneAndQuit },
                { 'type': addOneAndQuit },
                { 'type': addOneAndQuit },
            ]
    }, checkpointFile = sys.argv[-1], checkpointSyncInterval = 0)
