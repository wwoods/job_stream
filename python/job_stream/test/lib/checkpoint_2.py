
import job_stream

import sys

class addOneAndQuit(job_stream.Job):
    def handleWork(self, w):
        self._forceCheckpoint(True)
        self.emit(w + 1)


class getToFifty(job_stream.Frame):
    CAP = 50

    def handleFirst(self, stash, w):
        stash.value = w


    def handleNext(self, stash, w):
        stash.value += w


    def handleDone(self, stash):
        if stash.value < self.CAP:
            self.recur(stash.value // 2)
        else:
            self.emit(stash.value)


class getToHundred(getToFifty):
    CAP = 100


if __name__ == '__main__':
    # 1 + 1 = 2 + 2 = 4 + 3 = 7 + 4 = 11 + 6 = 17 + 9 = 26 + 14 = 40 + 21 = 61
    # 61 + 1 = 62 + 32 = 94 + 48 = 142 + 1 = 143
    # 5 is the only one that doesn't match this, so:
    # 5 + 3 = 8 + 5 = 13 + 7 = 20 + 11 = 31 + 16 = 47 + 24 = 71
    # 71 + 1 = 72 + 37 = 109 + 1 = 110
    # So, 3 143s and 1 110 are expected.
    job_stream.work = [ 1, 2, 3, 4 ]
    job_stream.run({
            'jobs': [
                {
                    'frame': getToFifty,
                    'jobs': [
                        { 'type': addOneAndQuit },
                    ]
                },
                { 'type': addOneAndQuit },
                {
                    'frame': getToHundred,
                    'jobs': [
                        { 'type': addOneAndQuit },
                    ]
                },
                { 'type': addOneAndQuit },
            ]
    }, checkpointFile = sys.argv[-1], checkpointSyncInterval = 0)
