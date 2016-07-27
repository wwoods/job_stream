
import job_stream.common as common

class addTwo(common.Job):
    def handleWork(self, w):
        self.emit(w + 2)


class multiply(common.Reducer):
    def handleInit(self, stash):
        stash.value = 1

    def handleAdd(self, stash, work):
        stash.value *= work

    def handleJoin(self, stash, other):
        stash.value *= other.value

    def handleDone(self, stash):
        self.emit(stash.value)


if __name__ == '__main__':
    common.work = [ 1, 5 ]
    # Adds 4 and multiplies numbers; so 5 * 9 = 45
    common.run({
            'reducer': multiply,
            'jobs': [
                { 'type': addTwo }, { 'type': 'addTwo' }
            ] })
