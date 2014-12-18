
import job_stream

class addTwo(job_stream.Job):
    def handleWork(self, w):
        self.emit(w + 2)


class multiply(job_stream.Reducer):
    def handleInit(self, stash):
        stash.value = 1

    def handleAdd(self, stash, work):
        stash.value *= work

    def handleJoin(self, stash, other):
        stash.value *= other.value

    def handleDone(self, stash):
        self.emit(stash.value)


if __name__ == '__main__':
    job_stream.work = [ 1, 5 ]
    # Adds 4 and multiplies numbers; so 5 * 9 = 45
    job_stream.run({
            'reducer': multiply,
            'jobs': [
                { 'type': addTwo }, { 'type': 'addTwo' }
            ] })
