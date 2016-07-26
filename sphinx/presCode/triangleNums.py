from job_stream.inline import Work, Multiple
with Work(range(5)) as w:
    @w.frame
    def startTriangle(store, first):
        if not hasattr(store, 'value'):
            store.n = first
            store.value = 0
            return Multiple(range(first))

    w.job(lambda x: x+1)

    @w.frameEnd
    def endTriangle(store, x):
        store.value += x

    @w.result
    def printResult(store):
        print("Triangle number {} is {}".format(store.n, store.value))

