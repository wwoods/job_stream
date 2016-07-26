import random
def failsSometimes(x):
    if random.random() < 0.002:
        raise ValueError("Random failure")
demoKwargs = { 'checkpointSyncInterval': 0., 'checkpointInterval': 1e-6 }

from job_stream.inline import Work
with Work(range(2, 1000), checkpointFile="example.chkpt", **demoKwargs) as w:
    @w.job
    def isPrime(x):
        failsSometimes(x)  # Fails 0.2% of the time
        for y in range(2, int(x ** 0.5)+1):
            if x % y == 0:
                return
        return x

    @w.finish
    def printPrimes(r):
        print("Found {} primes; last 3: {}".format(len(r), sorted(r)[-3:]))

