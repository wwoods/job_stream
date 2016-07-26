from job_stream.inline import Work
with Work(range(2, 1000)) as w:
    @w.job
    def isPrime(x):
        for y in range(2, int(x ** 0.5)+1):
            if x % y == 0:
                return
        return x

    @w.finish
    def printPrimes(r):
        print("Found {} primes; last 3: {}".format(len(r), sorted(r)[-3:]))

