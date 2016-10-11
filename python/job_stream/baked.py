r"""Pre-baked templates for common distributed operations.
"""

import job_stream.inline as inline
from job_stream.inline import Args, Multiple, Work

import collections
import contextlib
import math
import numpy as np
import pandas as pd
import sys

@contextlib.contextmanager
def sweep(variables={}, trials=0, output=None, trialsParms={},
        showProgress=True):
    """Generic experiment framework; wraps a user's job_stream (realized via
    :mod:`job_stream.inline.Work`.  The wrapped job_stream should take a
    parameter dictionary and return a dictionary of results to track.  The
    numerical statistics of several invocations of the user's code will be
    reported.  For example:

    .. code-block:: python

        from job_stream.baked import sweep
        import numpy as np

        with sweep({ 'a': np.arange(10) }) as w:
            @w.job
            def square(id, trial, a):
                return { 'value': a*a + np.random.random() }

    The above will print out a CSV file (in a legible, table format) that 
    details the mean of ``value`` for ``a = 0, a = 1, ..., a = 9``.  
    Additionally, the standard deviation and expected (95% confidence) error
    of the reported mean will be printed.

    .. note:: While training e.g. a machine learning model, it may be
            desirable to print the model's accuracy at different points
            throughout the training.  To accomplish this, it is recommended
            that the user code remember the accuracy throughout training and
            multiplex the column in the final answer (e.g., ``value_at_0``,
            ``value_at_1000``, etc).

    Args:
        variables (any):
            Any of:

            * :class:`dict`: ``{ 'parameter name': [ 'values to try' ] }``.

              Tries every combination of values for the specified parameters.

              Often, ``values to try`` will come from a function such as
              ``numpy.linspace``, which generates a number of intermediate
              values on a range.

              .. warning:: When more than one parameter is specified, the
                      number of experiments that will run will be the
                      multiplication of the number of values to try
                      for each; that is, all combinations are tried.
                      This will quickly take a long time to run, so be
                      careful.

            * :class:`list`: ``[ { 'parameter name': 'value' } ]``.

              Tries only the specified combinations.  The dictionaries are
              passed as-is.

            Regardless of the type passed, the arguments seen by the user's
            jobs will also include ``id``, a unique identifier for the
            combination of parameters, and ``trial``, the current iteration of
            that combination.

        trials (int):
            The number of times to try each parameter combination.

            If greater than zero, this number is exact.

            If zero, the number will be automatically discerned based on the
            standard deviations of each returned property.  More specifically,
            the algorithm proposed by Driels et al. in 2004, "Determining the
            Number of Iterations for Monte Carlo Simulations of Weapon
            Effectiveness," will be used to guarantee that every mean returned
            will be within 10% of the true value with 95% confidence.

            If less than zero, the same algorithm is used as for a zero value,
            but the number of trials ran will not exceed ``abs(trials)``.

        output (str): If ``None``, results are printed to stdout and the
            program exits.  If anything else, presumed to be the path to a
            CSV file where the results will be dumped.

        trialsParms (dict):
            A dictionary of keys used to configure the auto-detect used to
            determine the number of trials needed.

            E
                The percentage of relative error between the true mean and the
                reported mean.  For instance, if E is 0.1, then there is a 95%
                confidence that the true value is on the range of the estimated
                value * (1. +- 0.1).  Setting to 0 will disable this stopping
                criteria.  Defaults to 0.1.

            eps
                The absolute minimum estimated error.  Setting to 0 will
                disable this stopping criteria.  Defaults to 1e-2.

            min
                The minimum number of trials to run.  Defaults to 3, as this is
                usually enough to get a stable idea of the variable's standard
                deviation.

            max
                The maximum number of trials to run.  Defaults to 10000.  May
                also be specified by setting the argument ``trials`` to a
                negative number.

        showProgress (bool): If True (default), then print out progress
                indicators to stderr as work is completed.

    Return:
        Nothing is returned.  However, both stdout and, if specified, the csv
        indicated by ``output`` will have a grid with all trial ids,
        parameters, and results printed.  Each result column will represent
        the mean value; additionally, the estimated standard deviation and the
        95% confidence error of the reported mean will be printed.
    """
    if not isinstance(variables, (list, dict)):
        raise ValueError("variables must be list or dict: {}".format(
                variables))
    if not isinstance(trials, int):
        raise ValueError("trials must be int")
    if output is not None and not isinstance(output, str):
        raise ValueError("output must be str or None")
    if not isinstance(trialsParms, dict):
        raise ValueError("trialsParms must be a dict")

    trialsParmsDefaults = { 'E': 0.1, 'eps': 1e-2, 'min': 3, 'max': 10000 }
    for k, v in trialsParms.items():
        if k not in trialsParmsDefaults:
            raise ValueError("Bad trialsParms key: {}".format(k))

        trialsParmsDefaults[k] = v
        if k == 'min' and trials > 0:
            raise ValueError("trialsParms['min'] cannot be specified with "
                    "trials > 0, as that disables trial count detection.")
        elif k == 'max' and trials != 0:
            raise ValueError("trialsParms['max'] cannot be specified with "
                    "trials != 0.  Use a negative trials number instead.")
    trialsParmsDefaults.update(trialsParms)
    if trialsParmsDefaults['E'] <= 0. and trialsParmsDefaults['eps'] <= 0.:
        raise ValueError("Both E and eps cannot be zero in trialsParms")
    if trialsParmsDefaults['min'] > trialsParmsDefaults['max']:
        raise ValueError("min cannot be greater than max: {} / {}".format(
                trialsParmsDefaults['min'], trialsParmsDefaults['max']))

    allParms = set()
    nExperiments = 0
    if isinstance(variables, list):
        nExperiments = len(variables)
        for v in variables:
            if not isinstance(v, dict):
                raise ValueError("If a list, variables must contain dicts")
            if len(allParms) == 0:
                allParms.update(v.keys())
            else:
                if allParms != set(v.keys()):
                    raise ValueError("All parameter dictionaries must have "
                            "same keys; found {} against {}".format(allParms,
                                set(v.keys())))
            for k in v:
                allParms.add(k)
    elif isinstance(variables, dict):
        nExperiments = 1
        for k, v in variables.items():
            allParms.add(k)
            nExperiments *= len(v)
    else:
        raise NotImplementedError(variables)

    nTrialsMin = trialsParmsDefaults['min']
    nTrialsMax = trialsParmsDefaults['max']
    if trials != 0:
        nTrialsMax = abs(trials)
        if trials > 0:
            nTrialsMin = trials
        else:
            nTrialsMin = min(nTrialsMin, abs(trials))

    with Work([1]) as w:
        def _getNextParams(store):
            """Returns the next parameter combination that needs to be tried.
            """
            if isinstance(variables, list):
                if not hasattr(store, 'i'):
                    store.i = 0
                if store.i == len(variables):
                    return
                store.i += 1
                v = variables[store.i - 1]
                if not isinstance(v, dict):
                    raise ValueError("Parameter was not dict? {}".format(v))
            elif isinstance(variables, dict):
                if not hasattr(store, 'stack'):
                    store.keys = list(variables.keys())
                    store.stack = { name: 0 for name in store.keys }
                    store.carry = 0

                if store.carry != 0:
                    # All set, no more combinations to try
                    return

                # Load up next set
                v = {}
                for name, vals in variables.items():
                    v[name] = vals[store.stack[name]]

                # Increment and cascade
                store.carry = 1
                for k in store.keys:
                    if store.carry == 0:
                        break
                    store.stack[k] += 1
                    if store.stack[k] >= len(variables[k]):
                        store.stack[k] = 0
                        store.carry = 1
                    else:
                        store.carry = 0
            else:
                raise NotImplementedError(variables)

            if showProgress:
                sys.stderr.write("job_stream.baked: Starting {} of {} with {} "
                        "trials\n".format(store.id, nExperiments-1,
                            store.actualMin))

            # v is dict with parameters at the moment; give it an ID
            v = v.copy()
            v['id'] = store.id
            store.id += 1
            return (v, store.actualMin)


        def _getZc(n):
            """For a given number of trials n, returns z_c from Driels et al.
            and whether or not there should be an extra trial due to
            uncertainty.
            """
            # An extra trial is required for low counts, due to the fact
            # that there is higher variance in the calculated deviation.
            extra = 1

            vFree = n - 1
            zc = 1.96
            if vFree > 15:
                # Normal distribution, and enough that we do not need to
                # have an extra trial.
                extra = 0
            elif vFree >= 10:
                # Here and below is a t-distribution; note that this comes
                # from the 97.5% column in Table 3 of Driels et al., since
                # those coefficients don't include the tail
                zc = 2.23
            elif vFree >= 5:
                zc = 2.57
            elif vFree >= 4:
                zc = 2.78
            elif vFree >= 3:
                zc = 3.18
            elif vFree >= 2:
                zc = 4.30
            elif vFree >= 1:
                zc = 12.71
            return zc, extra


        @w.frame
        def spinUpParms(store, first):
            if not hasattr(store, 'init'):
                store.init = True
                store.id = 0
                store.results = []

                initial = []
                # Start as many experiment combinations as we have CPUs for,
                # with one extra to keep things busy
                nCpu = inline.getCpuCount()

                # Calculate the actual minimum number of trials to run
                store.actualMin = min(nTrialsMax, max(nTrialsMin,
                        (nCpu // nExperiments)))

                while len(initial) < (nCpu // store.actualMin) + 1:
                    n = _getNextParams(store)
                    if n is None:
                        break
                    initial.append(n)
                return Multiple(initial)

            # Get here, we're done.  Print results, optionally put in csv
            otherCols = { 'id', 'trials' }
            otherCols.update(allParms)
            valCols = set()
            for r in store.results:
                for k in r:
                    if k in otherCols or k in valCols:
                        continue
                    valCols.add(k)
            cols = [ 'id', 'trials' ] + sorted(allParms) + sorted(valCols)
            df = pd.DataFrame(store.results, columns=cols)
            df.set_index('id', inplace=True)
            print(df.to_string())
            if output is not None:
                df.to_csv(output)

        @w.frame(emit=lambda s: s.result)
        def spinUpTrials(store, first):
            if not hasattr(store, 'init'):
                store.init = True
                store.parms = first[0]
                store.trialDone = 0
                store.trialNext = first[1]
                store.resultAvg = collections.defaultdict(float)
                store.resultVar = collections.defaultdict(float)
                return Multiple([ Args(trial=i, **store.parms)
                        for i in range(store.trialNext) ])

            # If we get here, this trial is done; convert variances to
            # deviations
            devs = { k: v ** 0.5 for k, v in store.resultVar.items() }
            store.result = store.parms
            for k, v in [ ('trials', store.trialDone) ]:
                if k in store.result:
                    raise ValueError("Duplicate key: {}".format(k))
                store.result[k] = v
            for k, v in store.resultAvg.items():
                store.result[k] = v
                devk = '{}_dev'.format(k)
                if devk in store.result:
                    raise ValueError("Duplicate key: {}".format(devk))
                store.result[devk] = devs[k]

                # Calculate error region with 95% confidence
                n = store.trialDone
                zc, _ = _getZc(n)
                errk = '{}_err'.format(k)
                if errk in store.result:
                    raise ValueError("Duplicate key: {}".format(errk))
                store.result[errk] = zc * devs[k] / n ** 0.5

        yield w

        @w.frameEnd
        def spinDownTrials(store, result):
            if (not isinstance(result, dict)
                    or store.trialDone + 1 > store.trialNext):
                raise ValueError("Result from sweep()'s pipeline must be a "
                        "single dict (cannot emit Multiple either)")

            store.trialDone += 1
            n = store.trialDone

            avgs = store.resultAvg
            devs = store.resultVar

            # Recurse through each result; all keys must be present in each
            # result
            for k in avgs.keys():
                if k not in result:
                    raise ValueError("Key {} was not in a result".format(k))

            for k, v in result.items():
                if k in store.parms:
                    raise ValueError("Duplicate result key {} found in "
                            "parameters as well".format(k))
                if n != 1 and k not in avgs:
                    raise ValueError("Key {} was not in a result".format(k))

                # Valid for n == 1
                oldAvg = avgs[k]
                oldDev = devs[k] if not np.isnan(devs[k]) else 0.
                newAvg = oldAvg + (v - oldAvg) / n
                avgs[k] = newAvg
                if np.isnan(newAvg):
                    devs[k] = newAvg
                else:
                    devs[k] = max(0., oldDev + oldAvg ** 2 - newAvg ** 2 + (
                            v ** 2 - oldDev - oldAvg ** 2) / n)

            numToSpawn = 0
            if store.trialNext >= nTrialsMax:
                # Nothing more to run
                pass

            totalNeeded = nTrialsMin
            if n == 1:
                # Not enough information to predict the number of trials, so
                # do not propagate more unless only one was propagated in the
                # first place.
                if store.trialNext != 1 or nTrialsMax == 1:
                    # More than one propagated
                    numToSpawn = 0
                else:
                    # We need another to get statistical significance
                    numToSpawn = 1
            else:
                # Determine if more results are needed... remember, we're
                # looking for 95% confidence that the true mean is +- E% of the
                # Number of free variables is the same as the number of samples
                # minus one
                zc, extra = _getZc(n)

                numNeeded = 0
                # Find the most needed
                for k in avgs.keys():
                    avg = avgs[k]
                    if np.isnan(avg):
                        continue
                    dev = devs[k] ** 0.5
                    err = trialsParmsDefaults['E'] * abs(avg)
                    err = max(err, trialsParmsDefaults['eps'])
                    need = zc * dev / err
                    numNeeded = max(numNeeded,
                            int(math.ceil(need ** 2 + extra)))

                totalNeeded = numNeeded
                # Spawn a number equal to num needed minus num already spawned
                numNeeded = numNeeded - store.trialNext
                # Spawn at most two more per completed... this is quite quick
                # growth but will be bounded by the absolute number needed.
                numToSpawn = min(2, max(0, numNeeded))
                numToSpawn = max(0, min(
                        store.trialDone - store.trialNext
                            + inline.getCpuCount(),
                        nTrialsMax - store.trialNext,
                        numToSpawn))

            trialOld = store.trialNext
            store.trialNext += numToSpawn
            if showProgress:
                sys.stderr.write("job_stream.baked: {} done with trial "
                        "{}{}{}\n".format(
                            store.parms['id'], n-1,
                            # Information on new trials started
                            "; starting {}:{}".format(trialOld,
                                store.trialNext) if numToSpawn else "",
                            # Information on needed trials
                            "; estimated {} needed".format(
                                totalNeeded)
                                if numToSpawn or n == store.trialNext
                                else ""))

            return Multiple([
                    Args(trial=store.trialNext - 1 - i, **store.parms)
                    for i in range(numToSpawn) ])

        @w.frameEnd
        def spinDownParms(store, parms):
            store.results.append(parms)
            return _getNextParams(store)

