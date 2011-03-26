import time
import sys
import numpy

import paisley

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, waitForDeferred

def benchmark(times, timer=time.time, timeStore=None, progressDest=sys.stdout):
    def _decorator(f):
        def _decorated(*args, **kwargs):
            for x in xrange(times):
                startTime=timer()
                result = yield f(*args, **kwargs)
                timeStore.setdefault(f.__name__, []).append(timer()-startTime)

                if x%(times*.10) == 0.0:
                    progressDest.write('.')
                    progressDest.flush()
            progressDest.write('\n')

        _decorated.__name__ = f.__name__

        return inlineCallbacks(_decorated)

    return _decorator

RUN_TIMES = 1000
TIMES = {}

benchmarkDecorator = benchmark(RUN_TIMES, timeStore=TIMES)


@benchmarkDecorator
def bench_saveDoc(server):
    d = server.saveDoc('benchmarks', """
        {
            "Subject":"I like Planktion",
            "Author":"Rusty",
            "PostedDate":"2006-08-15T17:30:12-04:00",
            "Tags":["plankton", "baseball", "decisions"],
            "Body":"I decided today that I don't like baseball. I like plankton."
        }
""")
    return d


@inlineCallbacks
def run_tests(server):
    for bench in [bench_saveDoc]:
        print "benchmarking %s" % (bench.__name__,)
        result = yield bench(server).addCallback(_printCb)
        print "    avg: %r" % (
            sum(TIMES[bench.__name__])/len(TIMES[bench.__name__]),)
        print "    std: %r" % (
            numpy.std(TIMES[bench.__name__]),)
        print "    min: %r" % (
            min(TIMES[bench.__name__]),)
        print "    max: %r" % (
            max(TIMES[bench.__name__]),)
        print "  total: %r" % (
            sum(TIMES[bench.__name__]),)


def run():
    s = paisley.CouchDB('localhost')
    d = s.createDB('benchmarks')
    d.addBoth(_printCb)
    d.addCallback(lambda _: run_tests(s))

    return d


def _printCb(msg):
    if msg is not None:
        print msg


if __name__ == '__main__':
    def _run():
        d = run()
        d.addBoth(_printCb)
        d.addBoth(lambda _: reactor.stop())

    reactor.callWhenRunning(_run)
    reactor.run()
