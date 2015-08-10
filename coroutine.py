#! /usr/bin/python2

from __future__ import print_function, unicode_literals
import functools
import datetime
import weakref
import sys

class Coroutine(object):
    """Wrapper class for coroutines with a self parameter."""
    def __init__(self, func, args, kwargs):
        weak_self = weakref.proxy(self)
        
        self.generator = func(weak_self, *args, **kwargs)
        self.weak_send = lambda *a, **kwa: weak_self.send(*a, **kwa)
        
        print("Coroutine created.")
        next(self.generator)

    def __del__(self):
        print("Coroutine destroyed.")

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.generator)

    next = __next__

    def send(self, *args):
        try:
            arg = args if len(args) > 1 else args[0] if args else None
            return self.generator.send(arg)
        except StopIteration:
            return None


def coroutine(func):
    """Decorator to turn a generator into a Coroutine factory"""
    return lambda *args, **kwargs: Coroutine(func, args, kwargs)


    
        

@coroutine
def G(self, a):
    print("Kezdodik %r: %r" % (self, a))
    yield
    b = yield 1
    print("Folytatodik %r: %r" % (self, b))
    c = yield 2
    print("Befejezodik %r: %r" % (self, c))


def proba():
    g = G(100)
    ws = g.weak_send
    
    cb1 = ws(100)
    cb2 = ws(200)
    #try:
    ws(300)
    #except StopIteration as e:
    #    pass

    print("Hat jo")
    #print("rc=%d" % sys.getrefcount(g))
    #cb1 = None
    #cb2 = None
    #print("rc=%d" % sys.getrefcount(g))
    g = None
    print("Hm?")

#proba()


