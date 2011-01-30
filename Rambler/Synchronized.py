"""Provides Java's like synchronization for instances and classes."""

from threading import RLock


class synchronized(object):
    def __init__(self, method):
        self._method = method
        
    def __get__(self, obj, klass):

        if  "__lock__" not in obj.__dict__:
            obj.__lock__ = RLock()

        return synchronizedmethod(obj, self._method)

class staticsynchronized(object):
    def __init__(self, method):
        self._method = method

    def __get__(self, obj, klass):
        if not hasattr(klass, "__lock__"):
            klass.__lock__ = RLock()

        return staticsynchronizedmethod(klass,self._method)

class classmethodsynchronized(object):
    def __init__(self, method):
        self._method = method

    def __get__(self, obj, klass):
        if not hasattr(klass, "__lock__"):
            klass.__lock__ = RLock()

        return classsmethodsynchronizedmethod(klass,self._method)




class synchronizedmethod(object):
    def __init__(self,instance, method):
        self._method = method
        self._instance = instance

    def __call__(self, *args, **kw):
        try:
            self._instance.__lock__.acquire()
            return apply(self._method, (self._instance, ) + args, kw)
        finally:
            self._instance.__lock__.release()

class staticsynchronizedmethod(object):
    def __init__(self,klass, method):
        self._method = method
        self._klass = klass

    def __call__(self, *args, **kw):
        try:
            self._klass.__lock__.acquire()
            return apply(self._method,  args, kw)
        finally:
            self._klass.__lock__.release()

# dumb name I know.. it's like the ATM machine
class classsmethodsynchronizedmethod(object):
    def __init__(self,klass, method):
        self._method = method
        self._klass = klass

    def __call__(self, *args, **kw):
        try:
            self._klass.__lock__.acquire()
            return apply(self._method,  (self._klass, ) + args, kw)
        finally:
            self._klass.__lock__.release()
        
        



if __name__ == "__main__":
    class O(object):
        def foo(self):
            print "Ellloooo"
        foo = synchronized(foo)

        def bar():
            print "Right back at you!"
        bar = staticsynchronized(bar)

        

    o = O()
    o.foo()
    o.bar()
    O.bar()
