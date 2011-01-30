import thread, threading

# fyi: if you use a class to be the component, then it's a session,
# not a service. A service is a singleton objec

class ThreadStorageService(object):
    # sucks, python2.3 doesn't have threading.local()
    _tss = {}
    _tss_lock = thread.allocate_lock( )
    def _getThreadStorage(klass, threadName=None):
      """ Return a thread-specific storage dictionary. """
      if not threadName:
          threadName = threading.currentThread().getName()

      klass._tss_lock.acquire( )
      try:
            return klass._tss.setdefault(threadName, {  })
      finally:
            klass._tss_lock.release( )

    _getThreadStorage = classmethod(_getThreadStorage)

    def getFromThread(klass, threadName, storageKey):
        # While it's not enforced, it's generally understood that
        # threads calling this function will not manipulate the data
        # they recieve.  You can look, but don't touch!
        tss = klass._getThreadStorage(threadName)

        # Should this use a get and return a default instead of
        # raising a KeyError?  Should it raise some other, custom,
        # error?
        return tss[storageKey]

    getFromThread = classmethod(getFromThread)
    

    # Since all these functions are for a single thread, there's no
    # possibility of another thread coming in and manipulating the
    # same variables so we're cool to manipulate away without worrying
    # about locks.
    def getFromCurrent(klass, storageKey):
        tss = klass._getThreadStorage()
        return tss[storageKey]

    getFromCurrent = classmethod(getFromCurrent)

    def addToCurrent(klass, storageKey, value):
        tss = klass._getThreadStorage()
        tss[storageKey] = value

    addToCurrent = classmethod(addToCurrent)

    def delFromCurrent(klass, storageKey):
        tss = klass._getThreadStorage()
        del tss[storageKey]

    delFromCurrent = classmethod(delFromCurrent)
