import sys
from types import ClassType
import gc
import pprint
import linecache

try:
    # in order to debug frames you need to have the threadframe module
    # installed
    
    from  threadframe import dict as getframes
    
except ImportError:
    def getframes():
        return {}


class DebugService(object):
    #interface = "ciIDebugService"

    #__epo_name__ = "DebugService"
    
    """Provides debug information about a running application."""

    def refcount(self, limit, types=(type, ClassType)):
        """Returns a list of refcount by object type"""
        dict={}
        for m in sys.modules.values():
            for sym in dir(m):
                ob=getattr(m, sym)
                if type(ob) in types:
                    dict[ob]=sys.getrefcount(ob)
        pairs=[]
        append=pairs.append
        for ob, v in dict.items():
            if hasattr(ob, '__module__'):
                name='%s.%s' % (ob.__module__, ob.__name__)
            else: name='%s' % ob.__name__
            append((v, name))
        pairs.sort()
        pairs.reverse()
        if limit != 0: 
            pairs=pairs[:limit]

        ret = []
        for count, name in pairs:
            ret.append("%s\t%s" % (count, name))
        return "\n".join(ret)



    def get_referrers(self, name):
        # Look up the object via sys

        path = name.split(".")
        module = ".".join(path[:-1])
        obj = path[-1]

        if sys.modules.has_key(name):
            obj = sys.modules[name]
        else:
            module = sys.modules[module]
            obj = getattr(module, obj)

        return pprint.pformat(gc.get_referrers(obj))

    def getThreadStates(self):
        states = []

        frames = getframes()
        for k, f in frames.items():
            if f:
                states.append("Thread %s: file %s<%s>" %
                              (k, f.f_code.co_filename, f.f_lineno))

        return "\n".join(states)

    def getThreadStackTrace(self, threadId):
        f = getframes().get(threadId)
        trace = []
        while f != None:
            filename = f.f_code.co_filename
            lineno = f.f_lineno
            name =  f.f_code.co_name
            line = linecache.getline(filename, lineno)
            trace.append('File "%s", line %s, in %s\n\t %s' %
                         (filename, lineno, name, line.strip()))
            f = f.f_back
        trace.reverse()
        return "\n".join(trace)

