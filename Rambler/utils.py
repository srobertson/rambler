import sys
from types import ClassType, FunctionType
from Rambler import Component
def getClass(classPath):
    """Returns the class represented by dotted notation
    >>> getClass('MyClass.MyClass.MyClass")
    MyClass

    If classPath ends with a "." this method will assume it should
    continue looking for th class by repeating the name to the left of
    the period. This is useful if you create a class in some package,
    module that share the same name
    
    >>> getClass('MyClass.')
    MyClass


    """
    traverse = False
    paths = classPath.split('.')

    if len(paths) == 1:
        try:
            klass = eval(classPath)
            return klass
        except NameError:                
            raise ImportError("Invalid class path %s" % classPath)

    className = paths[-1]
    if className == '':
        traverse = True
        className = paths[-2]

    moduleName = ".".join(paths[:-1])
    module = __import__(moduleName)
    module = sys.modules[moduleName]

    try:
        klass = getattr(module, className)
    except AttributeError:
        raise ImportError("Could not import class %s from module %s" % (className, moduleName))

    if type(klass) not in (type, ClassType, FunctionType, Component):
        raise ImportError("%s is not a valid Class or Type" % classPath)
    else:
        return klass



