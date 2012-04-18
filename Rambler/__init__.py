import os, sys,re
import itertools
flatten = itertools.chain.from_iterable

from hashlib import md5
# import these, here so other Rambler components can do a from Rambler import Interface, etc...
from zope.interface import Interface, Attribute, implements, classProvides

from datetime import datetime
from dateutil.relativedelta import relativedelta
from types import NoneType

class nil(object):
  """Singelton that provides the nil interface. The nil interface supports
  any method name with any combination of arguments. The result of calling
  the method is always nil. 
  
  >>> nil.my_fake_method()
  nil
  >>> nil.yet_anoth_method_that_does_not_exist()
  nil
  """
  __singelton = None
  def __new__(cls):
    # always returns the same instance of nil. Their can be only one!
    if cls.__singelton is None:
      cls.__singelton=super(cls, cls).__new__(cls)
    return cls.__singelton
    
  def __getattr__(self, name):
    return self
  def __getitem__(self, key):
    return self
    
  def __call__(self, *arg, **kw):
    return self
    
  def __iter__(self):
    return self
    
  def next(self):
    raise StopIteration

  def __repr__(self):
    return 'nil'
    
  def __add__(self, other):
    return other
  __radd__ = __add__
  
  def __nonzero__(self):
    return False
  
  def __sub__(self, other):
    t = type(other)
    if t in (int,long,float,complex):
      return -other
    else:
      raise TypeError, "unsupported operand type(s) for -: %s %s" % (nil, t)
      
  def __rsub__(self, other):
    return self.__sub__(-other)
  
  def __div__(self, other):
    t = type(other)
    if t in (int,long,float,complex):
      return t(0)
    else:
      raise TypeError, "unsupported operand type(s) for /: %s %s" % (nil, t)
      
nil = nil()

class outlet(object):
  """Provides an outlet for one component to be plugged into another component. """

  def __init__(self, compName, interface=None, allowUnassembled=False, missing=None, callback=None):
    self.compName = compName
    self.interface = interface
    self.component = None
    self.allowUnassembled = allowUnassembled
    self.missing = missing
    self.callbacks=[]
    if callback:
      self.callbacks.append(callback)

  def __get__(self, obj, klass=None):
    if self.component is not None:
      comp = self.component
    else:
      comp = self.missing
      
    if comp is None:
      raise AttributeError("%s has not been bound to object: %s  class: %s" % (self.compName, obj, klass))
    else:
       return comp

  def __set__(self, obj, component):
    self.component = component
    if component:
      for callback in self.callbacks:
        # notify the callback that the object got the component it was
        # waiting for. Note this method is really only to support the
        # descriptors in this file like "option"
        callback(obj, component)
    else:
      # component is being unbound
      pass


class error(object):
  """Provides components with an easyway of defining errors."""

  # would be cool if I could figure out how to automatically add an
  # outlet for the error factory here. I suppose this could be done
  # in the component binder
    
  def __init__(self, errorCode, domain=None, description="", reason="", suggestion=""):
    self.errorCode = errorCode
    self.domain = domain
    self.userInfo={'error.description': description,
                   'error.reason': reason,
                   'error.suggestion': suggestion}

  def __get__(self, obj, klass=None):
    userInfo = self.userInfo.copy()

    if self.domain is not None:
      # No domain set
      newError = obj.errorFactory.newErrorWithDomain(self.domain, self.errorcode, userInfo)
    else:
      newError = obj.errorFactory.newError(self.errorCode, userInfo)

    return newError

class field(object):
  is_relation  = False
  def __init__(self, type_or_default, fget=None, fset=None, default=None):
    self.name = None # Set the first time Entity.fields() is called
    self.fget = fget
    self.fset = fset
      
    self.type = type(type_or_default)
    if self.type is type:
      # it's not a default variable
      self.type = type_or_default
    else:
      default = type_or_default
      
    self.default = default
    
  def __call__(self, fget):
    """Syntactic sugar for defining a getter using a descriptor see setter for example"""
    self.fget = fget
    return self
    
  def setter(self, fset):
    """Syntactic sugar for defining a setter using a descriptor
    
    Example:
    
    class Foo(Entity):
      name field(str)
      
      @name.setter
      def set_name(self, value):
        ... do something with value ..
        self['name'] = value
    """
    self.fset = fset
    #return fset
    return self
    
  def attr_name(self, cls):
    # Return the attribute name the field is bound to
    if self.name is None:
      # Entity.fields()  sets the field name for all fields of the entity the first
      # time it's called
      cls.fields()
    return self.name
    
  def __get__(self, instance, cls):
    if instance is None:
        # Someone's getting the attribute directly off the class
        return self
        
    if self.fget:
      return self.fget(instance)
      
    try:
      return instance.attr[self.attr_name(cls)]
    except KeyError:
      if callable(self.default):
        instance.attr[self.attr_name(cls)] = self.default()
      else:
        instance.attr[self.attr_name(cls)] = self.default
      return instance.attr[self.attr_name(cls)]

  def __set__(self, obj, value):
    # TODO: Might need away to specify that a field allows none
    name = self.attr_name(obj.__class__)
          
    if self.fset:
      return self.fset(obj, value)


    if value is not None and not isinstance(value, self.type):
      try:
        value = self.type(value)
      except Exception,e:
        raise TypeError, "Expecting %s for %s" % (self.type, name)

    obj.attr[name] = value



def annotateCurrentClass(method,*args,**kw):
    """Assuming this function was executed at the class scope, this method
    will lookup the given class out of the stack and call the method
    with the current class as the first argument.

    Raises an error if this method was called outside of the class scope.

    Typically this function is used when you want to do some meta
    programing. Say for instance that you want to declare that your
    class subscribes to a given event.

    To use this properly you need to create an an anotation callback which
    is the method that will be passed to annotateCurrentClass

    >>> def anotateEvent(classObject, topic):
    ...   if not has_attr(classObject,'__event_subscriptions__')
    ...      classObject.__event_subscriptions__ = set()
    ...   classObject.__event_subscriptions__.add(topic)
    
    Then create your annotation method. This method is used to
    actually anotate what ever class you are defining. In this example
    we create a method which states that a given class should
    subscribe to a given event topic.

    >>> def subscribe(topic):
    ...   annotateCurrentClass(annotateEvent, topic)
    
    Now we can declare that Foo hears the shutdown event, which
    should add the __event_subscriptions__ attribute to our Foo
    class for us.

    >>> class Foo:
    ...   subscribe('shutdown')
    
    >>> 'shutdown' in Foo.__event_subscriptions__
    True
    """
    frame = sys._getframe(2)
    locals = frame.f_locals

    # Try to make sure we were called from a class def. In 2.2.0 we can't
    # check for __module__ since it doesn't seem to be added to the locals
    # until later on.
    if (locals is frame.f_globals) or (
        ('__module__' not in locals) and sys.version_info[:3] > (2, 2, 0)):
        raise TypeError("annontateCurrentClass can be used only from a class definition.")

    method(locals, *args, **kw)

  
def coroutine(func):
  """Decorator for declaring asynch functions.

  Automatically adds a dependency to the scheduler to your component. Any invocation made
  to the method will be scheduled using scheduler.new()
  """

  frame = sys._getframe(1)
  locals = frame.f_locals
  try:
    if (locals is frame.f_globals) or (
      ('__module__' not in locals) and sys.version_info[:3] > (2, 2, 0)):
      raise TypeError("option can be used only from a class definition.")

    if 'scheduler' not in locals:
      locals['scheduler'] = outlet('Scheduler')
  finally:
    del frame
    del locals

  def start(*args,**kw):
    scheduler = args[0].scheduler
    return scheduler.new(func(*args,**kw), getattr(coroutine,'context', None))
  return start




class option(object):
  """Descriptor for accessing options. 
    
  Usage: option(section, option, default)
  """

  # None could be a valid default value, so we look explicitly for
  # this value to determine if no default was set.

  _noDefaultMarker = ()
  last_warning  = None
  warn_interval = relativedelta(minutes=5)
    
  # TODO: get rid of section
  def __init__(self, section, option, default=_noDefaultMarker):
    if option:
      self.option  = ("%s.%s") % (section,option)
    else:
      self.option = section
    self.default = default
      
    # ensure the class has a ConfigService outlet
    frame = sys._getframe(1)
    locals = frame.f_locals

    # Try to make sure we were called from a class def. In 2.2.0 we can't
    # check for __module__ since it doesn't seem to be added to the locals
    # until later on.
    if (locals is frame.f_globals) or (
        ('__module__' not in locals) and sys.version_info[:3] > (2, 2, 0)):
        raise TypeError("option can be used only from a class definition.")

    if 'configService' not in locals:
      locals['configService'] = outlet('ConfigService',callback=self.configServiceBound)
    else:
      locals['configService'].callbacks.append(self.configServiceBound)
      
    del frame
    del locals
    
  
  def __call__(self, func):
    """Objects can use options as decorators to receive notifications when
    an option has changed. 
    
    Example:
    
    class Foo(object):
      @option('http_server.port', 10)
      def port(self, new_val):
        # do something with the new port number
    """
    
    self.option_changed = func
    return self
      
  def __get__(self, instance, cls=None):
    obj = instance or cls
    return obj.configService.get(self.option)
    
    if self.default is self._noDefaultMarker:
      return obj.configService.get(self.option)
    else:
      try:
        return obj.configService.get(self.option, self.default)
      except AttributeError:
        # Missing the configService, could legitamatly happen during testing..
        # so warn ocassinally
        ocls = self.__class__ 
        t = datetime.now()
        if ocls.last_warning is None or (t > (ocls.last_warning + ocls.warn_interval)):
          ocls.last_warning = t
          print "Warning ConfigService not bound to %s" % obj
        return self.default
    
  def configServiceBound(self, obj, configService):
    if self.default is not self._noDefaultMarker:
      configService.set_default(self.option, self.default)

class Bundle(object):
  def __init__(self, bundlePath):    
    sys.path.append(os.path.dirname(os.path.abspath(bundlePath)))
    self.path = bundlePath
    self.digests = {}

  def pathForResource(self, resource):
    """Returns the absolute path to a resource (aka file) based on the appBundle's directory.

    For example:
    if your application was in /etc/rambler.d/myapp
    >> app.pathForResurce('/app.info')
    '/etc/rambler.d/myapp/app.info'

    Note preceding slashes are ignored leaving it out like in the
    next example works as well.
    
    >> app.pathForResurce('app.info')
    '/etc/rambler.d/myapp/app.info'

    Also note, though not currently implemented, in the future if
    we run Rambler on a different platform say windwos you should
    still use unix file seperators, this method will be updated to
    return the correct path for you. It should work something like
    this

    >> app.pathForResource('app.info')
    'c:\\Program Files\\Rambler\\apps\\myapp\\app.info'
    
    """
    return os.path.abspath(os.path.join(self.path, resource))

  def resourceExists(self, resource):
    """Returns true if the give resource exits in the application bundle."""
    return os.path.exists(self.pathForResource(resource))

  def resourceChanged(self, resource):
    """Given a resource calculate it's md5 and return true if it
    is different from the last time this method was called. If
    this is the first time this method is called for this resouce
    it will return true

    """

    md5sum = md5.md5()
    path = self.pathForResource(resource)
    # get the previous digest if it exits
    oldDigest = self.digests.get(path, "")

    try:
      resourceFile = open(path)
    except IOError:
      return Application.CONFIG_MISSING
    
    for line in resourceFile:
      md5sum.update(line)
    resourceFile.close()

    digest = md5sum.digest()
    
    if digest == oldDigest:
      return False
    else:
      self.digests[path] = digest
      return True

        
def load_classes(module_or_package_name, base_class):
  '''Given a module or package name and a base_class, return all classes found directly in the module.
  If a package name is given, the system will recurse the package loading each module that was found under 
  the package.
  '''

  try:          
    __import__(module_or_package_name)
    module = sys.modules[module_or_package_name]
    if hasattr(module,'__path__'):
      modules = recurse_package(module)
    else:
      modules = [module]
  except ImportError,e:
    #TODO: This method needs access to the logger
    if not e.args[0].startswith('No module named'):
      # blab about import errors other than no module named
      print "Error importing %s: %s" % (module_or_package_name, e)
    return []
  except ValueError:
    pass

  classes = set()
  for module in modules:
    for name, obj in module.__dict__.items():
      if (issubclass(type(obj), type) and issubclass(obj, base_class) 
           and obj.__module__ == module.__name__):
           
          classes.add(obj)

  return classes

is_python = re.compile('^(?!__)(.*)\.py.?$')
def recurse_package(package):
  # TODO: optimize me, looks like we're loading both pyc and py files, the sets filter it out
  # but it might save some load time

  yield package
  
  for root, dirs, files in os.walk(package.__path__[0]):
    x_root = root[len(package.__path__[0]) + 1:].split(os.path.sep)

    for f_name,ignore in  itertools.groupby(sorted(flatten(filter(None, map(is_python.findall, files))))):
      parts = [package.__name__]
      parts.extend(x_root)
      parts.append(f_name)
      mod_name = '.'.join(filter(None,parts))

      if mod_name not in sys.modules:
        try:
          __import__(mod_name)
        except ImportError,e:
          #todo: might be nice to log.debug this error here
          #if not e.message.startswith('No module named'):
          # blab about import errors other than no module named
          print "Error importing %s: %s" % (mod_name, e)
            
          continue
      yield sys.modules[mod_name]

#
class Component(type):
  compReg = outlet('ComponentRegistry')
  
  classes = {}
  
  def __new__(cls, name, bases, dct):
    if name in Component.classes and bases == ():
      # it's a placeholder class, and we've made one before
      n_cls = Component.classes[name]
    else:
      n_cls = Component.classes[name]  = type.__new__(cls, name, bases, dct)
      if(bases):
        n_cls.__depends__ = []
        for bc in  bases:
          if type(bc) == Component:
            n_cls.__depends__.append(bc)
            setattr(n_cls, bc.__name__, outlet(bc.__name__))

    return n_cls
    
  def rebase(cls):
    for d in cls.__depends__:
      comp = cls.compReg.lookup(d.__name__)
      if not isinstance(comp, type):
        comp = type(comp)
      
      # Fix for unit tests, if the system is bound and rebound
      # the rebased object remains which  break the search algorythm
      # in load_classes. By replacing the placeholder class with the real
      # component, it will be used instead of the placeholder next time
      # Component.__new__ is called
      Component.classes[d.__name__] = comp
      bases = list(cls.__bases__)
      try:
        bases[bases.index(d)] = comp
      except ValueError:
        # rebase called twice, this happens during unittesting as the whole system is bound and rebound
        break
        
      cls.__bases__ = tuple(bases)
      

def component(className):
  comp = Component(className,(),{'assembled': classmethod(lambda cls:  cls.rebase())})
  
  return comp


        
