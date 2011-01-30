from collections import deque

import warnings
import re

# Do not add anything to this dict!
EMPTY={}

def deprecated(func):
  def warn(*args, **kw):
    func_name = func.func_name
    # convert camelCase to camel_case
    new_name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', func_name)
    new_name = re.sub(r'([a-z\d])([A-Z])',r'\1_\2', new_name).lower()
    
    
    warnings.warn("camel case method %s deprecated use %s" % (func_name, new_name), DeprecationWarning)
    return func(*args, **kw)
  return warn


class RObject(object):
    """Rambler objects provide key/value codeing similar to apple's
    NSObject. But for python of Course"""
    
    
    # KVO Constants
    KeyValueChangeSetting     = 1
    KeyValueChangeInsertion   = 2
    KeyValueChangeRemoval     = 3
    KeyValueChangeReplacement = 4
    
    KeyValueObservingOptionNew   = 0x01
    KeyValueObservingOptionOld   = 0x02
    
    KeyValueChangeKindKey    = intern("KeyValueChangeKindKey")
    KeyValueChangeNewKey     = intern("KeyValueChangeNewKey")
    KeyValueChangeOldKey     = intern("KeyValueChangeNewKey")
    KeyValueChangeIndexesKey = intern("KeyValueChangeIndexesKey")
    
    
    @deprecated
    def valueForKey(self, key):
      return self.value_for_key(key)
      
    @deprecated
    def valueForKeyPath(self, keyPath):
      return self.value_for_key_path(keyPath)
      
    @deprecated
    def valueForUndefinedKey(self, key):
      return self.value_for_undefined_key(key)
      
    @deprecated
    def setValueForKey(self, value, key):
      return self.set_value_for_key(value, key)
      
    @deprecated
    def setValueForKeyPath(self, value, keyPath):
      return self.set_value_for_key_path(value, keyPath)
      
    def __init__(self, **kw):
      self.set_values(kw)
      
    #def __getattr__(self, key):
    ##  return self.value_for_undefined_key(key)
    
    def value_for_key(self, key):
        # todo: valueForUndefinedKey.. which means we need to use
        # __slots__ or the EDS

        if (not hasattr(self,key)) and (hasattr(self, '__slots__') and key not in self.__slots__):
            return self.value_for_undefined_key(key)

        # If the object dosen't define __slots__ we're not sure if the
        # value should be there or not, default it to None, we might
        # tighten this up in the future.
        attribute = getattr(self, key, None)

        if callable(attribute):
            results = attribute()
        else:
            results = attribute
            
        return results

    def value_for_key_path(self, key_path):
        """Returns the value for the specified key, relative to self"""

        parts = key_path.split('.')
        key = parts[0]

        if len(parts) == 1:
            sub_path = ""
        else:
            sub_path = '.'.join(parts[1:])

        obj = self.value_for_key(key)
        if sub_path:
            return obj.value_for_key_path(sub_path)
        else:
            return obj
    
    def value_for_undefined_key(self, key):
        raise AttributeError, "%s has no key %s" % (self.__class__.__name__, key)
        
    def set_value_for_undefined_key(self, value, key):
      raise AttributeError, "%s has no key %s" % (self.__class__.__name__, key)
              
    def set_value_for_key(self, value, key):
        self.will_change_value_for(key)
        attribute_type = None
        if value is not None and hasattr(self, "__slots__") and type(self.__slots__) == dict:
            # if the Object has __slots__ defined and it's a
            # dictionary we'll asume the value of the __slots__
            # dictionary should be the type the value we're setting
            # should be coreced to.
            valueType = type(value)

            # it's not an error to not be defined in the slots, it's possible
            # there may be a set<Atribute> method

            attribute_type = self.__slots__.get(key)
            if attribute_type and valueType != attribute_type:
                value = attribute_type(value)

        
        set_method_name = 'set_' + key
        
        if hasattr(self, set_method_name):
            getattr(self, set_method_name)(value)
        elif hasattr(self, key) or attribute_type:
            # if the object already has a key with the value or it's 
            # been defined in the slot
            setattr(self, key, value)
        else:
          self.set_value_for_undefined_key(value, key)

        self.did_change_value_for(key);

      
    def set_value_for_key_path(self, value, key_path):
        parts = key_path.split('.')
        key = parts[0]

        if len(parts) == 1:
            sub_path = ""
        else:
            sub_path = '.'.join(parts[1:])

        if sub_path:
            obj = self.value_for_key(key)
            obj.set_value_for_key_path(value, sub_path)
        else:
            self.set_value_for_key(value, key)
            
    def set_values(self, keyed_values):
      # values are coming in as a dictionary, we convert them to a
      # tuple of tuples so that they can be sorted.

      # for instance we might have a dictionary like this
      # {'name':'blah', 'primaryContact': '123-1',
      # 'primaryContact.firstName':'John',
      # 'primaryContact.lastName':'Cash'}

      # we want to set the attributes in order, so that the
      # primaryContact object whose pyramryKey is 123-1 is set on
      # the top level object prior to use setting
      # primaryContact.firstName and primaryContact.lastName.
      
      keyed_values = keyed_values.items()
      keyed_values.sort()

      errors = {}
      for key_path, value in keyed_values:
        self.set_value_for_key_path(value, key_path)
      
      
            
    # KVO
    def add_observer(self, observer, key_path, options=0, *args, **kw):
      """Register an observer to hear Key/Value events"""
      if not hasattr(self, '_oldvals'):
        # don't waste memory unless someone is observing this object
        self._oldvals = deque()
        self.observation_info = {}
      self.observation_info[(observer, key_path)] = (options, args, kw)
    
    def remove_observer(self, observer, key_path):
      """Register an observer to hear Key/Value events"""
      if  hasattr(self, '_oldvals'):
        del self.observation_info[(observer, key_path)]
    
    def will_change_value_for(self, key):
      # About to change the attribute of an object, store it's old value,
      # note if changing multiple attrs you must call did_change_value_for
      # in the exact reverse order  you called will_change_value_for_key.

      # For example:
      # object.will_change_value_for_key('x')
      # object.will_change_value_for_key('y')
      # object.x = 10;
      # object.y = 11;
      # object.did_change_value_for_key('y')
      # object.did_change_value_for_key('x')
      if hasattr(self, '_oldvals'):
        self._oldvals.append(self.value_for_key(key))
      
    def did_change_value_for(self, key):
      if hasattr(self, '_oldvals'):
        oldval = self._oldvals.pop()
        value = self.value_for_key(key)
        for (observer, key_path), (options, args, kw) in self.observation_info.items():
          if key_path != key:
            continue
          if options:
            changes = {self.KeyValueChangeKindKey: self.KeyValueChangeSetting}
            if self.KeyValueObservingOptionNew  & options:
              changes[self.KeyValueChangeNewKey] = value
            if self.KeyValueObservingOptionOld & options:
              changes[self.KeyValueChangeOldKey] = oldval
          else:
            changes = None

          try:
            observer.observe_value_for(key_path, self, changes, *args, **kw)
          except TypeError:
            # Sometimes we want both a class and instances of the classes to be observers.
            # In this case we'll encounter unbound method errors
            if issubclass(observer,object):
              observer.observe_value_for.im_func(observer, key_path, self, changes, *args, **kw)
            else:
              raise
          
    def changing(self, *keys):
      """Returns a Python Context manager that simplifies  KVO notifications.
      
      with self.changing('key1', 'key2'):
        self.key1 = 'new value'
        self.key2 = 'new value'
      
      is equivalent to:
      
      self.will_change_value_for('key1')
      self.will_change_value_for('key2')
      self.key1 = 'new value'
      self.key2 = 'new value'
      self.did_change_value_for('key2')
      self.did_change_value_for('key1')
      
      
      """
      return KVOContextManager(self, keys)
      
    # key value coding
    @classmethod
    def init_with_coder(cls, coder):
      obj = cls()
      for key, key_type in getattr(cls, '__slots__', EMPTY).items():
        try:
          decode_method_name = 'decode_%s_for' % key_type.__name__
          # attempt to call encode_type_for() the given type, for examlp
          # encode_int_for(...) if the value is an int. If the coder does
          # support the specific type we use the generic to encode_object_for(...)
          # method
          decode_val_for_key = getattr(coder, decode_method_name, coder.decode_object_for)
          obj.set_value_for_key(decode_val_for_key(key), key)
        except:
          cls.log.exception('Exception encountered decoding %s as %s', key, key_type)
          raise
      return obj
      
      
    def encode_with(self, coder):
      """Introspect the given object and returns a dictionary of values that should be persisted"""

      for key, key_type in getattr(self, '__slots__', EMPTY).items():
        if hasattr(self, key):
          encode_method_name = 'encode_%s_for' % key_type.__name__
          # attempt to call encode_type_for() the given type, for example
          # encode_int_for(...) if the value is an int. If the coder does
          # support the specific type we use the generic to encode_object_for(...)
          # method
          encode_val_with_key = getattr(coder, encode_method_name, coder.encode_object_for)
          value = getattr(self, key)
          encode_val_with_key(value, key)
       
      # if we use just __dict__ private variables will be encoded, not what we want..
      # hmm
       
      #for key, value in getattr(self, '__dict__', EMPTY).items():
      #  encode_method_name = 'encode_%s_for' % type(value).__name__
      #  encode_val_with_key = getattr(coder, encode_method_name, coder.encode_object_for)
      #  encode_val_with_key(value, key)
      
    
      
   

class KVOContextManager(object):
  def __init__(self, obj, keys):
    self.obj = obj
    self.keys = keys
    
  def __enter__(self):
    for key in self.keys:
      self.obj.will_change_value_for(key)

  def __exit__(self,ex_type, value,traceback):
    # note this method posts updates even if an exception was encountered,
    # not sure if this is a good thing or not. We'll see.
    
    for key in reversed(self.keys):
      self.obj.did_change_value_for(key)
    
    


class Entity(RObject):
    """Base Class for all Entities in Rambler.

    In order to run a full set of tests we need to create a fake
    entity description service.
    
    >>> class Field:
    ...   def __init__(self, type):
    ...     self.type=type
    >>> class EDS:
    ...   registry={Entity:{'foo':Field(int),'child':Field(Entity)}}
    ...   def getField(self, victim, field):
    ...     return self.registry[victim][field]

    >>> Entity.EntityDescriptionService = EDS()

    We also need to find the ErrorFactory to ourselves
    >>> from Rambler.ErrorFactory import ErrorFactory
    >>> Entity.errorFactory = ErrorFactory('test')
    
    Provides methods similar to apple's key value codeing, which is
    similar to get/setattr()

    For instance
    >>> e = Entity()
    >>> e.setValueForKey(10, 'foo')
    >>> e.foo
    10
    >>> e.valueForKey('foo')
    10

    Unlike get/setattr valueForKey allows you to specify attributes
    across relationships.

    >>> e.setValueForKey(Entity(), 'child')
    >>> e.setValueForKey(15, 'child.foo')
    >>> e.valueForKey('child.foo')
    15
    >>> e.child.foo
    15

    The above statements can be done in a single shot using setValuesForKeysWithDictionary()
    >>> del e
    >>> e = Entity()
    >>> keyedValues = {'foo': 10,
    ...                'child': Entity(),
    ...                'child.foo': 21}
    >>> e.setValuesForKeysWithDictionary(keyedValues)
    >>> e.foo
    10
    >>> e.child.foo
    21

    If we try to set an invalid value, a ValueError will be thrown
    with the type we were expecting on a special attribute
    
    >>> try:
    ...   e.setValueForKey('bar', 'foo')
    ... except ValueError, exc:
    ...   exc.attempted
    <type 'int'>

    And if we call setValuesForKeysWithDictionary() that includes one
    or more invalid values, we'll get a ciError object with each of
    those values.
    >>> keyedValues = {'foo': '10',
    ...                'child': Entity(),
    ...                'child.foo': 'bar'}

    >>> try:
    ...   e.setValuesForKeysWithDictionary(keyedValues)
    ... except Exception, exc:
    ...   pass
    ...   e.errorFactory.isError(exc)
    ...   exc.userInfo['child.foo']
    True
    'bar is not a valid number without decimals'

    Note that we also passed the first foo as a string and it was
    still successfully converted to an int.
    >>> e.foo
    10

    """

    # Define some error codes
    SET_VALUE_ERROR = 0

    
    def _get_home(klass):
        name = klass.EntityDescriptionService.getName(klass)
        homeClass = klass.EntityDescriptionService.getHome(name)
        # This is lame.  Scott told me to do it -F
        return klass.EntityDescriptionService.componentRegistry.lookup(homeClass.homeId)
    _get_home = classmethod(_get_home)




    def setValuesForKeysWithDictionary(self, keyedValues):

        # values are coming in as a dictionary, we convert them to a
        # tuple of tuples so that they can be sorted.

        # for instance we might have a dictionary like this
        # {'name':'blah', 'primaryContact': '123-1',
        # 'primaryContact.firstName':'John',
        # 'primaryContact.lastName':'Cash'}

        # we want to set the attributes in order, so that the
        # primaryContact object whose pyramryKey is 123-1 is set on
        # the top level object prior to use setting
        # primaryContact.firstName and primaryContact.lastName.
        
        keyedValues = keyedValues.items()
        keyedValues.sort()

        errors = {}
        for keyPath, value in keyedValues:
            
            # todo: if keypath refers to an Entity, test the value. if
            # the value is an Entity go ahead and set it. If it's not
            # an entity, assume it's the primaryKey, lookup the home
            # and find the real entity.
            try:
                self.setValueForKeyPath(value, keyPath)
            except Exception, e:
                if isinstance(e, ValueError) and  hasattr(e, 'attempted'):
                    # We got a value error, we know what type of
                    # value we were trying to convert so we can
                    # make a nice message
                    if HR_TYPES.has_key(e.attempted):
                        msg = "%s is not a valid %s" % (value, HR_TYPES[e.attempted])
                    else:
                        msg = "Couldn't convert %s to %s (we're really sorry this isn't more clear)" % (value, str(e.attempted))

                    errors[keyPath] = msg
                else:
                    # We got an error we weren't expecting.
                    errors[keyPath] = "We tried really hard (no really, we did!) to set the value to '%s', but I'm afraid we just couldn't do it.  Instead we got this unexpected error: %s" % (value, e)

        if errors:
            # Create and raise a ciError containing all the data we
            # collected.
            errors[self.errorFactory.REASON]      = (
                "Entity has invalid fields."
                )
                
            errors[self.errorFactory.DESCRIPTION] = (
                "We couldn't create the entity because one or more fields cause problems that we couldn't "
                )

            errors[self.errorFactory.SUGGESTION] = (
                "Verify the field data is correct."
                )

            err = self.errorFactory.newError(self.SET_VALUE_ERROR,
                                             errors)

            raise err

        # optimization hint: if we're setting more than one attribute
        # for a subobject, we actually end up traversing it several
        # times with this approach. If we're looking for speed it
        # might be possible to keep track of the objects that we've
        # traversed over and call setValeForKey directly on them
        # in other words do something like
        # for keyPath, value in keyedValues:
        #   keyPath=keyPath.split()
        #   key = ".".join(keyPath[:-1])
        #   attribute = keyPath[-1]
        #   object = cahched.get(key, self.valueFor(keyPath))
        #   object.setValueForKey(value, key)
        #   # cache the object for the next iteration
        #   cache[key] = object
