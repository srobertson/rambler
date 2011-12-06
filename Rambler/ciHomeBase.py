from time import time

from Rambler import outlet, error
from Money import Money



# A mapping of types to human readable values
HR_TYPES = {int: "number without decimals",
            float: "number",
            Money: "number represeting a dollar amount"}


class Entity(object):
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

    We also need to bind the ErrorFactory to ourselves
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

    # TODO: remove onec we allow pk for any attribute
    def __init__(self, primaryKey):
	self.primaryKey = primaryKey

    def _get_home(klass):
        name = klass.EntityDescriptionService.getName(klass)
        homeClass = klass.EntityDescriptionService.getHome(name)
        # This is lame.  Scott told me to do it -F
        return klass.EntityDescriptionService.componentRegistry.lookup(homeClass.homeId)
    _get_home = classmethod(_get_home)


    def valueForKey(self, keyPath):
        """Returns the value for the specified key, relative to self"""

        obj = self
        for key in keyPath.split('.'):
            obj = getattr(obj, key)
        return obj

    def setValueForKey(self, value, keyPath):
        keys = keyPath.split('.')
        if len(keys) > 1:

            # the keypath looked like node1.node2.attribute we want to get
            # the object node2 so we can call setattr on it
            victim = self.valueForKey('.'.join(keys[:-1]))
        else :
            victim = self

        field = keys[-1]
        # Attempt to perform some sort of type conversion o the value
        # before setting it.
        try:
            vType = self.EntityDescriptionService.getField(victim.__class__, field).type
            if type(value) != vType:
                value = vType(value)
        except AttributeError:
            # This happens if value is an entity..ignore it
            pass
        except ValueError, e:
            e.attempted = vType
            raise e

        setattr(victim, field, value)

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
                self.setValueForKey(value, keyPath)
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
                "Entity has nvalid fields."
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



class ciHomeBase(object):
    # Must be overidden by sub-classes
    entityClass = None
    CORBABridge = outlet("CORBABridge")
    log = outlet("LogService")

    errorFactory = outlet("ErrorFactory")
    #FinderException = error(0,description="Entity not found")

    
    def __init__(self):
        
        #self._entities = {} 
        #self._nextId = 1

        # Mapping of user id's to observer's, remote notifications are
        # only sent back to the same user that made them. The key is
        # the user the value is the observer. The special key None is
        # reserved for all local observers.
        
        self._observers = {None:[]}

        if self.entityClass is None:
            raise NotImplementedError, "%s must implement entityClass" % self.homeId

        # We store these on the class because we don't need one per home instance.
        #if not hasattr(ciHomeBase, "RelationService"):
        #    ciHomeBase.RelationService = Server.getService("RelationService")
        #    ciHomeBase.PersistenceService = Server.getService("PersistenceService")
        #    ciHomeBase.EntityDescriptionService = Server.getService("EntityDescriptionService")

        #ks = Server.getService("KeyGenService")
        #if ks is not None:
        #    self.keyGenerator = ks.getKeyGenerator(self)

        # Subscribe to the logout event so that we can cleanup observers.

        if hasattr(self, "init"):
            self.init()

    def _set_eventChannel(self, eventChannel):
        eventChannel.subscribeToEvent("logout",
                                      self.cleanupObservers,
                                      str)

        #eventChannel.subscribeToEvent("Initializing", Handler(self.initKeyGen), str)
    eventChannel=property(None,_set_eventChannel)

    def assembled(self):
        
        # have to wait for all the system to start before we can use
        # the keygenerator, hopefully, the keymapper has already heard
        # the event befor us and is ready to answer questions about
        # keys.
        
        self.keyGenerator = self.KeyGenService.getKeyGenerator("NextHiKey")


    def create(self, name, entity):
        raise "NotImplemented", "The create function has not been implemented!"

    def remove(self, entity):

        if hasattr(self, "_remove"):
            self._remove(entity)
            
        self.notifyRemove(entity)
        self.PersistenceService.remove(entity)


    def findByPrimaryKey(self, pk):

        # Find the derived class
        if type(self.entityClass) == str:
            eds = self.EntityDescriptionService
            entityClass = eds.getClassForEntity(self.entityClass)
        else:
            entityClass = self.entityClass
        

	return self.PersistenceService.load(entityClass, pk)



    def getAll(self):
        
        eds = self.EntityDescriptionService
        entityName = eds.getName(self.entityClass)
	keys = self.PersistenceService.query('SELECT primaryKey from %s LIMIT 0' 
                                             % entityName)

        return self.PersistenceService.fetch(entityName, keys)

    def _get_homeId(klass):
        return klass.homeId
    _get_homeId = classmethod(_get_homeId)


    # DEPRECATED: Don's use home observers, use the query service

    def addObserver(self, observer):
        try: # If it's a corba object it won't raise an exception
            omniORB.setClientCallTimeout(observer, 500)
            CORBA.id(observer)
            self._getUserObservers().append(observer)
        except BAD_PARAM:
            # It's a local observer
            self._observers[None].append(observer)
            



    def removeObserver(self, observer):

        
        try:

            # If it's a corba object (i.e. a remote observer) it won't
            # raise an exception
            
            CORBA.id(observer)
            observers = self._getUserObservers()
            ior = self.CORBABridge.orb.object_to_string(observer)

            for i in range(0, len(observers)):
                if ior == Server.orb.object_to_string(observers[i]):
                    del observers[i]
                    break
                
        except BAD_PARAM:
            # It's a local observer
            observers = self._observers[None]
            try:
                del observers[observers.index(observer)]
            except AttributeError:
                # Sometimes observers is None, so we catch that and pass.
                pass



    def _notify(self, entity, event):

        # Handy function that notifies observers. It'll remove any
        # observer it can't talk to.
        assert event  in ("OnCreate", "OnUpdate", "OnRemove")

        # We need to convert our entity into a corbafied version of
        # itself so we can pass it to the observer if the observer is
        # client side.  For now we're just corbafying all of them, but
        # we need to think about this some more.
            
        remoteEntity = EntityInfo(self.homeId,
                                  self.EntityDescriptionService.getName(entity.__class__),
                                  entity._get_primaryKey())

        localEntity = entity

        for o in self._getObservers():
            try:
                CORBA.id(o)
                entity = remoteEntity
            except BAD_PARAM:
                entity = localEntity
            
            try:
                method = getattr(o, event)
                method(entity)
            except (TRANSIENT, COMM_FAILURE):
                # Can't contact this observer, remove it

                # TODO, we need to clean up resources after a user logs-out
                
                self.removeObserver(o)

    def notifyCreate(self, entity):
        self._notify(entity, "OnCreate")


    def notifyUpdate(self, entity):
        self._notify(entity, "OnUpdate")

    def notifyRemove(self, entity):
        self._notify(entity, "OnRemove")
    
    def _getUserObservers(self):

        """Handy helper function that return's the current
        user. Normally I would have set the IdetityService on the
        object to avoid a look-up. But there's a problem here since
        the IdentityService has to be registered after the user
        home."""

        #identService = Server.getService("IdentityService")
        #if identService is  None:
        #    # No identity service means no remote observers.
        #    return []

        user = self.sessionManager.getCredentials()
        username = user.username

        if not self._observers.has_key(username):
            self._observers[username] = []

        return self._observers[username]

    def _getObservers(self):
        
        """Returns a list of observers, all the local ones along with
        the remote ones for the currently authenticated user."""

        observers = []
        observers.extend(self._observers[None])
        try:
            remote_observers = self._getUserObservers()
            observers.extend(remote_observers)
        except NO_PERMISSION:

            # The only time I think this sholud happen is for
            # transactions started in the server.
            
            pass
        return observers

    def cleanupObservers(self, username):
        """ Deletes all the observers for the given user """

        if self._observers.has_key(username):
            self.log.debug("Cleaning up observers for %s for %s"%(self.homeId,username))
            del self._observers[username]



