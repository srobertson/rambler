from Rambler.Synchronized import synchronized, staticsynchronized,\
     classmethodsynchronized

class StorageNotInitialized(Exception):

    """Thrown when an attempt is made to acces a KeyGen's method
    before Initializing has been published."""
    
    pass


class Key(object):

    """Dummy class use to interact with the Persistence machinary properly."""

    def __init__(self, name, key):
        self.key = key
        self.name = name

    def _get_primaryKey(self):
        return self.name


class GeneratorBase(object):
    
    """Class methods that are common to both HiLow and Incremental
    keys, they take care of registering for events and intializing
    keys"""
    
    
    def assembled(klass):

        # we store key gens here, so when we see initializing we can
        # tell the keyGens it's safe to load
        
        klass.keyGens = {} 
        klass.inited = False

        klass.eventChannel.subscribeToEvent("Initializing", klass, str)
    assembled = classmethod(assembled)

    def handleMessage(klass, msg):
        for keyGen in klass.keyGens.values():
            keyGen.loadKey()
        klass.inited = True
    handleMessage = classmethod(handleMessage)

    
    def getKeyGenerator(klass, keyName):
        keyGens = klass.keyGens
        
        if not keyGens.has_key(keyName):
            keyGen = klass(keyName)
            if klass.inited:
                # looks like initialization happened before this
                # keyGenerator was created, so it's safe to call
                # loadKey on this keyGen
                
                keyGen.loadKey()
            keyGens[keyName] = keyGen

        return keyGens[keyName]
    
    getKeyGenerator = classmethodsynchronized(getKeyGenerator)


    

class IncrementalKeyGenerator(GeneratorBase):

    """Creates a KeyGenerator that returns an incremental key, (it
    increases by one, every time nextKey() is called. This generator
    loads it's key into memory on startup and provides a thredsafe
    manner of retreiving it. Whenever the key changes it is saved to
    the database.

    To function correctly KeyGenerators need the PersitanceService, a
    KeyMapper(althogh it's not directly used) and an Event
    Channel. These things will typically be setup by the
    componentRegistry, but for testing purposes we do the binding by
    hand.
    
    >>> from Rambler.EventChannel import EventChannel
    >>> IncrementalKeyGenerator.eventChannel = EventChannel()

    Normally the storage attribute would be set to the persistence
    service, but we can get away with using our InMemeroyKeyMapper
    directly since they share the same interface as the
    PersistenceService. This saves us the hassle of trying to setup
    the persistence service for these tests.

    >>> IncrementalKeyGenerator.storage = InMemoryKeyMapper()

    Typically the Application object publishes the "Intitialized"
    event, but in this test we tell the EventChannel that object None
    does that.
    >>> IncrementalKeyGenerator.eventChannel.registerEvent("Initializing",
    ...    None, str)
    >>> IncrementalKeyGenerator.assembled()
    

    To access a key you call getKeyGenerator with a unique name. In
    this instance we use MyKeyName. This will return a key generator
    for the specified name.
    
    >>> keyGen = IncrementalKeyGenerator.getKeyGenerator('MyKeyName')

    A Key Generator is repsonsible for incrementing a value and making
    sure that the a duplicate key is never generated. Consequently,
    they need a storage object to persist their values and they can't
    be used until the entire system has been initialized (most
    storages wait until they receive the Initialized event before
    finalizing their setup.)

    So for instance if you acquired a KeyGenerator during assembly and
    tried to invoke one of it's operations before Startup you would
    receive an error.
    
    >>> keyGen.currentKey()
    Traceback (most recent call last):
    ...
    StorageNotInitialized

    >>> keyGen.nextKey()
    Traceback (most recent call last):
    ...
    StorageNotInitialized

    After the component system has been initialized you can work with keys.

    >>> IncrementalKeyGenerator.eventChannel.publishEvent('Initializing',
    ...    None, 'Fake Transaction Id')
    
    >>> keyGen.currentKey()
    0
    >>> keyGen.nextKey()
    1

    Getting a keygenartor with a different name will produce, different results.
    
    >>> keyGen = IncrementalKeyGenerator.getKeyGenerator('MyOtherKeyName')
    >>> keyGen.currentKey()
    0
    >>> keyGen.nextKey()
    1

    However if we go back and grab the KeyGenerator 'MyKeyName' again
    it will continue counting where it left off.
    
    >>> keyGen = IncrementalKeyGenerator.getKeyGenerator('MyKeyName')
    >>> keyGen.nextKey()
    2
    


    """

    def __init__(self, name):
        self.name = name
        self.value = None

    def loadKey(self):
        if self.value is None:
            try:
                key = self.storage.load(Key, self.name)
            except KeyError:
                # Key's not there, start it at 0
                key = Key(self.name, 0)
                self.storage.create(key)
            
            self.value = key.key
        else:
            raise RuntimeError("Load called twice for Key %s." % self.name)

    def nextKey(self):
        if self.value is None:
            raise StorageNotInitialized
        
        # We're doing this to get around the fact that data is
        # isolated in a transaction, but keys need to be global for
        # ALL transactions. So even though we load a key from the DB,
        # we ignore it and insert the value we've been tracking in
        # memory.
        
        value = self.currentKey() + 1
        
        lastKey = self.storage.load(Key, self.name)
        lastKey.key = value
        self.storage.update(lastKey)
        
        self.value = value
        #print "Loading: %s:%s" % (self.name, self.value)
                
        return value
    nextKey = synchronized(nextKey)


    def currentKey(self):
        """Returns the next key for the given name"""
        if self.value is None:
            raise StorageNotInitialized
        
        # Yeah, it's wierd that the value attribute is called key
        return self.value
    
    currentKey = synchronized(currentKey)

class HiLowKeyGenerator(GeneratorBase):

    """Creates a KeyGenerator which creates key that are composed of
    two integer's. One that the Key generated is instiated with and
    one that is incremented every time the nextKey() method is
    called. This design is meant to reduce load on a database because
    the only time we talk to the DB during startup. From then on
    all keys are tracked in memory only.

    The usage of this key is identical to the IncrementalKeyGenerator
    only the value's returned are different. An incremental Key
    generator returns a key that is incremented by one every time it's
    called. A HiLowKeyGenerator returns a string that consits of two
    number seperated by a dash.

    The following is component binding, see comments in
    IncrementalKeyGen doctest for an explanation.

    >>> from Rambler.EventChannel import EventChannel
    >>> HiLowKeyGenerator.eventChannel = EventChannel()

    Normally the storage attribute would be set to the persistence
    service, but we can get away with using our InMemeroyKeyMapper
    directly since they share the same interface as the
    PersistenceService. This saves us the hassle of trying to setup
    the persistence service for these tests.

    >>> HiLowKeyGenerator.storage = InMemoryKeyMapper()

    Typically the Application object publishes the "Intitialized"
    event, but in this test we tell the EventChannel that object None
    does that.
    >>> HiLowKeyGenerator.eventChannel.registerEvent("Initializing",
    ...    None, str)
    >>> HiLowKeyGenerator.assembled()
    

    To access a key you call getKeyGenerator with a unique name. In
    this instance we use MyKeyName. This will return a key generator
    for the specified name.
    
    >>> keyGen = HiLowKeyGenerator.getKeyGenerator('MyHiKey')

    >>> keyGen.currentKey()
    Traceback (most recent call last):
    ...
    StorageNotInitialized

    >>> keyGen.nextKey()
    Traceback (most recent call last):
    ...
    StorageNotInitialized


    After the component system has been initialized you can work with keys.

    >>> HiLowKeyGenerator.eventChannel.publishEvent('Initializing',
    ...    None, 'Fake Transaction Id')
    
    >>> keyGen.currentKey()
    '0-0'
    >>> keyGen.nextKey()
    '0-1'

    If we got a second hi key it's highkey value would be one higher
    >>> keyGen = HiLowKeyGenerator.getKeyGenerator('My2ndHiKey')
    >>> keyGen.currentKey()
    '1-0'
    >>> keyGen.nextKey()
    '1-1'

    """

    def __init__(self, keyName):
        # We ignore keyname, 
        self._lowKey = 0
        self._hiKey = None


    def loadKey(self):
        try:
            hkey = self.storage.load(Key, "NextHiKey")
        except KeyError:
            # Key's not there, start it at 0
            hkey = Key("NextHiKey", 0)
            self.storage.create(hkey)
            
        self._hiKey = hkey.key
        hkey.key +=1
        self.storage.update(hkey)


    def currentKey(self):
        if self._hiKey is None:
            raise StorageNotInitialized
        
        return "%s-%s" % (self._hiKey, self._lowKey)
    currentKey = synchronized(currentKey)

    def nextKey(self):
        if self._hiKey is None:
            raise StorageNotInitialized

        self._lowKey += 1
        key = "%s-%s" % (self._hiKey, self._lowKey)
        return key
    nextKey = synchronized(nextKey)



class InMemoryKeyMapper(object):
    """Stores keys non presistently in memory """
    def __init__(self):
        self.Keys = {}
        
    def create(self, object):
        self.Keys[object.name] = object.key

    def load(self, klass, primaryKey):
        return Key(primaryKey, self.Keys[primaryKey])
        
    def update(self, object):
        self.Keys[object.name] = object.key

        
    def remove(self, object):
        del self.Keys[object.name]




