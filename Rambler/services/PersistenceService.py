from Rambler.EventChannel import Handler
from Rambler.UnitOfWork import UnitOfWork

from zope.interface import Interface

from Rambler.Lazy import LazyCat, LazyMap
from Rambler import outlet
from Rambler import OQL

from Rambler.Events import Vote


class LoadProxy:
    """ A Class used by the LazyMap to proxy the
    load method of the PersistenceService.  This
    allows the LazyMap to pass more arguments to
    the load call
    """
    def __init__(self, klass, func):
        self.klass = klass
        self.func = func

    def __call__(self, key):
        return self.func(self.klass, key)


    
class PersistenceService(object):
    descriptionService = outlet("EntityDescriptionService")
    eventChannel       = outlet("EventService")
    MapperRegistry     = outlet("MappingService")
    queryService       = outlet("Query")
    txn                = outlet("TransactionService")
    log                = outlet("LogService")
    

    """Singleton object that Implements the PersistenceService
    interface on a per txn basis. All operations on this object are
    isolated to the transaction that they were invoked within.  """

    def __init__(self):
        self._txnUOW = {} # Transaction to UnitOfWork mappings

    def assembled(self):
        # Subscribe to the commit and rollback events
        self.eventChannel.subscribeToEvent(
            "prepare", Handler(self.prepare), str)
        self.eventChannel.subscribeToEvent(
            "rollback", Handler(self.rollback), str)

        # Register to vote
        self.eventChannel.registerEvent("vote", self, Vote)

                
        
    def _getUnitOfWork(self):
        
        """Returns the UnitOfWork for the current transaction. If a
        UnitOfWork does not exist for the current transaction one is
        created and a synchronization object is registered so that it
        can be removed at the end of the transaction.

        TRANSACTION_REQUIRED is thrown if this method is invoked
        outside of a transaction."""

        txnId = self.txn.get_transaction_name()
        if txnId == "":
            raise self.txn.TRANSACTION_REQUIRED()

        if not self._txnUOW.has_key(txnId):
            uow = self._txnUOW[txnId] = UnitOfWork()
        else:
            uow = self._txnUOW[txnId]

        return uow
        
    def create(self,obj):
        return self._getUnitOfWork().registerNew(obj)

    def fetch(self, entityNameOrClass, keys):
	if isinstance(entityNameOrClass, basestring):
	    klass = self.descriptionService.getClassForEntity(entityNameOrClass)
	else:
	    klass = entityNameOrClass

        return LazyMap(LoadProxy(klass, self.load), keys)
    
    def load(self, klass, primaryKey):
        uow = self._getUnitOfWork()
        state =  uow.getStatus(primaryKey)
        if state == UnitOfWork.NOT_EXIST:
            # Object hasn't been loaded in this transaction, let's see
            # if a mapper can load it.
            obj = self.MapperRegistry.getMapper(klass).load(klass, primaryKey)
            #obj = UpdateWrapper(obj)
            uow.registerClean(obj)
            return obj
        elif state == UnitOfWork.REMOVED:
            raise KeyError
        else:
            return uow.get(primaryKey)

    def build(self, klass, primaryKey, data):

        """Builds an object based on data that is already available,
        rather than relying on the Mapper to retrieve it from some
        storage. The PersistenceService still relies on the Mapper to
        build the actual object, but it will only do so if the object
        is not already in the unit of work. """

        uow = self._getUnitOfWork()
        state = uow.getStatus(primaryKey)
        if state == UnitOfWork.NOT_EXIST:

            # We don't know anything about this object have the Mapper
            # build us one.
            
            obj = self.MapperRegistry.getMapper(klass).build(klass, primaryKey, data)
            uow.registerClean(obj)
            return obj
        elif state == UnitOfWork.REMOVED:
            # This can probably happen if the object was deleted in
            # the current transaction but not from the storage.
            
            raise KeyError
        else:
            return uow.get(primaryKey)

            

    def update(self, obj):
        uow = self._getUnitOfWork()
        state =  uow.getStatus(obj._get_primaryKey())

        # If it's dirty or new there's no need to flag the object
        # again.
        
        if (state != UnitOfWork.NEW) and (state != UnitOfWork.DIRTY):

            return self._getUnitOfWork().registerDirty(obj)

    def remove(self,obj):
        return self._getUnitOfWork().registerRemoved(obj)

    def prepare(self, txnId):
        uow = self._getUnitOfWork()

        try:
            # Save the new objects
            readOnly = 1
            for obj in uow.getNew():
                readOnly = 0
                self.MapperRegistry.getMapper(obj.__class__).create(obj)

                            
            for obj in uow.getDirty():
                readOnly = 0
                self.MapperRegistry.getMapper(obj.__class__).update(obj)

            for obj in uow.getRemoved():
                readOnly = 0
                self.MapperRegistry.getMapper(obj.__class__).remove(obj)

            if readOnly:
                self.eventChannel.publishEvent("vote", self, self.txn.VoteReadOnly)
            else:
                self.eventChannel.publishEvent("vote", self, self.txn.VoteCommit)

            # We're done with this UnitOfWork

            del self._txnUOW[txnId]

        except:
            # Error encountered during prepare, vote rollback
            self.log.exception("Exception encountered while preparing")
            self.eventChannel.publishEvent("vote", self, self.txn.VoteRollback)
            raise

    def rollback(self, txnId):
        """Discards the UnitOfWork for the current transaction."""
        if self._txnUOW.has_key(txnId):
            del self._txnUOW[txnId]

    def query(self, query):
        """ Returns a list of keys that match the given query.  Queries the
        storage first, then modifies the result set based on data in the
        UnitOfWork."""

        # temporary assertion used to hunt down all old dictionary style queries
        assert isinstance(query, basestring)

        # Persistance service queries should only return the
        # primaryKey, i.e. the query should always look like.

        #select primaryKey from <table> where <whatever>
        parsedQuery = self.queryService.parse(query)
        assert len(parsedQuery.columns) == 1
        assert parsedQuery.columns[0].basename == 'primaryKey'

        # for backwards compatability these internal queries should
        # always return everything. Not sure why, but if you want to
        # edit ParsedeResult returned by pyparsing you have to use
        # dictionary notation.

        parsedQuery['limit'] = 0

        results = self.queryService.query(parsedQuery)

        keys = []

        # TODO: We need te strip the whole CORBA nonsense out of the
        # RecordSet, There's allot of overhead here between us pulling
        # values out in a datasource, stuffing them into a CORBA
        # structure then converting them back to regular python types
        # here.

        while 1:
            try:
                records = results.fetchMany("query", 1024)
            except StopIteration:
                break
            
            keys.extend([record['primaryKey'] for record in records.value()])

        uow = self._getUnitOfWork()
        klass = self.descriptionService\
                .getClassForEntity(parsedQuery.rootEntity)
        
        # erase the keys that have been removed in this transaction
        for obj in uow.getRemoved():
            if isinstance(obj, klass):
                keys.remove(obj._get_primaryKey())
        

        # ain't list comphrehesion cool, in almost one statment we
        # examine each object that has been registered as new, if it's
        # an instance of the klass we're querying for we see if it
        # matches our where clause, if it does we add it's key to the
        # list of keys that our database returned.

        matchesWhere = OQL.makeFilterFunc(parsedQuery)
        
        keys.extend( 
            [obj._get_primaryKey() for obj in uow.getNew()
             if isinstance(obj, klass) and matchesWhere(obj)]
            )


        return tuple(keys)
        
##    def getOQLQuery(self, query, resultType=None):

##        # This is a throw back from the old design where the
##        # persistence service only knew about Entities... The OQLQuery
##        # still uses the CatalogService which is ZODB specific.
##        uow = self._getUnitOfWork()

##        # Adding a couple of keys for the pagination code.
##        if query.has_key('query_offset'):
##            query['query_offset'] = int(query['query_offset'])
##            if query['query_offset'] < 0:
##                query['query_offset'] = 0
##        else:
##            query['query_offset'] = 0

##        if query.has_key('query_limit'):
##            query['query_limit'] = int(query['query_limit'])
##        else:
##            query['query_limit'] = 3000

##        return OQLQuery(query, uow, resultType)
    

##class UpdateWrapper:

##    """Wraps an object, if any _set_ methods are invoke registers the
##    object as updated with PersistenceService."""

##    def __init__(self, obj):
##        self._obj = obj
##        self._dirty = 0

##    def __getattr__(self, attr):
##        if attr.startswith('_set_') and not self._dirty:
##            self._dirty = 1
##            Server.getService("PersistenceService").update(self._obj)
            
##        return getattr(self._obj, attr)


class IMappingObject(Interface):
    """Moves data between a paticular type of object to a paticular
    storage."""

    def create(object):
        """Add the object to the appropriate storage."""

    def load(klass, primaryKey):
        """Loads an object with the given key. Raises KeyError if the
        object does not exist with that key."""
        
    def update(self, object):
        """Update the storage with the new values. Raises KeyError if
        the object does not exist in the storgage."""

    def remove(self, object):
        """Remove the object from the storage. Raises KeyError if the
        object does not exist in the storage."""


from copy import copy
class InMemoryDataMapper:
    """Keeps objects in memory. Useful for testing"""
    def __init__(self):
        #print "Initing InMemoryDataMapper"
        self._objects = {}

    def create(self, object):
        self._objects[object._get_primaryKey()] = object

    def load(self, klass, primaryKey):
        entity = self.copyEntity(self._objects[primaryKey])
        entity.__watching__ = True
        return entity

    def update(self, object):
        self._objects[object._get_primaryKey()] = object

    def remove(self, object):
        del self._objects[object._get_primaryKey()]
    
    # These methods impliment the QueryService interface

    def handleMessage(self, msg):
        pass
            
    def OnRemove(self, entity):
        pass
    
    def OnCreate(self, entity):
        pass
    
    def OnUpdate(self, entity):
        pass
    
    def query_keys(self, entityHome, **kw):
        # Don't do any sorting
        if kw.has_key('sort_on'):
            del kw['sort_on']
        if kw.has_key('sort_order'):
            del kw['sort_order']


        homeId = entityHome.homeId
        newMatches = []
        keyvalpairs = kw.items()

        # Apply the search to all objects in the db, taking in to
        # account any changes that may have been made in this TXN
        
        suspects =  self._objects.copy()
       ## uow = Server.getService("PersistenceService")._getUnitOfWork()


##        for obj in uow.getNew() + uow.getDirty():
##            if hasattr(obj, "_get_primaryKey"):
##                suspects[obj._get_primaryKey()] = obj

        # Strip out all keys that start with "query" first
        keyvalpairs = filter(lambda x: not x[0].startswith("query"), keyvalpairs)

        # Now perform the search
        for primaryKey, obj in suspects.items():
            match = 0
            if hasattr(obj, '_get_home') and homeId == obj._get_home().homeId:
                match = 1
                for key, val in keyvalpairs:
                    if not hasattr(obj, key):
                        match = 0
                    else:
                        if type(val) != list:
                            if getattr(obj, key)() != val:
                                match = 0
                        else:

                            if getattr(obj, key)() not in val:
                                match = 0

                    if match == 0:
                        break
                    
            if match:
                newMatches.append(primaryKey)

        return newMatches

    def query_objs(self, entityHome, **kw):

        keys = self.query_keys(entityHome, **kw)
        PS = Server.getService("PersistenceService")

        # Use the PersistenceService to get the right version of the
        # object.
        
        klass = entityHome.entityClass
        return [PS.load(klass, pk) for pk in keys]

    def copyEntity(self, entity):
        return copy(entity)
        n = entity.__class__(entity._get_primaryKey())
        for attr in dir(entity):
            if attr.startswith("_get_"):

                name = attr[5:]
                if name not in ("home", "primaryKey"):
                    setter  = getattr(n, "_set_" + name)
                    getter = getattr(entity, attr)
                    setter(getter())
        return n
        
        
    
class MappingService(object):
    """A registry of mapping objects to classes."""

    def __init__(self):
        self._mappers = {}

    def registerMapper(self, klass, mapper):
        self._mappers[klass] = mapper

    def getMapper(self, klass):
        return self._mappers.get(klass)
        


