"""Entities don't store refernces to other entities, rather they use
the reltion service to track relations."""

from Rambler import outlet
from bisect import bisect
from Rambler.Events import NewEntityEvent
from Rambler.handlers import DefaultHandler

from copy import copy

from Rambler.Lazy import LazyMap


from EntityDescriptionService import Field

class RelationService(object):
    componentRegistry = outlet('ComponentRegistry')
    _storage = outlet('PersistenceService')
    relationRegistry = outlet('RelationRegistryService')
    eds = outlet('EntityDescriptionService')

    def __init__(self):

        self._relations = {}
        #self._storage = Server.getService("PersistenceService")

    def _get_eventService(self):
        return self._eventService

    def _set_eventService(self, eventService):
        self._eventService = eventService
        
        #eventService = Server.getService("EventService")
        eventService.subscribeToEvent("NewEntity",
                                      RelationshipScrubber(self),
                                      NewEntityEvent)

        # Add our handler to the Server so that we can set the
        # RelationRegistry up with data from the descriptor.
        Server.registerHandler(RelationHandler)

    def relate( self, obj1, obj2, relation):
        # obj1 = From, obj2 = To
        """ Relates two entities based on the relation name.  Note the
        order of the entiites must be the order that the roles were
        defined in the descriptor."""
        if obj1 is None or obj2 is None:
            print "Tried to relate an object to None for relation %s: %s %s" % (relation, obj1, obj2)
        
        role1, role2 = self.relationRegistry.getRelation(relation)
        ref1 = self.buildRef(obj1)
        ref2 = self.buildRef(obj2)
        pk1 = self.buildKey(obj1, relation, role1.name)
        pk2 = self.buildKey(obj2, relation, role2.name)

        try:
            rr = self._storage.load(role1.multiplicity, pk1)
        except KeyError:
            rr = role1.multiplicity(ref1, relation, role1.name)
            self._storage.create(rr)
        rr.relate(ref2, self._storage)

        try:
            rr = self._storage.load(role2.multiplicity, pk2)
        except:
            rr = role2.multiplicity(ref2, relation, role2.name)
            self._storage.create(rr)
        rr.relate(ref1, self._storage)

        self._storage.update(obj1)
        self._storage.update(obj2)

        # Dear diary, I met a new man and I think we might just get
        # along
        
        diary = self._storage.load(RelationDiary,
                                   RelationDiary._get_primaryKey())
        
        diary.log("relate", ref1, ref2, relation)


    def unrelate( self, obj1, obj2, relation ):
        # obj1 = From, obj2 = To
        """ Unrelates two entities based on the relation name.  Note the
        order of the entiites must be the order that the roles were
        defined in the descriptor."""

        role1, role2 = self.relationRegistry.getRelation(relation)
        ref1 = self.buildRef(obj1)
        ref2 = self.buildRef(obj2)
        pk1 = self.buildKey(obj1, relation, role1.name)
        pk2 = self.buildKey(obj2, relation, role2.name)
        
        rr = self._storage.load(role1.multiplicity, pk1)
        rr.unrelate(ref2, self._storage)

        rr = self._storage.load(role2.multiplicity, pk2)
        rr.unrelate(ref1, self._storage)

        # Let the Persistence Machinery know the object has been updated.
        self._storage.update(obj1)
        self._storage.update(obj2)

        # Dear diary, I just went through a horrible break up. I hate
        # him I hate him I hate him!
        diary = self._storage.load(RelationDiary,
                                   RelationDiary._get_primaryKey())

        diary.log("unrelate", ref1, ref2, relation)


# NOTE: Up until this point we've been removing the silly from and to
# nomenclature from the code. But in order to remain backwards
# compatability with the QueryService we're leaving the rest of the
# functions as is. If at some point in the future we finally do
# repalce the QueryService, we'll change the function names

# FYI: obj1 = From, obj2 = To
        
        
    def isPointingTo(self, obj1, obj2, relation):
        # Q. Does anyone ever call this method?
        role1, role2 = self.relationRegistry.getRelation(relation)
        
        pk1 = self.buildKey(obj1, relation, role1.name)
        ref2 = self.buildRef(obj2)

        try:
            rr = self._storage.load(role1.multiplicity, pk1)
            return rr.isRelated(ref2)

        except KeyError:
            return 0

    def findPointingFrom( self, obj1, relation ):
        role1, role2 = self.relationRegistry.getRelation(relation)
        key = self.buildKey(obj1, relation, role1.name)
        return self._findPointing(key, role1)


    def findPointingTo( self, obj2, relation ):
        role1, role2 = self.relationRegistry.getRelation(relation)
        key = self.buildKey(obj2, relation, role2.name)
        return self._findPointing(key, role2)


    def _findPointing(self, pk, role):
        try:
            rr = self._storage.load(role.multiplicity, pk)
        except KeyError:
            return []

        results = rr.value
        if not type(results) == Set: #OOSet:
            if rr.value is None:
                return ()

            results = (rr.value,)

        # Set's don't support __getitem__ so convert them to a tuple
        return LazyMap(self.getEntity, tuple(results))


    def getKeysForRelations(self, obj, relation, roleName):
        roles = self.relationRegistry.getRelation(relation)

        role = None
        for r in roles:
            if r.name == roleName:
                role = r
                break
        assert role is not None
        
        key = self.buildKey(obj, relation, roleName)
        rr = self._storage.load(role.multiplicity, key)
        return rr.value


    def removeAllRelationsForEntity(self, entity):
        """Removes all the relations for a give entity. This method

        should be called whenever an entity is deleted."""

        # Hooray, since we're currently using postgres, and our
        # constraints take care of all this, this entire function
        # is not only reduntant, it's downright broken.  This is
        # kind of a bad thing, as it ties us to the storage...we'll
        # need to think of some way of breaking this functionality
        # out of the core package and into the storage plugins.
        return
    
        # List off all other entities that have changend do to this
        # operation.

        victims = set()#OOset()

        diary = self._storage.load(RelationDiary,
                                   RelationDiary._get_primaryKey())


        ds = self.eds
        for relation,role,position in \
                self.relationRegistry.getRelationsForEntity(ds.getName(entity.__class__)):

           
            
            otherRole = self.relationRegistry.getReciprocal(relation, role.name)
            
            pk = self.buildKey(entity, relation, role.name)
            ref = self.buildRef(entity)

            try:
                rr = self._storage.load(role.multiplicity, pk)
            except KeyError:
                # Just because we can have a relation, doesn't mean
                # one has been setup yet.
                continue

            value = rr.value
            if value is None:
                # Relation was already removed in a previous call, ignore
                continue
                
            if type(value) is not Set: #OOSet:
                value = (value, )
            else:
                # Unrelating modifies this value.  Modifying
                # a value you're looping over is baaaad
                value = set(value) #OOset(value)
                
            for oref in value:
                victims.add(oref)
                opk = oref + (relation, otherRole.name)
                orr = self._storage.load(otherRole.multiplicity, opk)
                orr.unrelate(ref, self._storage)
                rr.unrelate(oref, self._storage)
                if position == 0:
                    diary.log("unrelate", ref, oref, relation)
                else:
                    diary.log("unrelate", oref, ref, relation)

            self._storage.remove(rr)

        for homeId, pk in victims:
            home = Server.getHome(homeId)
            obj = home.findByPrimaryKey(pk)
            self._storage.update(obj)
        


    def buildRef(self, entity):
        homeId = self.eds.getHome(self.eds.getName(entity.__class__)).homeId
        key = (homeId,
               entity._get_primaryKey())

        return key

    def buildKey(self, entity, relation, roleName):
        homeId = self.eds.getHome(self.eds.getName(entity.__class__)).homeId

        key = (homeId,
               entity._get_primaryKey(),
               relation,
               roleName)

        return key

    def getEntity(self, result):
        homeKey, pk = result
        home = self.componentRegistry.lookup(homeKey)
        return home.findByPrimaryKey(pk)




class RelationshipScrubber:
    """Removes relationships when an entity is deleted."""
    # TODO: Rename this class to divorce attorney

    def __init__(self, RelationService):
        self.RelationService = RelationService

    def handleMessage(self, msg):
        """Add the ReltionshipScrubber as an observer to each home that's registered. """
        if isinstance(msg, NewEntityEvent):
            # add ourselves in as an observer
            msg.entityHome.addObserver(self)

    def OnRemove(self, entity):
        """ When an object is created """
        self.RelationService.removeAllRelationsForEntity(entity)
        
    def OnCreate(self, entity):
        pass

    def OnUpdate(self, entity):
        pass

class RelationDiary(object):
    __slots__ = { "diary": list,
                  "_storage": object, # Mapper interface
                  "_records": dict,
                  "_ps" : object # PersistenceService
                  }

    def __init__(self, storage):
        self._storage = storage
        self._records={}
        self.diary = []

    def log(self, action, ref1, ref2, relation):
        # Dear diary today I saw....
        self.diary.append((action, ref1, ref2, relation))
        self._ps.update(self)

    def load(self, klass, primaryKey):
        if not  self._records.has_key(primaryKey):
            try:
                self._records[primaryKey] = self._storage.load(klass, primaryKey)
            except KeyError:
                # Relation was created in this transaction.  Create our
                # own copy.
                ref = primaryKey[:2]
                relation = primaryKey[2]
                role = primaryKey[3]
                relation = klass(ref, relation, role)
                self._records[primaryKey] = relation

        return self._records[primaryKey]

    def update(self, object):

        """All records loaded through the diary will be udated"""
        
        pass

    def getRecords(self):
        return self._records.values()

    def _get_primaryKey():
        # Needed to make the PersistenceService happy
        return "RelationDiary"
    _get_primaryKey = staticmethod(_get_primaryKey)

    def _printRecords(self):
        """ A Debug method """
        print '----------------------------------'
        for key, record in self._records.items():
            value = record.value
            if type(value) == tuple:
                value = (value,)
            print key
            for v in value:
                print '\t%s' % (v,)

        print '----------------------------------'


class RelationRecord(object):
    relationRegistry = outlet("RelationRegistryService")
    
    def relate(self, ref, storage):
        raise NotImplementedError()

    def unrelate(self, ref, storage):
        raise NotImplementedError()

    def isRelated(self, ref):
        raise NotImplementedError()

    def _get_primaryKey(self):
        return self.primaryKey
    
class SingleRelationRecord(RelationRecord):
    __slots__ = {"primaryKey" : tuple, # (home id, primaryKey, relation name)
                 "relation": str, # Name of the relation
                 "ref": tuple,
                 "role": str,
                 "value"  : tuple, # A tuple representing the other RR's primaryKey
                 }

    name = 'one'
    def __init__(self, ref, relation, role):
        self.primaryKey = ref + (relation, role)
        self.ref = ref
        self.relation = relation
        self.role = role
        self.value = None
        #self._storage = storage

    def relate(self, ref,  storage):
        # load the relation record for ob1

        if self.value == ref:
            # Already related so nothing to do
            return
        
        if self.value is not None:

            # We're related to something else, so instead of being a
            # slut do the right thing and break up first.
            
            otherRole = self.relationRegistry.getReciprocal(self.relation, self.role)
            pk = self.value + (self.relation, otherRole.name)

            klass = otherRole.multiplicity
            orr = storage.load(klass, pk)
            orr.unrelate(self.ref, storage)
            
        self.value = ref
        storage.update(self)

    def unrelate(self, ref, storage):
        assert self.value == ref
        
        self.value = None
        storage.update(self)

    def isRelated(self, ref):
        return self.value == ref

class ManyRelationRecord(RelationRecord):
    __slots__ = {"primaryKey" : tuple, # (home id, primaryKey, relation name)
                 "relation": str, # Name of the relation
                 "ref": tuple,
                 "role": str,
                 "value"  : tuple, # Tuple of tuples
                 }

    name = 'many'
    def __init__(self, ref, relation, role):
        self.primaryKey = ref + (relation, role)
        self.ref = ref
        self.relation = relation
        self.role = role
        #self.value = OOset()

    def getValue(self):
        try:
            return self._value
        except AttributeError:
            self._value = set() #OOset()
            return self._value
    
    def setValue(self, value):
        # Make a copy
        self._value = set(value) #OOset(value)
        
    value = property(getValue, setValue)
    def relate(self, ref, storage):
        if ref not in self.value:
            self.value.add(ref)
            storage.update(self)

    def unrelate(self, ref, storage):
        try:
            self.value.remove(ref)
        except KeyError:
            pass

        storage.update(self)

    def isRelated(self, ref):
        return ref in self.value


class Role(object):
    __slots__ = {"description": str,
                 "name": str,
                 "multiplicity": RelationRecord,
                 "cascade_delete": bool,
                 "source": str,
                 "field": str}
    classMap = {"one": SingleRelationRecord,
                "many":ManyRelationRecord}
    
    
    def __init__(self, multiplicity, source):
        self.description = ""
        self.name = ""
        self.multiplicity = multiplicity
        self.cascade_delete = False
        self.source = source.lower()
        self.field = ""

    def __repr__(self):
        return "<Role %s %s>" % (self.multiplicity, self.source)


        

class RelationRegistryService(object):
    classMap = {"one": SingleRelationRecord,
                "many":ManyRelationRecord}

    ds = outlet('EntityDescriptionService')

    def __init__(self):
        self._relations = {}
        self._entityRelations = {}
        
    def addRelation(self, name, role1, role2):
        #print "Adding: ", name, role1, role2

        # Role names need to be defined for our system, but EJB makes
        # it optional. In fact the only constraint is that the role
        # name has to be unique within the relation. So if there's no
        # name we default it to either role1 or role2 respectivly.

        name = str(name)
        role1.name =  role1.name or "role1"
        role2.name =  role2.name or "role2"

        # Role types are swapped from the logical order.  If a role
        # is defined as having a multiplicity of Single, then the
        # OPPOSITE role must have a single pointer back to this role
        # thus it gets a SingleRelationRecord.  If a role is defined
        # as Many, the opposite object can store multiple pointers
        # and thus needs to be a ManyRelationRecord.
        multiclass1 = self.classMap[role1.multiplicity.lower()]
        multiclass2 = self.classMap[role2.multiplicity.lower()]
        role1.multiplicity = multiclass2
        role2.multiplicity = multiclass1

        roles = (role1, role2)
        self._relations[name] = roles

        #ds = Server.getService("EntityDescriptionService")
        ds = self.ds
        for x in range(2):
            role = roles[x]
            entityName = role.source
            entityRelations = self._entityRelations.get(entityName, [])

            # Log the relation the entitiy is envolved, what rolename
            # and wether it's the first or second role
            
            entityRelations.append((name, role, x))
            self._entityRelations[entityName] = entityRelations

            # If the role has a field attribute defined, notify the
            # EntityDescrptionService about the new field.
            if role.field:
                # Get the name of the entity we point to
                if x == 0:
                    otherName = role2.source
                else:
                    otherName = role1.source
                klass = ds.getClassForEntity(otherName)
                field = Field(role.field, klass, name, role.name)
                ds.addField(entityName, field)

    def getRelation(self, name):
        if self._relations.has_key(name):
            return self._relations[name]
        else:
            raise RuntimeError("Relation '%s' has not been defined." % name)

    def getRelationNames(self):
        return self._relations.keys()

    def getRelationsForEntity(self, entityName):
        """
        entityName should be the class name (ie. ciLead)
        """
        return self._entityRelations.get(entityName, [])

    def getRelationRole(self, relation, roleName):
        """
        Returns the role objects for the given role/relation
        """
        role1, role2 = self.getRelation(relation)
        if role1.name == roleName:
            return role1
        elif role2.name == roleName:
            return role2
        else:
            raise RuntimeError("No matching rolename %s in relation "\
                               "%s!" % (roleName, relation))
        
    def getReciprocal(self, relation, roleName):
        role1, role2 = self.getRelation(relation)
        if role1.name == roleName:
            return role2
        elif role2.name == roleName:
            return role1
        else:
            raise RuntimeError("No matching rolename %s in relation "\
                               "%s!" % (roleName, relation))
                               
#RelationRegistry = RelationRegistry()
#Server.registerService(RelationRegistry, "RelationRegistryService")
    
class RelationHandler(DefaultHandler):

    def __init__(self):
        self.relation = None
        self._data = []
        self._ignoreData = True
    
    def startElement(self, name, attrs):
        if name == "relationships":
            self._ignoreData = False
        elif name == "ejb-relation":
            self._roles = []
        elif name == "ejb-relationship-role":
            self._description = ""
            self._ejb_name = ""
            self._multiplicity = None
            self._cascade_delete = False
            self._cmr_field_name = ""
            self._ejb_relationship_role_name = ""

    def characters(self, ch):
        if self._ignoreData:
            return
        self._data.append(ch)

    def endElement(self, name):
        data = str("".join(self._data).strip())
        if name == "ejb-relation":
            self.relationRegistry.addRelation(str(self._ejb_relation_name), *self._roles)

        elif name == "ejb-relationship-role":
            r = Role(self._multiplicity, self._ejb_name)
            r.description = str(self._description)
            r.field = str(self._cmr_field_name)
            r.cascade_delete = self._cascade_delete
            r.name = str(self._ejb_relationship_role_name)
            
            self._roles.append(r)
        elif name == "relationships":
            self._ignoreData = True
            #print RelationRegistry.getRelationNames()
        elif name == 'cascade-delete':
            # Since this is an empty tag, handle it differently
            self._cascade_delete = True
        elif data:
            name = name.replace('-', '_')
            #print 'Setting :%s = "%s"' % (name, data)
            setattr(self, "_" + name, data)
            
        self._data = []
#RelationHandler = RelationHandler()


from threading import Lock
class InMemoryRelationMapper:
    """Used for testing/debugging"""
    def __init__(self):
        self.relations = {}

    def create(self, object):

        if isinstance(object, RelationRecord):
            #wconn["Relations"][object.primaryKey] = object.value
            # Do nothing as the diary will handle persisting the object later
            return
        else:

            raise RuntimeError("Asked to create an object whose type "\
                               "%s we don't support." % type(object))

    def load(self, klass, primaryKey):
        if klass == RelationDiary:
            return RelationDiary(self)
        else:
            value = self.relations[primaryKey]
            ref = primaryKey[:2]
            relation = primaryKey[2]
            role = primaryKey[3]
            k = klass(ref, relation, role)
            k.value = value
            return k

    def update(self, object):
        
        # Ignore changes made to RelationRecords, wait for the diary
        # and replay changes directly to the DB
        if type(object) == RelationDiary:

            for action, ref1, ref2, relation in object.diary:
                role1, role2 = self.relationRegistry.getRelation(relation)
                pk1 = ref1 + (relation, role1.name)
                pk2 = ref2 + (relation, role2.name)

                obj1 = object.load(role1.multiplicity, pk1)
                obj2 = object.load(role2.multiplicity, pk2)

                if action == "relate":
                    obj1.relate(ref2, object)
                    obj2.relate(ref1, object)
                elif action == "unrelate":
                    obj1.unrelate(ref2, object)
                    obj2.unrelate(ref1, object)

            for record in object.getRecords():
                self.relations[record.primaryKey] = record.value

    def remove(self, object):
        if isinstance(object, RelationRecord):
            del self.relations[object._get_primaryKey()]
        else:
            raise RuntimeError("Asked to remove an object whose type "\
                               "%s we don't support." % type(object))
