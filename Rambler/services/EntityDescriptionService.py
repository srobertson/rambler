import sys
from Rambler import field, option, outlet,load_classes
from Rambler.utils import getClass
from Rambler.handlers import DefaultHandler

from Rambler.ciHomeBase import Entity
from Rambler.CompBinder import Binding
from Rambler.Events import EntityRegisteredEvent
from StringIO import StringIO

#NOTE: This module is in a transition state. Originally entities had
#home objects (which were balically factory objects) which were 
#seperate classs from the actual entities.


default_bindings = [ Binding('EntityDescriptionService',object, 'EntityDescriptionService'),

                     # EDS is first so when eventChannel is bound we can
                     # publish the right event another optinon is to have the
                     # home signal EDS when the CompReg instantiates it...
                     
                     Binding('EventService', object, 'eventChannel'),
                     Binding('RelationService', object, 'RelationService'),
                     
                     Binding('PersistenceService',object,'PersistenceService'),
                     #Binding('KeyGenService',object,'KeyGenService'),
                     Binding('SessionManager',object,'sessionManager'),
                     Binding('ErrorFactory', object, 'errorFactory')]

log=None

class EntityDescriptionService(object):
    """Provides information about entities, such as their
    class, attributes and relationships."""
    
    componentRegistry = outlet('ComponentRegistry')
    eventChannel = outlet('EventService')
    log = outlet('LogService')
    configService = outlet('ConfigService')
    app = outlet('Application')
    #appName = option('application','name')
    
    def __init__(self):
        self._entityInfo={}
        self._entityNames = {}
        self._entityClasses = {}
        self._entityHomeClasses = {}
        self._registeredNames = []
        self._entityInterfaces = {}
        self._entityDescriptions = {}

    def assembled(self):
        self.eventChannel.registerEvent("EntityRegistered", self, EntityRegisteredEvent)
        # cheesy hack to get the logger bound to this comp to other
        # methods in this module. Hopefully when we rework this module
        # to auto generate homes this will go away
        global log
        log = self.log
        
        mod_names = ['Rambler', self.app.name] + [ext.name for ext in self.app.config.extensions]
        for mod_name in mod_names:
          mod_full_name = mod_name + ".models"
          for cls in load_classes(mod_full_name, object):
            if hasattr(cls, "provides"):
              name = cls.provides
            else:
              name = cls.__name__
            try:
              class_path =  cls.__module__ + "." + cls.__name__
              self.addNewStyleEntityInfo(name, cls,class_path,[],[])
            except TypeError:
              # thrown if the constructor takes arguments, if so the class isn't
              # a service. We need a better way of identifying component types w/o
              # require an import of a class that makes a code dependency
              pass


    def addNewStyleEntityInfo(self, entityName, interface, classPath, entityInfo, bindings, description="(Uknown)"):
        compName = entityName # Component names are case sensitive

        entityName = entityName.lower() 
        if entityName in self._registeredNames:
            raise Exception, "An entity with the name '%s' has already been registered with the Entity Description Service" % (entityName)
        self._registeredNames.append(entityName)

        entityClass = getClass(classPath)
        for fieldDef in entityInfo:
            #import pdb; pdb.set_trace()
            # Add any properties that weren't  implemented by the base class.
            if not entityClass.__dict__.has_key(fieldDef.name):
                setattr(entityClass, fieldDef.name, 
                        field(fieldDef.name, fieldDef.type))



        # TODO: Consider not deriving classes and just modifying the class in place...
        #entityClass = deriveClass(compName, getClass(classPath), entityInfo)
        #modClass(entityClass)

        # Store the fields keyed both on the EntityName and the Class itself
        self._entityInfo[entityName] = entityInfo
        self._entityNames[entityName] = entityName
        
        # New style entities share the same class, however I'm leaving this out to see what breaks
        #self._entityHomeClasses[entityName] = entityClass

        self._entityInterfaces[entityName] = interface
        self._entityDescriptions[entityName] = description
        
        self.linkEntityInfo(entityClass, entityName)
        self._entityClasses[entityName] = entityClass

        bindings.extend(default_bindings)

        self.componentRegistry.addComponent(
            compName,
            entityClass)#, 
            #bindings) 

        self.eventChannel.publishEvent("EntityRegistered", self,
                                  EntityRegisteredEvent(entityName, self))

        

    def addEntityInfo(self, entityName, interface, entityClassName, homeClassName, entityInfo, bindings, description="(No Description)"):
        entityName = entityName.lower() 
        if entityName in self._registeredNames:
            raise Exception, "An entity with the name '%s' has already been registered with the Entity Description Service" % (entityName)
        self._registeredNames.append(entityName)

        if entityClassName:
            baseEntityClass = getClass(entityClassName)
        else:
            baseEntityClass = Entity

        homeClass = getClass(homeClassName)

        # For the moment only derive classes that don't have an
        # implementation, when we auto generate homes, we'll then
        # derive everything.
        
        if baseEntityClass == Entity:
            entityClass = deriveClass(entityName, baseEntityClass, entityInfo)
        else:
            entityClass = baseEntityClass

        
        modClass(entityClass)

        # Store the fields keyed both on the EntityName and the Class itself
        self._entityInfo[entityName] = entityInfo
        self._entityNames[entityName] = entityName
        self._entityHomeClasses[entityName] = homeClass
        self._entityInterfaces[entityName] = interface
        self._entityDescriptions[entityName] = description

        self.linkEntityInfo(entityClass, entityName)


        
        if baseEntityClass != Entity:

            # Retain backwards compatability for those object's
            # implemnteng their own homes, this will allow us to find
            # the derived class using the base class specified in the
            # descriptor.
            self.linkEntityInfo(baseEntityClass, entityName)
            

        self._entityClasses[entityName] = entityClass

        bindings.extend(default_bindings)
        self.componentRegistry.addComponent(
            homeClass.homeId,
            homeClass(), # Note we start the home here
            bindings)

        # Stupid hack, so that instances of entities have the same
        # services available to them as their home objects TODO: Make
        # entities like sessions, where you don't implement the home
        # class, but instead the home _is_ the class for the entitiy
        
        self.componentRegistry.addComponent(
            entityName,
            entityClass, 
            bindings) 

        self.eventChannel.publishEvent("EntityRegistered", self,
                                  EntityRegisteredEvent(entityName, self))

    def linkEntityInfo(self, newKey, oldKey):
        """
        Adds a new valid key for an existing entityInfo, allowing
        you to look up the entity info via the new key.
        """
        self._entityInfo[newKey] = self._entityInfo[oldKey]
        self._entityNames[newKey] = self._entityNames[oldKey]
        
    def getFields(self, entityNameOrClass):

        """Returns a list of fields for the given entityName. A
        KeyError exception is thrown if there's nothing known about
        that entity."""
        if hasattr(entityNameOrClass, 'lower'):
            entityNameOrClass = entityNameOrClass.lower()

        fields = self._entityInfo.get(entityNameOrClass)
        if fields is None:
            raise KeyError("Nothing known about entity named '%s'" % entityNameOrClass)
        return fields
        
    
    def getField(self, entityNameOrClass, fieldName):
        if hasattr(entityNameOrClass, 'lower'):
            entityNameOrClass = entityNameOrClass.lower()

        fields = self.getFields(entityNameOrClass)

        for field in fields:
            if field.name == fieldName:
                return field

        # If we made it this far we don't have a field with the given
        # name
        raise AttributeError("%s has no field named %s" %
                             (entityNameOrClass,fieldName))

    def addField(self, entityNameOrClass, field):
        """Adds a new field to a registered entity.  Used by the
        RelationService to add relational fields.
        """
        if hasattr(entityNameOrClass, 'lower'):
            entityNameOrClass = entityNameOrClass.lower()

        info = self._entityInfo[entityNameOrClass]
        info.append(field)

    def getClassForEntity(self, entityName):
        """Returns the class for the specified entityName as
        defined in the descriptor."""
        return self._entityClasses[entityName.lower()]
        
    def getName(self, value):
        """Given a class or name, returns the entity name as
        defined in the descriptor"""
        return self._entityNames[value]

    def getEntityNames(self):
        """Return a list of all the entityNames we know about"""
        return self._registeredNames
    
    def getHome(self, entityName):
        """Returns the home class for the given entity (NOT
        and instance)"""

        try:
            # DEPRICATE ME: 
            return self._entityHomeClasses[entityName]
        except KeyError:
            # If we don't have it, then it's probbably a new style
            # entity, in which case the home is the same thing as a class
            return self.getClassForEntity(entityName)

    def getInterface(self, entityName):
        return self._entityInterfaces[entityName]

    def getDescription(self, entityName):
        return self._entityDescriptions[entityName]

#EntityDescriptionService = EntityDescriptionService()

class Field(object):
    def __init__(self, name, eType, relationName=None, role=None, description="(No Description)"):
        self._name = name
        if type(eType) != type:
            self._type = getClass(eType)
        else:
            self._type = eType
        self._relationName = relationName
        self._role = role
        
        self._description = description
        
        if relationName is not None and role is None:
            raise ValueError("Role is required when Field is a relation")
        
    def getName(self):
        return self._name
    name = property(getName)

    def getType(self):
        return self._type 
    type = property(getType)

    def isRelation(self):
        return self._relationName is not None

    def getRelationName(self):
        if self.isRelation():
            return self._relationName
        else:
            raise ValueError("Field is not a relation")
    relationName = property(getRelationName)
    
    def getRole(self):
        if self.isRelation():
            return self._role
        else:
            raise ValueError("Field is not a relation")
    role = property(getRole)

    def getDescription(self):
        return self._description
    description = property(getDescription)


from xml.sax.saxutils import XMLGenerator
class EntityHandler(DefaultHandler):

    def __init__(self):
        self._data = []
        self._ignoreData = True
        
        
        self._subdoc = None
        self._subdocname = None
        self._subdocdata = None
        
    
    def startElement(self, name, attrs):

        if self._subdoc:
            self._subdoc.startElement(name, attrs)
            return
                             
        if attrs.has_key('xmlns'):

            # This element defines it's own namespace, which means
            # that it's children nodes sholud be collected into data
            # For examlpe:
            # <field-description xmlns="http://docbook.org/ns/docbook">
            #  <para>This description uses docbook to format</para>
            #  <para> it's text</para>
            # </field-description>
            #
            # because the node defines an attribute named xmlns,
            # the attribute self._fild_description will equal 
            #  <para>This description uses docbook to format</para>
            #  <para> it's text</para>

            self._subdocname = name
            self._subdocdata = StringIO()
            self._subdoc = XMLGenerator(self._subdocdata)
        
            
        if name == "entity":

            self._fields = []
            self._ejb_name = ""
            self._remote = ""
            self._home = ""
            self._ejb_class = ""
            self._bindings=[]
            self._ignoreData = False
            self._description = "(No Description)"

        elif name == "cmp-field":
            self._field_name = ""
            self._field_type = ""
            self._field_description ="(No Description)"


            # Cute looks like we've been blanking out these fields, but they're never used wrong fields
            #self._entityName = ""
            #self._entityClassName = ""
            #self._homeClassName = ""

        elif name == "bind":
            # This tag is optional in the file.  If we didn't reset it
            # for each bind tag, we'd end up keeping the first
            # specified binding for each of the following tags until
            # another bind tag specified it.
            
            self._allow_unassembled = False

    def characters(self, ch):
        if self._subdoc:
            self._subdoc.characters(ch)
            return

        if self._ignoreData:
            return
        
        self._data.append(ch)

    def endElement(self, name):
        if self._subdoc:
            if name == self._subdocname:
                # The element that started the subdoc ended so get the
                # data ready.

                self._data.append(self._subdocdata.getvalue())
                self._subdoc = None
                self._subdocdata = None
                self._subdocname = None
            else:
                # The element with the name space hasn't ended yet
                # keep reserializing data back to an xml string.
                
                self._subdoc.endElement(name)
                return


        if self._ignoreData:
            return
        
        data = "".join(self._data).strip()
        if name == "entity":
            if hasattr(self,'_class'):
                # it's a new style entity
                self.eds.addNewStyleEntityInfo(str(self._ejb_name),
                                       self._remote,
                                       self._class,
                                       self._fields,
                                       self._bindings,
                                       description = self._description
                                       )

            else:
                self.eds.addEntityInfo(str(self._ejb_name),
                                       self._remote,
                                       self._ejb_class,
                                       self._home,
                                       self._fields,
                                       self._bindings,
                                       description = self._description
                                       )
            self._ignoreData = True

        elif name == "cmp-field":
            field = Field(str(self._field_name), str(self._field_type),
                          description=self._field_description)
            self._fields.append(field)

             
        elif name == "bind":

            self._bindings.append(
                Binding(self._component, object, self._attribute, self._allow_unassembled)
                )

        elif name == "allow-unassembled":
            # Singleton element with no data.  If present, means true
            self._allow_unassembled = True

        elif data:
            name = '_' + name.replace('-', '_')
            #print 'Setting :%s = "%s"' % (name, data)
            setattr(self, name, data)
            
        self._data = []



props="""
def _get_%(name)s(self):
    try:
        return self.__%(name)s
    except AttributeError:
        # Looks like the attribute was never set
        return None

def _set_%(name)s(self, val):

    # Sigh, can't do type checknig for now, because we don't have a
    # good method to access the types whith these dynamically
    # generated funcions. The foollowing would be an example of how we
    # could do this if we ever implemented a type service

    # typeService = compReg.getService('TypeService')

    # if not typeService.isType(val, %(type)s):
    #    raise ValueError('%(name)s must be of type %(type)s not %%s' %% valtype.__name__)
    self.__%(name)s = val
"""


def add_property(cls, field):
    # Generate the get and set method for this field
    name = field.name
    functionDefs = props % {'name':field.name,
                            'type':field.type.__name__}


    # create a get and set method in this name space
    exec functionDefs

    getmethod = '_get_' + name
    setmethod = '_set_' + name

    # Add the generated get and set methods if the base class hasn't
    # implemented them.


    if not hasattr(cls, getmethod):
        log.debug("Implementing %s" % getmethod)
        setattr(cls, getmethod, locals()[getmethod])

    if not hasattr(cls, setmethod):
        log.debug("Implementing %s" % setmethod)
        setattr(cls, setmethod, locals()[setmethod])

##    if not hasattr(cls, name):
        
##        # If the baseclass didn't implement the property, grab the
##        # get/set method's either the ones we may have just
##        # implemented or the ones possibly implemented by the
##        # baseclass and add them as a property
##        log.debug("Converting get and set methods to a property")
##        get = getattr(cls, getmethod)
##        set = getattr(cls, setmethod)
##        setattr(cls, name, property(get, set))


get_home = """
def _get_home(klass):
    name = klass.EntityDescriptionService.getName(klass)
    return klass.EntityDescriptionService.getHome(name)
_get_home = classmethod(_get_home)
"""

constructor = """
def __init__(self, primaryKey):
    self._set_primaryKey(primaryKey)
"""
def deriveClass(name, baseclass, entityInfo):
    log.debug("Deriving class for %s %s" % (name, baseclass))

    cls = type(name, (baseclass,), {})
    for field in entityInfo:
        # Add any properties that weren't  implemented by the base class.
        
        if not hasattr(cls, field.name):
            add_property(cls, field)

    # Generate the _get_home method
    exec get_home 
    cls._get_home = _get_home

    # Generate the __init__ method

    exec constructor
    cls.__init__ = __init__

    return cls

    
def modClass(klass):

    """Replaces all method's that start with _set_ with ones that notify the
    Persistence server the object has been updated.  Also set up a property
    that uses the _get_ and _set_ methods so we don't have to call those
    directly."""
    
    for attr in dir(klass):
        if attr.startswith("_get_"):
            propname = attr[5:]
            setname = "_set_" + propname
            if hasattr(klass, setname):
                setattr(klass, setname, setter(getattr(klass, setname)))
                setattr(klass, propname, property(getattr(klass, attr),
                                                  getattr(klass, setname)))
            else:
                setattr(klass, propname, property(getattr(klass, attr)))



class setter(object):
    """Register the object as changing when this method is called."""

    def __init__(self, method):
        self._method = method
        
    def __get__(self, obj, klass):
        return settermethod(obj, self._method)

class settermethod(object):
    def __init__(self,instance, method):
        self._method = method
        self._instance = instance
        self.__name__ = method.__name__
        
    def __call__(self, *args, **kw):
        obj = self._instance
        
        # If the object has the __loading__ attribute dirty flagging is
        # disabled.
        if hasattr(obj, "__watching__") and not hasattr(obj, "__dirty__"):
            obj.__dirty__ = True
            #Server.getService("PersistenceService").update(obj)
            obj.PersistenceService.update(obj)

        if obj is not None:
            # The use the stored instance as the first argument
            args = (self._instance, ) + args

        return apply(self._method, args, kw)

