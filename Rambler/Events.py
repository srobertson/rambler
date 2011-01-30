class EntityRegisteredEvent(object):
    """This event is broadcast by the server everytime a new entity is
    registered"""
    # This depricates NewEntityEvent with something more general purpose

    def __init__(self, entityName, descriptionService):
        self.entityName = entityName
        self.descriptionService = descriptionService
        
    def getFields(self):

        """Returns a list of fields for the given entityName. A
        KeyError exception is thrown if there's nothing known about
        that entity."""

        return self.descriptionService.getFields(self.entityName)
    
    def getField(self, fieldName):
        return self.descriptionService.getFields(self.entityName, fieldName)

    def getClassForEntity(self):
        """Returns the class for the specified entityName as
        defined in the descriptor."""
        return self.descriptionService.getClassForEntity(self.entityName)
        
    
    def getHome(self):
        """Returns the home class for the given entity (NOT
        and instance)"""

        return self.descriptionService.getHome(self.entityName)


    def getInterface(self):
        return self.descriptionService.getInterface(self.entityName)

        
    

class NewEntityEvent(object):

    """This event is broadcast by the server everytime a new entity is
    registered"""
    
    def __init__(self, entityHome, entityClass, objrefClass):
        self.entityClass = entityClass
        self.entityHome = entityHome
        self.objrefClass = objrefClass
        
class InitializeEvent(object):

    """ This event is broadcast by the server when there is a brand new
    database that needs to be initialized """

    pass

class ShutdownEvent(object):
    """ This event is broadcast by the server when it is about to do
    a graceful shutdown """
    pass

class LoginEvent(object):
    """Published by IdentityManagers when users login."""

    def __init__(self, token, username, password, callback):
        self.token    = token
        self.username = username
        self.password = password
        self.callback = callback

from types import InstanceType
Vote = InstanceType
