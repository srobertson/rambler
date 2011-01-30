class UnitOfWork:
    NOT_EXIST = 0
    NEW       = 1
    CLEAN     = 2
    DIRTY     = 3
    REMOVED   = 4

    def __init__(self):
        self.clear()
        
    
    def registerClean(self, obj):

        """Registers an object that is unchanged from the database. An
        object can not be registered as clean if it exists in any
        other state."""

        pk = obj._get_primaryKey()
        status = self.getStatus(pk)
        if status == self.NOT_EXIST:
            self._objects[self.CLEAN][pk] = obj
            self._obj_states[pk] = self.CLEAN
        else:

            raise ValueError, "Object with the primary key of %s" \
                  "already registered with the following status %s" % (pk,status)
            

    def registerDirty(self, obj):
        
        """Marks an object as being modified in the current
        transaction and needing to be updated. An object can only be
        registered as dirty if it's been previously registered as
        clean."""

        pk = obj._get_primaryKey()
        status = self.getStatus(pk)
        if status == self.CLEAN:
            obj = self.get(pk)
            del self._objects[status][pk] 
            self._objects[self.DIRTY][pk] = obj
            self._obj_states[pk] = self.DIRTY
        else:

            raise ValueError, "Object with the primary key of %s" \
                  " with an invalid status of %s" % (pk,status)


    def registerRemoved(self, obj):

        """Marks an object as needing to be removed at the end of the
        transaction. An object can only be registered as removed if it
        was registered as new, clean or dirty prior to being
        removed. """

        pk = obj._get_primaryKey()
        status = self.getStatus(pk)
        if status != self.NOT_EXIST:
            obj = self.get(pk)
            del self._objects[status][pk] 
            self._objects[self.REMOVED][pk] = obj
            self._obj_states[pk] = self.REMOVED
        else:

            raise ValueError, "Object with the primary key of %s, has not been registered." 

        

    def registerNew(self, obj):

        """Marks an object as being newly created in the current
        transaction. An object can only be registered new if it has no
        previous state."""

        pk = obj._get_primaryKey()
        status = self.getStatus(pk)
        if status == self.NOT_EXIST:
            self._objects[self.NEW][pk] = obj
            self._obj_states[pk] = self.NEW
        else:

            raise ValueError, "Object with the primary key of %s" \
                  " already registered with the following status %s" % (pk,status)
        



    def getStatus(self, primaryKey):
        """Returns the status of the given object. The status can be either
        - NEW
        - DIRTY
        - REMOVED
        - CLEAN
        - NOT_EXIST
        """
        return self._obj_states.get(primaryKey, self.NOT_EXIST)

    def get(self, primaryKey, default=None):

        """Returns the given object or the default value if the object
        doesn't exist."""
        status = self.getStatus(primaryKey)
        if status == self.NOT_EXIST:
            return default
        else:
            return self._objects[status][primaryKey]

    def getNew(self):

        """Returns a list of all new objects in the current
        transaction."""
        return self._objects[self.NEW].values()
        
    def getClean(self):

        """Returns a list of all the clean objects in the current
        transaction."""

        return self._objects[self.CLEAN].values()

    def getDirty(self):

        """Returns a list of all the dirty objects in the current
        transaction."""
        return self._objects[self.DIRTY].values()

    def getRemoved(self):

        """Returns a list of all objects that need to be removedi in
        the current transaction."""
        return self._objects[self.REMOVED].values()

    def clear(self):

        """Removes all the object from every possible state."""
        self._objects = {
            self.NEW:{},
            self.CLEAN:{},
            self.DIRTY: {},
            self.REMOVED:{}
            }

        self._obj_states = {}


