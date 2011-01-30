from Rambler.ciHomeBase import ciHomeBase, Entity
from Rambler import Server
from time import time

class Doodle(Entity):

    interface = "Doodle"

    def __init__(self, pk):
        self._set_primaryKey(pk)
        self._wedgied = 0
        self._name = ""

    def _get_primaryKey( self ):
        return self._primaryKey

    def _set_primaryKey(self, pk):
        self._primaryKey = pk


    def _get_home():
        return Server.getHome("doodleHome")

    _get_home = staticmethod(_get_home)

    def _get_wedgie( self ):
        return self._wedgied

    def _set_wedgie( self, wedgied ):
        self._wedgied = wedgied

    def _get_name( self ):
        return self._name

    def _set_name( self, name ):
        self._name = name

class DoodleHome(ciHomeBase):
    # If this object needs to be registered, the following 2 are also needed.

    homeId = "doodleHome"
    interface = "DoodleHome"
    entityClass = Doodle

    def create( self ):
        pk = '%.5f' % time()
        doodle = Doodle(pk)

        self.PersistenceService.create(doodle)

        # Notify our observers that a new entity has been created
        #self._notifyCreate(doodle)

        return doodle
