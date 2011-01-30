from Rambler.ciHomeBase import ciHomeBase, Entity

from Rambler import Server
from time import time

class Widget(Entity):

    interface = "Widget"

    def __init__(self, pk):
        self._set_primaryKey(pk)
        self._wedgied = 0
        self._name = ""

    def _get_primaryKey( self ):
        return self._primaryKey

    def _set_primaryKey(self, pk):
        self._primaryKey = pk


    def _get_home():
        return Server.getHome("widgetHome")

    _get_home = staticmethod(_get_home)

    def _get_name(self):
        return self._name

    def _set_name(self, name):
        self._name = name

    def _get_wedgie( self ):
        return self._wedgied

    def _set_wedgie( self, wedgied ):
        self._wedgied = wedgied

    def _set_other1(self, other):
        ciHomeBase.RelationService.relate(other, self, "relation")

    def _get_other1(self):
        enum = ciHomeBase.RelationService.findPointingTo(self, "relation")
        if enum:
            return enum[0]
        else:
            return None

    def _set_other2(self, other):
        ciHomeBase.RelationService.relate(self, other, "relation")

    def _get_other2(self):
        enum = ciHomeBase.RelationService.findPointingFrom(self, "relation")
        if enum:
            return enum[0]
        else:
            return None

    def _set_parent(self, parent):
        ciHomeBase.RelationService.relate(parent, self, "many_test")

    def _get_parent(self):
        enum = ciHomeBase.RelationService.findPointingTo(self, "many_test")
        if enum:
            return enum[0]
        else:
            return None

    def _get_children(self):
        enum = ciHomeBase.RelationService.findPointingFrom(self, "many_test")
        return enum
        
    def _set_doodle(self, other):
        ciHomeBase.RelationService.relate(self, other, "widget_doodle")

    def _get_doodle(self):
        enum = ciHomeBase.RelationService.findPointingFrom(self, "widget_doodle")
        if enum:
            return enum[0]
        else:
            return None


class WidgetHome(ciHomeBase):
    # If this object needs to be registered, the following 2 are also needed.

    homeId = "widgetHome"
    interface = "WidgetHome"
    entityClass = Widget

    def create( self ):
        pk = '%.5f' % time()
        widget = Widget(pk)

        self.PersistenceService.create(widget)

        # Notify our observers that a new entity has been created
        #self._notifyCreate(widget)

        return widget
