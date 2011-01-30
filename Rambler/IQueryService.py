from IEventChannel import ISubscriber

class IQueryService(ISubscriber):
    """ Handles the querying of objects and is a subscriber for
    the event channel    """

    def handleMessage(msg):
        """ Handle a message relating to the events from the
        EventQueue, for example when the ciContact class is passed
        in it builds the catalog indexes from that
        """
        
    def query_keys(entity, **kw):
        """ Query the catalog for some results
        
        entity - a given entity
        
        kw - a mapping of a dictionary of traditional Zope style catalog
        querys such as {"text":"foo",}
        
        This will return a list of keys
        """

    def query_objs(entity, **kw):
        """ Query the catalog for some results
        
        entity - a given entity
        
        kw - a mapping of a dictionary of traditional Zope style catalog
        querys such as {"text":"foo",}
        
        This will return a list of actual objects
        """
        
    def reset():
        """ Resets all the catalogs by clearing the data
        Mostly used in debugging """

    def OnRemove(entity):
        """ When an entity is removed """
    
    def OnCreate(entity):
        """ When an entity is added """
    
    
    
