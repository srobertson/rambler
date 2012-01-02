import Rambler
from Rambler import outlet,nil
from bisect import insort

from zope.interface import implements
from zope.interface.verify import verifyObject
from Rambler.IEventChannel import IEventChannel, ISubscriber
from zope.interface import Interface


class EventService(object):
    """Implements the EventChannel interface. See IEventChannel for
    usage informatin.

    This sohuld work well for small applications, but be warned that this
    verison will keep objects around in memory, which could cause
    memory leaks if your not careful.

    Note on error checks:

    Most of the error checking are done using asserts so that the code
    will run faster when using python's -O switch. One side effect of
    this is that most of the Unit tests for exceptions will fail.
    
    """
    implements(IEventChannel)
    log = outlet("LogService",missing=nil)
    
    #def will_disassemble(self):
    #  self._events.clear()
    #  self._publishers.clear()
    #  self._subscribers.clear()
    #  self._exclusive.clear()
       
    def __init__(self):
        self._events={}     # Dictionary of event name to interface
                            # mapping. 

        self._publishers={} # Publisher to event name mapping

        self._subscribers={}# Event to subscriber mapping

        self._exclusive={}  # Mapping of events with exclusive
                            # publishers. 

    def registerEvent(self, eventName, publisher, msgInterface,
                      exclusive=False):

        # If the event is already registered 
        if self._publishers.has_key(eventName):

            # Is some other object the exclusive publisher?
            assert not self._exclusive.has_key(eventName), \
                   "'%s' has an exclusive publisher."% eventName
            
            # Has the event been registered with a different message
            # interface?
            assert self._events[eventName] == msgInterface or \
                   issubclass(msgInterface, self._events[eventName]), \
                   "'%s' has been registered with a " \
                   "different message interface." % eventName

            # you can't be the exclusive publisher.
            assert not exclusive, "Can't be the exclusive publisher for "\
                   "this event because other publishers have already "\
                   "registered."


        self._events[eventName] = msgInterface
            
        self._addPublisher(eventName,publisher)
        
        if exclusive:
            self._exclusive[eventName] = 1

    def unregisterEvent(self, eventName, publisher):
        if self._publishers.has_key(eventName):
            self._publishers[eventName].remove(publisher)
            if not len(self._publishers[eventName]):
                del self._publishers[eventName]
                del self._events[eventName]
                if self._exclusive.has_key(eventName):
                    del self._exclusive[eventName]
                
                
            
    def _addPublisher(self,eventName, publisher):
        if not self._publishers.has_key(eventName):
            self._publishers[eventName] = []

        self._publishers[eventName].append(publisher)

    def publishEvent(self,eventName,publisher,msg):
        self.log.debug("Publishing %s" % eventName)
        
        publishers = self._publishers.get(eventName)
        assert publishers and publisher in publishers, "Publisher must register "\
               "before attempting to publish."

        regInterface = self._events[eventName]
        msgType = type(msg)
        assert msgType == regInterface or \
               issubclass(msgType, regInterface) or \
               (issubclass(regInterface, Interface) and verifyObject(regInterface,msg)), \
               "Publisher tried to publish a message with an invalid "\
               "interface."
        
        subscribers = self._subscribers.get(eventName, ())
        for priority, subscriber in subscribers:
            if hasattr(subscriber, 'handleMessage'):
              subscriber.handleMessage(msg)
            else:
              subscriber(msg)


    publish = publishEvent

    def subscribeToEvent(self, eventName, subscriber, msgInterface, priority=10):
        self.log.debug("subscribing %s to event %s" % (subscriber, eventName))
        # If event is registered
        if self._events.has_key(eventName):
            regMsgInterface = self._events[eventName]
            assert msgInterface == regMsgInterface or \
                   issubclass(regMsgInterface, msgInterface),\
                  "Tried to subscribe to an event but the message " \
                  "Interfaces does not match."
        else:
            self._events[eventName] = msgInterface

        self._addSubscriber(eventName,subscriber, priority)

    def unsubscribeFromEvent(self, eventName, subscriber):
        if self._subscribers.has_key(eventName):
            subscribers = self._subscribers[eventName]
            for i in range(len(subscribers)):
                victim = subscribers[i][1]
                if subscriber == victim:
                    del subscribers[i]
                    break

            if not len(self._subscribers[eventName]):
                del self._subscribers[eventName]
    
    def _addSubscriber(self,eventName, subscriber, priority):
        if not self._subscribers.has_key(eventName):
            self._subscribers[eventName] = []

        subscribers = self._subscribers[eventName]
        insort(subscribers, (priority, subscriber))

    


        
class Handler:

    """Listens for a paticular event and calls the function it was
    instatiated with when the Event is published."""
    implements(ISubscriber)

    def __init__(self, func):
        self.func = func

    def handleMessage(self, msg):
        self.func(msg)

           
