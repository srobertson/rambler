from zope.interface import Interface
FALSE=0
class IEventChannel(Interface):
    """Broadcasts events to different objects."""

    def registerEvent(eventName, publisher, msgInterface, exclusive=FALSE):
        
        """Register an object as a source for a paticular
        event. Multiple objects can register to be the source for the
        same event so long as the Interface for the message matches and
        no other object has registered itself as the exclusive
        provider for the event.

        name -- a case sensitive string representing the name of the
        event.

        msgInterface -- An interface class that messages will be
        tested against when an object later publishes to the event.

        exclusive -- If true then no other objects will be allowed to
        register themselves as an event source for this event.
        """

    def unregisterEvent(eventName, publisher):

        """ Removes an object from being a source for a particular event. """

    def publishEvent(eventName,publisher, msg):
        """Publishes the event to all the subscribed methods. If an
        object attempts to publish an event and it hasen't regsitered
        itself as a source for the event an UneregisteredPublisherError will
        be raised.

        If the msg does not mathch the interface of the msgInterface
        then a MessageInterfaceMismatchError will be raised.

        If an even 
        """

    def subscribeToEvent(eventName,subscriber,msgInterface):
        """Registers an object to recieve events of a a given name."""

    def unsubscribeFromEvent(eventName,subscriber):
        """Unregister an object from receiving events. """

class ISubscriber(Interface):
    def handleMessage(msg):
        """Called by the event channel when an event is published."""
