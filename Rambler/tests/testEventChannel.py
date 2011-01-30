from unittest import TestCase, TestSuite, main, makeSuite

# Sucks that we need to do this.
from omniORB import importIDL
importIDL('../idl/epo.idl', ['-I../idl'])

from Rambler.EventChannel import EventChannel
from Interface import Interface
from Interface.Implements import implements

class Publisher:
    pass

class Subscriber:
    message = ""
    def handleMessage(self, msg):
        self.message = msg

class ITestMessage(Interface):
    pass

class TestMessage:
    pass
implements(TestMessage,ITestMessage)

class ExtendString(str):
    pass

class Test(TestCase):

    def setUp(self):
        ec=self.eventChannel = EventChannel()
    
        publisher=self.publisher=Publisher()
        ec.registerEvent("test", publisher, str)

        subscriber=self.subscriber=Subscriber()
        ec.subscribeToEvent("test",subscriber,str)

    def testPublish(self):
       
        ec = self.eventChannel
        ec.publishEvent("test",self.publisher,"Blah")

        assert self.subscriber.message == "Blah"

    def testPublishInvalidMessage(self):
        """Tests to make sure that a publisher can't publish an
        invalid message to an event."""

        ec = self.eventChannel

        try:
            ec.publishEvent("test",self.publisher,TestMessage())
        except AssertionError:
            pass
        else:
            self.fail("Expected an exception.")

        

    def testSubscriberExpectsDifferentMsgInterface(self):
        """Tests to make sure that an exception is thrown when an
        object subscribes to an event but expects a differet interface
        for a message."""

        ec=self.eventChannel
        newsub = Subscriber()
        try:
            ec.subscribeToEvent("test",newsub,ITestMessage)
        except AssertionError:
            pass
        else:
            self.fail("Expected an Assertion error.")

    def testSubscriberExpectsSuperInterface(self):
        """Tests to ensure that a subscriber can subscribe to an event
        that publishes a message whose interface inherits from the
        interface that the subscriber is expecting."""

        ec = self.eventChannel
        pub = Publisher()
        
        ec.registerEvent("subclass", pub, ExtendString)
        newsub = Subscriber()
        ec.subscribeToEvent("subclass",newsub,str)
        
        ec.publishEvent("subclass",pub,ExtendString("Did this work?"))
        assert newsub.message == "Did this work?"


    def testMultiplePublishers(self):
        ec = self.eventChannel
        subscriber = self.subscriber
        pub1 = self.publisher
        
        pub2 = Publisher()
        ec.registerEvent("test", pub2, str)
        
        ec.publishEvent("test",pub1,"Pub 1")
        assert subscriber.message == "Pub 1"
                
        ec.publishEvent("test",pub2,"Pub 2")
        assert subscriber.message == "Pub 2"

    def testMultiPublisherWithInvalidMessages(self):
        """Tests to make sure that an exception is thrown when a
        second publisher registers the same event with a different
        message interface."""
        ec = self.eventChannel
        pub = Publisher()

        try:
            ec.registerEvent("test", pub, ITestMessage)
        except AssertionError:
            pass
        else:
            self.fail("Expected an assertion error.")

        # Test to make sure that it's ok to register a message
        # interface that extends a registered interface.
        ec.registerEvent("test", pub, ExtendString)
        
    def testExclusiveRegister(self):
        ec = self.eventChannel

        pub1 = Publisher()
        ec.registerEvent("exclusive", pub1, str,1)

        pub2 = Publisher()
        try:
            ec.registerEvent("exclusive", pub2, ITestMessage)
        except AssertionError:
            pass
        else:
            self.fail("expected an exception")

    def testExclusiveRegisterAfterNonExclusiveRegister(self):
        """Make sure that an exception is thrown if an object
        tries to be the exclusive publisher of an event after other
        objects registered the event in non-exclusive mode."""

        ec = self.eventChannel
        pub = Publisher()
        try:
            ec.registerEvent("test", pub, str, 1)
        except AssertionError:
            pass
        else:
            self.fail("Expected an exception.")

    def testUnregisterEvent(self):
        ec = self.eventChannel
        ec.unregisterEvent("test", self.publisher)
        self.assertRaises(KeyError, ec.publishEvent,"test",self.publisher,"Blah")

    def testUnsubscribeFromEvent(self):
        ec = self.eventChannel
        ec.unsubscribeFromEvent("test", self.subscriber)
        ec.publishEvent("test", self.publisher, "Blah")

        assert self.subscriber.message != "Blah"

    
def test_suite():
    return TestSuite((
        makeSuite(Test),
        ))

if __name__=='__main__':
    main(defaultTest='test_suite')
