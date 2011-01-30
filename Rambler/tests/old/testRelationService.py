import unittest, os

import logging
logging.basicConfig()
#logging.getLogger("EventChannel").setLevel(logging.DEBUG)
#logging.getLogger("Rambler").setLevel(logging.DEBUG)
#logging.getLogger("ZODBPool").setLevel(logging.DEBUG)

# Sucks that we need to do this.
from omniORB import importIDL
importIDL('Widget.idl', ['-I../idl'])

from Rambler import Server
Server.init("giop:tcp::6666")

from Rambler.PersistenceService import InMemoryDataMapper
# Sigh, all these import's have to be done in the proper order
##from Rambler import ZODBPool, ZODBMapper
##from ZODB import FileStorage, Persistent, DB
##from tempfile import mktemp

##tempdir = '/tmp' # Just for now

##filename = mktemp()
##storage = FileStorage.FileStorage(filename)
##db = DB(storage, cache_size=1000)
##Server.registerService(ZODBPool.ZODBConnectionPool(db, 6), "ZODBPool")

from Rambler.ciRelationService import ciRelationService
RS = Server.registerService(ciRelationService, "RelationService")
Server.loadDescriptor('widget.xml')
#RS.registerRelation("relation", Promiscuous())
ES = Server.getService("EventService")
ES.publishEvent("Initializing", Server, "") # Needed to init the ZODB properly

from Rambler.tests.Widget import Widget, WidgetHome
#Server.loadConfig(Widget, 'Widget.cfg')
#Server.registerEntity(WidgetHome, Widget)


# Commit the transaction server starts at init
Server.txn.commit(0)



from time import time, sleep


class Test(unittest.TestCase):
    def setUp(self):
        self.ps = Server.getService("PersistenceService")
        self.ms = Server.getService("MappingService")
        self.ms.registerMapper(Widget, InMemoryDataMapper() )

        self.rs = Server.getService("RelationService")
        Server.txn.begin()


    def tearDown(self):
        Server.txn.rollback()
        

    def testRelate(self):

        """Create two objects relate them, then see if the relation
        service returns the right values."""


        w1 = Server.getHome('widgetHome').create()
        w2 = Server.getHome('widgetHome').create()

        self.rs.relate(w1, w2, "relation")

        assert self.rs.isPointingTo(w1, w2, "relation")
                
        Server.txn.commit(0)
        Server.txn.begin()

        assert self.rs.isPointingTo(w1, w2, "relation")

        # Check primary keys, cause technically we shouldn't be
        # holding onto w1 and w2 after the commit.

        assert self.rs.findPointingFrom(w1, "relation")[0]._get_primaryKey() == w2._get_primaryKey()
        assert self.rs.findPointingTo(w2, "relation")[0]._get_primaryKey() == w1._get_primaryKey()

    def testRemoveEntity(self):

        """Verifies that deleting an entity removes all the relations
        the entity was involved in."""

        w1 = Server.getHome('widgetHome').create()
        w2 = Server.getHome('widgetHome').create()
        
        self.rs.relate(w1, w2, "relation")
        assert self.rs.isPointingTo(w1, w2, "relation")
        
        Server.txn.commit(0)
        Server.txn.begin()

        w2 = Server.getHome('widgetHome').findByPrimaryKey(w2._get_primaryKey())
        assert self.rs.isPointingTo(w1, w2, "relation")
        
        Server.getHome('widgetHome').remove(w2)

        Server.txn.commit(0)
        Server.txn.begin()

        
        assert not self.rs.isPointingTo(w1, w2, "relation")

    def testRelateTwo(self):

        """Create three objects, relate them, then see if the relation
        service returns the right values."""
        
        w1 = Server.getHome('widgetHome').create()
        w2 = Server.getHome('widgetHome').create()
        w3 = Server.getHome('widgetHome').create()

        self.rs.relate(w1, w2, "many_test")
        self.rs.relate(w1, w3, "many_test")

        relatives = self.rs.findPointingFrom(w1, "many_test")
        assert len(relatives) == 2, "Should have 2 relatives, found %s" % len(relatives)
        
        Server.txn.commit(0)
        Server.txn.begin()


        relatives = self.rs.findPointingFrom(w1, "many_test")
        num = len(relatives)
        assert num == 2, "Number of relatives is %s" % num
         
    def testRelateThree(self):

        """Create three objects, relate them, then see if the relation
        service returns the right values."""
        
        w1 = Server.getHome('widgetHome').create()
        w2 = Server.getHome('widgetHome').create()
        w3 = Server.getHome('widgetHome').create()
        w4 = Server.getHome('widgetHome').create()

        self.rs.relate(w1, w2, "many_test")
        self.rs.relate(w1, w3, "many_test")
        self.rs.relate(w1, w4, "many_test")

        relatives = self.rs.findPointingFrom(w1, "many_test")
        assert len(relatives) == 3, "Should have 3 relatives, found %s" % len(relatives)
        
        Server.txn.commit(0)
        Server.txn.begin()

        relatives = self.rs.findPointingFrom(w1, "many_test")
        assert len(relatives) == 3, "Should have 3 relatives, found %s" % len(relatives)

    def testRelationConflict(self):
        """Test 2 users trying to modify the same relation"""
        wHome = Server.getHome('widgetHome')

        w1a = wHome.create()
        w1b = wHome.create()
        w1c = wHome.create()
        w2 = wHome.create()

        # Keys for loading it later
        w1aKey = w1a._get_primaryKey()
        w1bKey = w1b._get_primaryKey()
        w1cKey = w1c._get_primaryKey()
        w2Key = w2._get_primaryKey()

        # w2 is assocated with w1a.  2 transactions each try to
        # move ti, one to w1b and one to w1c.  At the end, the last
        # committed transaction should win with no invalid references
        # laying around.
        self.rs.relate(w1a, w2, 'many_test')
        Server.txn.commit(0)

        # Load the objects in each transaction
        Server.txn.begin()
        w1b = wHome.findByPrimaryKey(w1bKey)
        w2a = wHome.findByPrimaryKey(w2Key)
        context1 = Server.txn.suspend()

        Server.txn.begin()
        w1c = wHome.findByPrimaryKey(w1cKey)
        w2b = wHome.findByPrimaryKey(w2Key)
        context2 = Server.txn.suspend()

        Server.txn.resume(context1)
        self.rs.relate(w1b, w2a, 'many_test')
        context1 = Server.txn.suspend()

        Server.txn.resume(context2)
        self.rs.relate(w1c, w2b, 'many_test')
        context2 = Server.txn.suspend()

        Server.txn.resume(context1)
        Server.txn.commit(0)

        Server.txn.resume(context2)
        Server.txn.commit(0)

        Server.txn.begin()
        # Reload all the objects for this transaction
        w1a = wHome.findByPrimaryKey(w1aKey)
        w1b = wHome.findByPrimaryKey(w1bKey)
        w1c = wHome.findByPrimaryKey(w1cKey)
        w2 = wHome.findByPrimaryKey(w2Key)
        
        # Validate the final object
        relatives = self.rs.findPointingFrom(w1c, 'many_test')
        assert len(relatives) == 1, "Should have 1 relative, found %s" % len(relatives)
        assert relatives[0]._get_primaryKey() == w2._get_primaryKey()

        # Validate the object we moved
        relatives = self.rs.findPointingTo(w2, 'many_test')
        assert len(relatives) == 1, "Should have 1 relative, found %s" % len(relatives)
        assert relatives[0]._get_primaryKey() == w1c._get_primaryKey()

        # Assure both the other objects have no relations
        relatives = self.rs.findPointingFrom(w1a, 'many_test')
        assert len(relatives) == 0, "Should have 0 relatives, found %s" % len(relatives)
        relatives = self.rs.findPointingFrom(w1b, 'many_test')
        assert len(relatives) == 0, "Should have 0 relatives, found %s" % len(relatives)
        
    def testTwoRelatesOneTransaction(self):
        """testTwoRelatesOneTransaction
        Assures that if you relate an object to two different objects over
        the course of a transaction that everything behaves properly as we
        have/had a problem with things freaking out.  The relations should
        all exist before the transaction starts.
        """
        wHome = Server.getHome('widgetHome')

        user1 = wHome.create()
        user2 = wHome.create()
        lead1 = wHome.create()
        lead2 = wHome.create()

        u1pk = user1._get_primaryKey()
        u2pk = user2._get_primaryKey()

        rs = self.rs

        rs.relate(user1, lead1, 'many_test')
        rs.relate(user2, lead2, 'many_test')

        Server.txn.commit(0)
        Server.txn.begin()
        
        lead3 = wHome.create()
        user1 = wHome.findByPrimaryKey(u1pk)
        user2 = wHome.findByPrimaryKey(u2pk)

        rs.relate(user1, lead3, 'many_test')
        #rs.relate(user2, lead3, 'many_test')

        try:
            Server.txn.commit(0) # This is usually the problem line
            Server.txn.begin()
        except:
            Server.txn.suspend()
            Server.txn.begin()
            assert 0, "Caught error during commit."

        assert 1


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))

    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
