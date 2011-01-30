import unittest, os


# Sucks that we need to do this.
from omniORB import importIDL
importIDL('Widget.idl', ['-I../idl'])
#importIDL('../idl/epo.idl', ['-I../idl'])

import logging
logging.basicConfig()
#logging.getLogger("EventChannel").setLevel(logging.DEBUG)
#logging.getLogger("Rambler").setLevel(logging.DEBUG)
#logging.getLogger("ZODBPool").setLevel(logging.DEBUG)


from Rambler import Server
Server.init("giop:tcp::6666")

#from Rambler.PersistenceService import PersistenceService, InMemoryDataMapper
from Rambler.PSQLPool import PSQLPool
Server.registerService(PSQLPool("host=dev2", 20), "PSQLPool")

Server.loadDescriptor('widget.xml')

import Rambler.PostgresMappers
tid = Server.txn.get_transaction_name()
Server.getService("EventService").publishEvent("Initializing", Server, tid)


#from tempfile import mktemp
#from threading import Thread
from time import time
#from thread import get_ident
from Rambler.tests.Widget import Widget

# Commit the transaction server starts at init
Server.txn.commit(0)

class Test(unittest.TestCase):
    def setUp(self):
        self.ps = Server.getService("PersistenceService")
        self.wh = Server.getHome("widgetHome")
        #self.ms = Server.getService("MappingService")
        #self.mapper = InMemoryDataMapper()
        #self.ms.registerMapper(Widget, self.mapper )
        Server.txn.begin()
        

    def tearDown(self):
        Server.txn.rollback()

    def testCreate(self):
        w = Widget()
        self.ps.create(w)
        w1 = self.ps.load(Widget, w._get_primaryKey())
        assert w1.been_wedgied() == w.been_wedgied() and w1._get_primaryKey() == w._get_primaryKey()
        Server.txn.commit(0)
        Server.txn.begin()

    def testRemove(self):
        w = Widget()
        self.ps.create(w)
        w1 = self.ps.load(Widget, w._get_primaryKey())
        self.ps.remove(w1)
        self.assertRaises(KeyError, self.ps.load, Widget, w._get_primaryKey())

    def testUpdate(self):
        w = Widget()
        self.ps.create(w)
        Server.txn.commit(0)
        Server.txn.begin()
        w1 = self.ps.load(Widget, w._get_primaryKey())
        assert not w1.been_wedgied()
        w1._set_wedgie(1)
        self.ps.update(w1)
        Server.txn.commit(0)
        Server.txn.begin()
        w2 = self.ps.load(Widget, w._get_primaryKey())
        assert w2.been_wedgied()
        

    def testRollback(self):
        w = Widget()
        # Put in unwedgied
        self.ps.create(w)
        Server.txn.commit(0)
        Server.txn.begin()

        w1 = self.ps.load(Widget, w._get_primaryKey())
        w1.wedgie()
        assert  w1.been_wedgied() != w.been_wedgied()

        # w1 should still be wedgied in this transaction
        w1 = self.ps.load(Widget, w._get_primaryKey())
        assert  w1.been_wedgied() != w.been_wedgied()

        Server.txn.rollback()
        Server.txn.begin()
        w1 = self.ps.load(Widget, w._get_primaryKey())
        #Changes have been discarded so these two objects should match
        assert  w1.been_wedgied() == w.been_wedgied()

    def testQuery(self):
        w = Widget()
        self.ps.create(w)
        keys = self.ps.query({'type' : 'Widget'})
        assert len(keys) == 1
        Server.txn.commit(0)
        Server.txn.begin()


        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))

    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
