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


# Sigh, all these import's have to be done in the proper order
from Rambler import ZODBPool, ZODBMapper
from ZODB import FileStorage, Persistent, DB
from tempfile import mktemp

tempdir = '/tmp' # Just for now

filename = mktemp()
storage = FileStorage.FileStorage(filename)
db = DB(storage, cache_size=1000)
Server.registerService(ZODBPool.ZODBConnectionPool(db, 6), "ZODBPool")

from Widget import Widget, WidgetHome
Server.loadConfig(Widget, 'Widget.cfg')
Server.registerEntity(WidgetHome, Widget)

# Commit the transaction server starts at init
Server.txn.commit(0)


from threading import Thread
from time import time, sleep
from thread import get_ident


class Test(unittest.TestCase):
    def setUp(self):
        self.ps = Server.getService("PersistenceService")
        self.ms = Server.getService("MappingService")
        self.mapper = ZODBMapper.ZODBEntityMapper()
        self.ms.registerMapper(Widget, self.mapper )
        Server.txn.begin()


    def tearDown(self):
        Server.txn.rollback()
        
    def testCreate(self):
        # Just test to make sure we don't get any errors.
        w = Server.getHome('widgetHome').create()
        Server.txn.commit(0)
        Server.txn.begin()

    def testLoad(self):
        w = Server.getHome('widgetHome').create()
        Server.txn.commit(0)
        Server.txn.begin()
        w1 = self.ps.load(Widget, w._get_primaryKey())
        assert w1._get_primaryKey() == w._get_primaryKey()

    def testUpdate(self):
        w = Server.getHome('widgetHome').create()
        Server.txn.commit(0)
        Server.txn.begin()
        w1 = self.ps.load(Widget, w._get_primaryKey())
        assert w1._get_wedgie() == 0
        w1._set_wedgie(1)
        Server.txn.commit(0)
        Server.txn.begin()
        w2 = self.ps.load(Widget, w._get_primaryKey())
        assert w2._get_wedgie() == 1

    def testRemove(self):
        w = Server.getHome('widgetHome').create()
        Server.txn.commit(0)
        Server.txn.begin()
        w1 = self.ps.load(Widget, w._get_primaryKey())
        self.ps.remove(w)
        Server.txn.commit(0)
        Server.txn.begin()
        self.assertRaises(KeyError, self.ps.load, Widget, w._get_primaryKey())
        
        



        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))

    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
