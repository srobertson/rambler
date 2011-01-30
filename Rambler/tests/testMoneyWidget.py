import unittest, os
from Money import Money, getCurrency

import logging
logging.basicConfig()
#logging.getLogger("EventChannel").setLevel(logging.DEBUG)
#logging.getLogger("Rambler").setLevel(logging.DEBUG)
#logging.getLogger("ZODBPool").setLevel(logging.DEBUG)

# Sucks that we need to do this.
from omniORB import importIDL
importIDL('MoneyWidget.idl', ['-I../idl'])

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

from MoneyWidget import MoneyWidget, MoneyWidgetHome
Server.loadConfig(MoneyWidget, 'MoneyWidget.cfg')
Server.registerEntity(MoneyWidgetHome, MoneyWidget)

# Commit the transaction server starts at init
Server.txn.commit(0)

orb = Server.orb
MoneyWidgetHome = orb.string_to_object("corbaname:rir:#moneyWidgetHome")

from threading import Thread
from time import time, sleep
from thread import get_ident

import epo

class Test(unittest.TestCase):
    def setUp(self):
        self.ps = Server.getService("PersistenceService")
        self.ms = Server.getService("MappingService")
        self.mapper = ZODBMapper.ZODBEntityMapper()
        self.ms.registerMapper(MoneyWidget, self.mapper )
        self.currency = getCurrency('usd')
        self.money = Money(10.00, self.currency)
        Server.txn.begin()


    def tearDown(self):
        Server.txn.rollback()
        
    def testCreate(self):
        # Just test to make sure we don't get any errors.
        w = Server.getHome('moneyWidgetHome').create(self.money)
        Server.txn.commit(0)
        Server.txn.begin()

    def testLoad(self):
        w = Server.getHome('moneyWidgetHome').create(self.money)
        Server.txn.commit(0)
        Server.txn.begin()
        w1 = self.ps.load(MoneyWidget, w._get_primaryKey())
        assert w1._get_primaryKey() == w._get_primaryKey()

    def testGet(self):
        w = Server.getHome('moneyWidgetHome').create(self.money)
        Server.txn.commit(0)
        Server.txn.begin()
        w1 = self.ps.load(MoneyWidget, w._get_primaryKey())
        assert w1._get_money().getAmount() == 10.0

    def testSet(self):
        w = Server.getHome('moneyWidgetHome').create(self.money)
        Server.txn.commit(0)
        Server.txn.begin()
        w1 = self.ps.load(MoneyWidget, w._get_primaryKey())
        assert w1._get_money().getAmount() == 10.0
        money = Money(5.00, self.currency)
        w1._set_money(money)
        Server.txn.commit(0)
        Server.txn.begin()
        w2 = self.ps.load(MoneyWidget, w._get_primaryKey())
        assert w2._get_money().getAmount() == 5.0

    def testCorba(self):
        money = epo.Money(5.0, 'USD')
        w = MoneyWidgetHome.create(money)

        m2 = w._get_money()
        assert m2.amount == 5.0, "Should be 5.0, is %s" % m2.amount
        assert m2.currencyCode == 'USD'


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
