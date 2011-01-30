import unittest, os

# Sucks that we need to do this.
from omniORB import importIDL
from omniORB.CORBA import TRANSACTION_ROLLEDBACK
importIDL('../idl/epo.idl', ['-I../idl'])

import logging
logging.basicConfig()
#logging.getLogger("EventChannel").setLevel(logging.DEBUG)
#logging.getLogger("Rambler").setLevel(logging.DEBUG)
#logging.getLogger("ZODBPool").setLevel(logging.DEBUG)


from Rambler import Server
from Rambler.EventChannel import EventChannel
Server.init("giop:tcp::6666")
# Commit the transaction server starts at init
Server.txn.commit(0)


from Rambler.PersistenceService import PersistenceService, InMemoryDataMapper
from Rambler.Events import Vote
from CosTransactions import VoteCommit, VoteReadOnly, VoteRollback

#from tempfile import mktemp
#from threading import Thread
from time import time
#from thread import get_ident

class VoterBase:
    def __init__(self):
        self.ec = Server.getService("EventService")
        self.ec.registerEvent("vote", self, Vote)
        self.ec.subscribeToEvent("prepare", self, str)

    def release(self):
        self.ec.unregisterEvent("vote", self)
        self.ec.unsubscribeFromEvent("prepare", self)
        
class ReadOnlyVoter(VoterBase):
    def handleMessage(self, msg):
        self.ec.publishEvent("vote", self, VoteReadOnly)

class CommitVoter(VoterBase):
    def handleMessage(self, msg):
        self.ec.publishEvent("vote", self, VoteCommit)

class RollbackVoter(VoterBase):
    def handleMessage(self, msg):
        self.ec.publishEvent("vote", self, VoteRollback)

class Destroyer(VoterBase):
    def handleMessage(self, msg):
        raise RuntimeError
        
class Test(unittest.TestCase):
    def setUp(self):
        self.ec = Server.getService("EventChannel")
        self.ps = Server.getService("PersistenceService")
        Server.txn.begin()
        self.ps._getUnitOfWork()

    def tearDown(self):
        Server.txn.rollback()

    def testErrorRaisesRollback(self):
        d = Destroyer()
        self.assertRaises(TRANSACTION_ROLLEDBACK, Server.txn.commit, 0)
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
