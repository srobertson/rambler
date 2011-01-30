import unittest

from Rambler import Server
Server.init("giop:tcp::6666")
# Forget about the first transaction
Server.txn.commit(0)
PS = Server.getService("PersistenceService")

from CosTransactions import StatusActive, StatusMarkedRollback

from PSQLPool import PSQLPool

import logging
import logging.config
logging.config.fileConfig("log.cfg")
log = logging.getLogger("DBPool")

import thread
import threading
from time import sleep

def txnStart():

    # Starts the txn and tickles the PersistenceService so the right
    # events are published
    Server.txn.begin()
    
    # Gurantees that the commit/rollback events will be published
    PS._getUnitOfWork()

class Test(unittest.TestCase):
    def setUp(self):
        self.pool = PSQLPool("host=dev2",3)
        txnStart()


    def tearDown(self):

        if Server.txn.get_status() in (StatusActive, StatusMarkedRollback):
            Server.txn.rollback()

        # Verify that all the connections are back at this point.
        qsize = self.pool._pool.qsize()
        assert  qsize == 3, "Qsize is %s" % qsize
        
    def testGetConnection(self):
        conn = self.pool.getConnection()
        assert conn

        # Verify that we always get the same connection for this
        # thread
        
        conn2 = self.pool.getConnection()
        assert conn is conn2

    def testMultiThreadGetConnection(self):
        """Verify thread blocks if there's not enough connections."""

        try:
            con1 = Consumer(self.pool)
            con2 = Consumer(self.pool)
            con3 = Consumer(self.pool)

            con1.start()
            con2.start()
            con3.start()
            
            assert con1.hasConn == False


            # We sholud be able to grab multiple connectinos in the
            # main thread and one should still be available for our consumer
            
            for x in range(10):
                self.pool.getConnection()

            con1.doWait("consume")
            assert con1.hasConn == True


            # Now consume the last connection
            con2.doWait("consume")
            assert con2.hasConn == True

            # We should be out of connections at this point so the
            # next consumer should block

            con3.doNoWait("consume")
            assert con3.hasConn == False

            # Put the connection back
            con2.doWait("rollback")
            assert con2.hasConn == False
            # Con 3 should have the connection now
            # Sleep long enough for thread 3 to finish
            sleep(.1)
            assert con3.hasConn == True
            
        finally:
            con1.stop()
            con2.stop()
            con3.stop()






import Queue
class Consumer(threading.Thread):
    def __init__(self, pool):
        threading.Thread.__init__(self)
        
        self._pool = pool
        self.action = Queue.Queue()
        self.hasConn = False

        
    def run(self):
        txnStart()
        while 1:
            
            action, actionLock = self.action.get()
            #import pdb; pdb.set_trace()

            if action is not None:
                name = action.__name__
                log.debug("Found new action %s" % name) 
                try:
                    # Restart the transaction
                    action()
                    
                finally:
                    if actionLock is not None:
                        # Signal to the other thead that the action is complete
                        log.debug("Releasing lock "+ name) 
                        actionLock.set()
            else:
                # Stop running
                Server.txn.rollback()
                if actionLock is not None:
                    actionLock.set()
                break


    def consume(self):
        log.debug("Getting a connection.")
        self._pool.getConnection()
        log.debug("Got a connection.")
        self.hasConn = True

    def rollback(self):
        Server.txn.rollback()
        self.hasConn = False
        txnStart()

    def doNoWait(self, name):
        self.action.put((getattr(self, name), None))

    def doWait(self, name):
        actionLock = threading.Event()
        self.action.put((getattr(self, name), actionLock))
        # Wait for the consumer to proccess this event
        actionLock.wait()


    def stop(self):
        stopLock = threading.Event()
        self.action.put((None, stopLock))
        stopLock.wait()

        
        

    

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()

