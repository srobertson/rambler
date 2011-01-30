import unittest, os


# Sucks that we need to do this.
from omniORB import importIDL, CORBA
importIDL('../idl/epo.idl', ['-I../idl'])

from Rambler import Server
Server.init("giop:tcp::6666")
#Server.getService("EventService").registerEvent("commit", None, str)

# Commit the transaction server starts at init
Server.txn.commit(0)


from Rambler import ZODBPool
from Rambler.Events import Vote

from ZODB import FileStorage, Persistent, DB
from tempfile import mktemp
from threading import Thread
from time import sleep
from thread import get_ident


tempdir = '/tmp' # Just for now

class Widget(Persistent):
    meta_type = 'Widget'

    def __init__(self):
        self.wedgied = 0

    def wedgie(self):
        self.wedgied = 1

    def been_wedgied(self):
        return self.wedgied

class ReadConsumer(Thread):

    # I just get a connection and sleep for a second!

    def setPool(self, pool):
        self._pool = pool

    def run(self):
        Server.txn.begin()
        conn = self._pool.getForRead()
        fake_commit()
        Server.txn.commit(0)
        sleep(1)

class WriteConsumer(Thread):

    # I just get a connection and sleep for a second!

    def setPool(self, pool):
        self._pool = pool

    def run(self):

        print get_ident(), " write consumer starting."
        Server.txn.begin()
        conn = self._pool.getForWrite()
        sleep(1)
        print get_ident(), " write consumer commiting."
        fake_commit()
        Server.txn.commit(0)
        print get_ident(), " write consumer finishing."
        
class VoterBase:
    def __init__(self):
        self.ec = Server.getService("EventService")
        self.ec.registerEvent("vote", self, Vote)
        self.ec.subscribeToEvent("prepare", self, str)

    def release(self):
        self.ec.unregisterEvent("vote", self)
        self.ec.unsubscribeFromEvent("prepare", self)
 
class Destroyer(VoterBase):
    def handleMessage(self, msg):
        raise RuntimeError
 
def fake_commit():
    # Fake the commit message, which should free resources
    #Server.getService("EventService").publishEvent("commit", , Server.txn.get_transaction_name())
    Server.getService("PersistenceService")._getUnitOfWork()

class Test(unittest.TestCase):
    def setUp(self):
        # Set up a new ZODB storage file for each test with a default Widget.
        self.filename = mktemp()
        storage = FileStorage.FileStorage(self.filename)
        db = DB(storage, cache_size=1000)
        conn = db.open()
        root = conn.root()
        root['test_obj'] = Widget()
        conn.close()

        self._conn_pool = ZODBPool.ZODBConnectionPool(db, 6)
        Server.txn.begin()

    def tearDown(self):
        for filename in os.listdir(tempdir):
            fullname = os.path.join(tempdir, filename)
            if fullname.startswith(self.filename):
                os.remove(fullname)
        self._conn_pool = None
        Server.txn.rollback()

    def testGetForRead(self):
        conn = self._conn_pool.getForRead()
        obj = conn['test_obj']

        assert isinstance(obj, Widget)

    def testGetForWrite(self):
        conn = self._conn_pool.getForWrite()
        obj = conn['test_obj']
        assert not obj.been_wedgied()
        obj.wedgie()

        fake_commit()
        Server.txn.commit(0)
        Server.txn.begin()

        conn = self._conn_pool.getForRead()
        obj = conn['test_obj']
        assert obj.been_wedgied()

    def testBlockForWrite(self):
        consumer = ReadConsumer()
        consumer.setPool(self._conn_pool)
        consumer.start()

        sleep(.5)

        conn = self._conn_pool.getForWrite()
        # Verify that the consumer died before the write connection was pulled
        assert not consumer.isAlive()

        consumer = ReadConsumer()
        consumer.setPool(self._conn_pool)
        consumer.start()
        sleep(2)

        # Consumer should still be alive be alive because we have the
        # write connection
        assert consumer.isAlive()

        fake_commit()
        Server.txn.commit(0)
        Server.txn.begin()
        sleep(2)

        # Consumer should be dead because we released the write
        # connection.
        
        assert not consumer.isAlive()

    def testSameTXNGetWrite(self):

        """The same transaction should be able to get the write & read
        connections multiple times without blocking."""
        conn1 = self._conn_pool.getForWrite()
        conn2 = self._conn_pool.getForWrite()
        conn3 = self._conn_pool.getForRead()
        
    def testBlockDifferentTXN(self):

        """Verifies that different transactions have to wait until the
        transaction is released."""
        consumer = WriteConsumer()
        consumer.setPool(self._conn_pool)
        consumer.start()

        # We should eventually get here after the thread dies
        sleep(1)
        conn1 = self._conn_pool.getForWrite()

        
        consumer = WriteConsumer()
        consumer.setPool(self._conn_pool)
        consumer.start()
        sleep(2)

        # Consumer should still be waiting for us to finish
        assert consumer.isAlive()

        fake_commit()
        Server.txn.commit(0)
        Server.txn.begin()

        sleep(2)

        # Consumer should be free
        assert not consumer.isAlive()

        # This shouldn't block, because the other write consumer has  died.
        conn1 = self._conn_pool.getForWrite()
        fake_commit()

    def testDeadlock(self):
        d = Destroyer()

        write_conn = self._conn_pool.getForWrite()

        consumer = WriteConsumer()
        consumer.setPool(self._conn_pool)
        consumer.start()

        try:
            fake_commit()
            Server.txn.commit(0)
            Server.txn.begin()
        except CORBA.TRANSACTION_ROLLEDBACK:
            d.release()
            consumer.join()

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
