import unittest
import logging
logging.basicConfig()
#log = logging.getLogger("EventChannel")
#log.setLevel(logging.DEBUG)

from omniORB import importIDL
importIDL('Widget.idl', ['-I../idl'])

from Rambler import Server
Server.init("giop:tcp::6666")
#Server.loadDescriptor('widget.xml')

#from Rambler.tests.Widget import Widget
from Rambler.PSQLPool import PSQLPool
Server.registerService(PSQLPool("host=dev2",10), "PSQLPool")

from Rambler.HiLowKeyGenService import HiLowKeyGenService, IncrementalKeyGenService, Key
Server.registerService(HiLowKeyGenService, "KeyGenService")
Server.registerService(IncrementalKeyGenService, "IKeyGenService")


from Rambler.SQLDescriptionService import PostgresSQLDescriptionService
sqld=Server.registerService(PostgresSQLDescriptionService, "SQLDescriptionService")


from Rambler.PostgresMappers import PostgresEntityMapper, PostgresRelationMapper, PostgresKeyMapper

ED = Server.getService("EntityDescriptionService")
MS = Server.getService("MappingService")

from Rambler.ciRelationService import SingleRelationRecord, ManyRelationRecord, RelationDiary
MS.registerMapper(SingleRelationRecord, PostgresRelationMapper)
MS.registerMapper(ManyRelationRecord, PostgresRelationMapper)
MS.registerMapper(RelationDiary, PostgresRelationMapper)
MS.registerMapper(Key, PostgresKeyMapper)

#Server.registerService(ciRelationService, "RelationService")
Server.loadDescriptor("widget.xml")
sqld.load("deployment.xml")


PostgresEntityMapper.init()
PostgresRelationMapper.init()
for entityName in ED.getEntityNames():
    klass = ED.getClassForEntity(entityName)
    MS.registerMapper(klass, PostgresEntityMapper)
    

tid = Server.txn.get_transaction_name()
Server.getService("EventService").publishEvent("Initializing", Server, tid)
Server.txn.commit(0)

from epo import FinderException

PS = Server.getService("PersistenceService")
def txnStart():

    # Starts the txn and tickles the PersistenceService so the right
    # events are published
    Server.txn.begin()
    
    # Gurantees that the commit/rollback events will be published
    PS._getUnitOfWork()


class TestEntityMapper(unittest.TestCase):
    def setUp(self):
        txnStart()

    def tearDown(self):
        Server.txn.rollback()


    def testCreateEntity(self):
        wHome = Server.getHome("widgetHome")
        widget = wHome.create()
        pKey = widget._get_primaryKey()
        Server.txn.commit(0)
        
        txnStart()

        pool = Server.getService("PSQLPool")
        conn = pool.getConnection()
        cursor = conn.cursor()
        cursor.execute("SELECT primaryKey, wedgie FROM Widget WHERE primaryKey='%s'" % pKey)

        assert cursor.rowcount == 1, "Should have found 1 record matching widget.  Found: %s" % cursor.rowcount

        data = cursor.fetchone()
        assert data[0] == pKey
        assert data[1] == 0

    def testLoadEntity(self):
        wHome = Server.getHome("widgetHome")
        widget = wHome.create()
        widget._set_wedgie(1)
        pKey = widget._get_primaryKey()
        Server.txn.commit(0)
        
        txnStart()
        widget = wHome.findByPrimaryKey(pKey)

        assert widget._get_primaryKey() == pKey
        assert widget._get_wedgie() == 1


    def testUpdateEntity(self):
        wHome = Server.getHome("widgetHome")
        widget = wHome.create()
        pKey = widget._get_primaryKey()
        Server.txn.commit(0)

        txnStart()
        widget = wHome.findByPrimaryKey(pKey)

        assert widget._get_primaryKey() == pKey
        assert widget._get_wedgie() == 0

        widget._set_wedgie(1)

        Server.txn.commit(0)

        txnStart()
        widget = wHome.findByPrimaryKey(pKey)

        assert widget._get_primaryKey() == pKey
        assert widget._get_wedgie() == 1
        

    def testRemoveEntity(self):

        wHome = Server.getHome("widgetHome")
        widget = wHome.create()
        pKey = widget._get_primaryKey()
        Server.txn.commit(0)

        txnStart()
        widget = wHome.findByPrimaryKey(pKey)
        wHome.remove(widget)
        
        Server.txn.commit(0)

        txnStart()
        try:
            wHome.findByPrimaryKey(pKey)
            raise AssertionError("Widget not deleted.")
        except FinderException:
            pass
        


class TestRelationMapper(unittest.TestCase):
    def setUp(self):
        txnStart()

    def tearDown(self):
        Server.txn.rollback()

    def testOneToOneRelation(self):
        wHome = Server.getHome("widgetHome")
        widget1 = wHome.create()
        widget2 = wHome.create()

        widgetKey1 = widget1.primaryKey
        widgetKey2 = widget2.primaryKey

        widget1._set_other1(widget2)

        Server.txn.commit(0)
        txnStart()

        widget1 = wHome.findByPrimaryKey(widgetKey1)
        widget2 = wHome.findByPrimaryKey(widgetKey2)

        assert widget1._get_other1().primaryKey == widgetKey2
        assert widget2._get_other2().primaryKey == widgetKey1        

        
    def testOneToManyRelation(self):

        wHome = Server.getHome("widgetHome")
        parent = wHome.create()
        child1 = wHome.create()
        child2 = wHome.create()


        
        parentKey = parent._get_primaryKey()
        childKey1 = child1._get_primaryKey()
        childKey2 = child2._get_primaryKey()

        print parentKey, childKey1, childKey2

        parent._set_wedgie(1)

        child1._set_parent(parent)
        child2._set_parent(parent)
        
        Server.txn.commit(0)
        txnStart()

        parent = wHome.findByPrimaryKey(parentKey)

        children = parent._get_children()
        assert len(children) == 2, "Should have 2 children.  Has %s" % len(children)

        for child in children:
            assert child._get_primaryKey() in [childKey1, childKey2]

        Server.txn.commit(0)
        txnStart()

        removed = wHome.findByPrimaryKey(childKey2)
        wHome.remove(removed)

        Server.txn.commit(0)
        txnStart()

        parent = wHome.findByPrimaryKey(parentKey)

        children = parent._get_children()
        assert len(children) == 1, "Should have 1 child.  Has %s" % len(children)

        assert children[0]._get_primaryKey() == childKey1

class TestKeyMapper(unittest.TestCase):
    def setUp(self):
        txnStart()

    def tearDown(self):
        Server.txn.rollback()

    def testKey(self):
        kgs = Server.getService("KeyGenService")
        kg = kgs.getKeyGenerator('')
        key = kg.nextKey()
        key2 = kg.nextKey()

        assert (int(key.split('-')[1]) +1) == (int(key2.split('-')[1]))

        Server.txn.commit(0)
        txnStart()
        
        key = kg.currentKey()
        assert key == key2

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestEntityMapper))
    suite.addTest(unittest.makeSuite(TestRelationMapper))

    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
