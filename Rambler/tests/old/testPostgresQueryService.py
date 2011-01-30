import unittest
import logging
logging.basicConfig()
log = logging.getLogger("EventChannel")
log.setLevel(logging.DEBUG)

from omniORB import importIDL
importIDL('Widget.idl', ['-I../idl'])

from Rambler import Server
Server.init("giop:tcp::6666")
#Server.loadDescriptor('widget.xml')

#from Rambler.tests.Widget import Widget
from Rambler.PSQLPool import PSQLPool
Server.registerService(PSQLPool("",10), "PSQLPool")

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


PostgresRelationMapper.init()
PostgresEntityMapper.init()
for entityName in ED.getEntityNames():
    klass = ED.getClassForEntity(entityName)
    MS.registerMapper(klass, PostgresEntityMapper)
    

from Rambler.PostgresQueryService import PostgresQueryService
PostgresQueryService = PostgresQueryService()

tid = Server.txn.get_transaction_name()
Server.getService("EventService").publishEvent("Initializing", Server, tid)

Server.txn.commit(0)

from Rambler.ciRelationService import SingleRelationRecord

PS = Server.getService("PersistenceService")
def txnStart():

    # Starts the txn and tickles the PersistenceService so the right
    # events are published
    Server.txn.set_timeout(0)
    Server.txn.begin()
    
    # Gurantees that the commit/rollback events will be published
    PS._getUnitOfWork()
    
class Test(unittest.TestCase):

    def setUp(self):
        self.eds = Server.getService("EntityDescriptionService")
        self.rr = Server.getService("RelationRegistryService")
        self.qs = PostgresQueryService
        txnStart()
        
    def tearDown(self):
        Server.txn.rollback()

    def testGetTypecodeForEntity(self):
        fields = self.eds.getFields('widget')
        fields = self._getFields(fields)
        
        typeCode = self.qs.getTypeCodeForEntity('widget')

        for i in range(len(fields)):
            field = fields[i]
            assert field.name == typeCode.member_name(i)
            # Should also assert the type here but we need a corba lookup thingy


    def testGetEntityInfo(self):
        fields = self.eds.getFields('widget')
        fields = self._getFields(fields)
        for i in range(len(fields)):
            field = fields[i]
            if field.isRelation():
                entityName = self.eds.getName(field.type)
                entityInfo = self.qs.getEntityInfo('widget', 'pk', i)
                assert entityInfo is not None
                assert entityInfo.entityName.lower() == entityName
                assert entityInfo.primaryKey == 'pk'

    def testGetSQLStatement(self):
        query = self.qs.getSQLStatement('widget')
        # This is a stupid way to test
        assert query == 'SELECT widget.primaryKey,widget.wedgie,widget.name,t1.role2,t2.role1,t3.role1,t4.role2 FROM widget  LEFT JOIN relation_relation AS t1 ON widget.primaryKey = t1.role1 LEFT JOIN relation_relation AS t2 ON widget.primaryKey = t2.role2 LEFT JOIN relation_many_test AS t3 ON widget.primaryKey = t3.role2 LEFT JOIN relation_widget_doodle AS t4 ON widget.primaryKey = t4.role1 WHERE widget.primaryKey in (%s)', "Invalid query:\n %s " % query


    def _getFields(self, fields):
        filtered = []
        # Filter out the many relations
        for field in fields:
            if field.isRelation():
                role = self.rr.getRelationRole(field.relationName, field.role)
                if role.multiplicity == SingleRelationRecord:
                    filtered.append(field)
            else:
                filtered.append(field)
        return filtered

    def testGetMetadata(self):
        w1 = Server.getHome('widgetHome').create()
        w2 = Server.getHome('widgetHome').create()

        w1._set_other1(w2)
        w1._set_name('foobar')
        w1._set_wedgie(99)

        w2._set_name('barfoo')
        w2._set_wedgie(69)

        pk1 = w1._get_primaryKey()
        pk2 = w2._get_primaryKey()

        Server.txn.commit(1)
        txnStart()

        results = self.qs.query({'type':'widget',
                                 'primaryKey':pk1,
                                 'query_select':
                                 ['wedgie', 'name', 'parent', 'doodle']})

        results = results.fetchMany('query', 100)
        assert len(results) == 1

        result = results[0].value()

        fields = self._getFields(self.eds.getFields('widget'))

        assert len(result.keys()) == len(fields)

        assert result['name'] == 'foobar'
        assert result['parent'] is None
        assert result['doodle'] is None
        #assert result['other1'].primaryKey == pk2
        #assert result['other2'] is None
        assert result['wedgie'] == 99
        assert result['primaryKey'] == pk1

        # Test getting 2 objects back
        results = self.qs.query_metadata('type=widget&primaryKey=%s&primaryKey=%s' % (pk1, pk2))
        results = results.fetchMany('query', 100)
        assert len(results) == 2

        result = results[0].value()

        fields = self._getFields(self.eds.getFields('widget'))

        assert len(result.keys()) == len(fields)

        assert result['name'] == 'foobar'
        assert result['parent'] is None
        assert result['doodle'] is None
        assert result['other1'].primaryKey == pk2
        assert result['other2'] is None
        assert result['wedgie'] == 99
        assert result['primaryKey'] == pk1

        result = results[1].value()

        fields = self._getFields(self.eds.getFields('widget'))

        assert len(result.keys()) == len(fields)

        assert result['name'] == 'barfoo'
        assert result['parent'] is None
        assert result['doodle'] is None
        assert result['other1'] is None
        assert result['other2'].primaryKey == pk1
        assert result['wedgie'] == 69
        assert result['primaryKey'] == pk2
        
            
            
        

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))

    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
