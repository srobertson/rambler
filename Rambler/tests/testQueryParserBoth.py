import unittest
from initRambler import txnStart, initialize

initialize("widget_doodle_relation_test.xml", "deployment_both.xml", "dbname=relation_test_both")
from Rambler import Server

from Rambler.tests.Widget import Widget
from Rambler.tests.Doodle import Doodle
from Rambler.QueryParser import QueryError, query2pgsql
from Rambler.PostgresMappers import OQLQuery as OrigOQLQuery
from cgi import parse_qs

class OQLQuery(OrigOQLQuery):
    # Fake OQLQuery object because we had to change the OQLQuery object to take
    # dicts instead of strings and I'm too lazy to change all my tests.
    def __init__(self, query):
        qdict = parse_qs(query)
        for key, value in qdict.items():
            if len(value) == 1:
                qdict[key] = value[0]
        self.query = OrigOQLQuery(qdict)

    def execute(self):
        return self.query.execute()

class Test(unittest.TestCase):

    def setUp(self):
        txnStart()
        self._pool = Server.getService("PSQLPool")
        self._conn = self._pool.getConnection()
        self._cursor = self._conn.cursor()

        self.wHome = Server.getHome("widgetHome")
        self.dHome = Server.getHome("doodleHome")

        self.d1 = self.dHome.create()
        self.w1 = self.wHome.create()

        self.w1._set_doodle(self.d1)

    def tearDown(self):
        self.wHome.remove(self.w1)
        self.dHome.remove(self.d1)
        self._pool.commit(Server.txn.get_transaction_name())

    def testWidgetDoodle(self):
        query = "type=widget&query_select=name&query_select=primaryKey&query_select=doodle.name&query_select=doodle.primaryKey"
        sql = OQLQuery(query).query._sql
        print sql

    def testDoodleWidget(self):
        query = "type=doodle&query_select=name&query_select=primaryKey&query_select=widget.name&query_select=widget.primaryKey"
        sql = OQLQuery(query).query._sql
        print sql

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))

    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
