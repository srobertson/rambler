import unittest

import ZODB
from Persistence import Persistent
from ZODB.FileStorage import FileStorage, FileStorageError
from Products.ZCatalog.Catalog import Catalog
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex

class Example(Persistent):
    meta_type='Example'
    
    def __init__(self, data):
        self._data= data

    def text(self):
        return self._data

class Application:
    def __init__( self, file='db.fs', verbose=None, timed=None):
        self.file= file
        self.db  = ZODB.DB( FileStorage( file ) )
        self.co  = self.db.open()
        self.root= self.co.root()
        if not self.root.has_key( 'cat' ):
            self.add_catalog()
        self.cat = self.root['cat']
        #urk
        get_transaction().commit()

    def add_catalog(self):
        self.cat = Catalog()
        self.cat.aq_parent= self.root
        # index
        self.cat.addIndex('text',FieldIndex('text'))
        # metadata
        self.cat.addColumn('text')
        self.root['cat']=self.cat

    def query_index(self, value):
        cat = self.root['cat']
        res = cat.searchResults(REQUEST=None, text=value)
        return res

    def index_file(self, file):
        data = open(file, 'r').read().split(' ')
        
        i = 0 
        for d in data:
            i += 1
            e = Example(d)
            self.cat.catalogObject(e,i)
        
        get_transaction().commit()

    def close(self):
        self.db.close()

class baseTestCatalog(unittest.TestCase):
    """ This is a very crude check that the catalog actually works
    not much else """
    def testIndexFiles(self):
        """ Successfully indexing the files """
        a=Application(verbose=0, timed=0)
        a.add_catalog()
        a.index_file(r'greek.txt')
        a.close()
        # no errors raised? that will do for now

    def testQueryFile(self):
        """ Querying the index """ 
        a=Application(verbose=0, timed=0)
        a.add_catalog()
        a.index_file('greek.txt')
        para = open('greek.txt', 'r').read().split(' ')[0]
        res = a.query_index(para)
        assert res, "No results returned"
        # should be 1 brains back
        assert len(res) == 1, "More than 1 result returned"
       
if __name__=='__main__':
    unittest.main()
