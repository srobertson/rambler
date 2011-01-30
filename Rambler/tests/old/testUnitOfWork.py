import unittest, os
import ZODBPool

from UnitOfWork import UnitOfWork
from time import time

class MyObject:

    def __init__(self):
        self._pk = time()
        
    def _get_primaryKey(self):
        return self._pk

    
class Test(unittest.TestCase):
    def setUp(self):
        self.uow = UnitOfWork()


    def tearDown(self):
        pass

    def testRegisterNew(self):
        """Registers an object as new, and checks to see if getStatus
        returns the appropriate state for the given object and that
        the object is returned when you call getNew()"""
        o = MyObject()
        pk = o._get_primaryKey()

        self.uow.registerNew(o)
        assert self.uow.getStatus(pk) == UnitOfWork.NEW
        assert o in self.uow.getNew()
        

    def testRegisterClean(self):
        """Registers an object as clean, and checks to see if getStatus
        returns the appropriate state for the given object and that
        the object is returned when you call getClean()"""
        o = MyObject()
        pk = o._get_primaryKey()

        self.uow.registerClean(o)
        assert self.uow.getStatus(pk) == UnitOfWork.CLEAN
        assert o in self.uow.getClean()
        
    def testRegisterDirty(self):
        """Registers an object as clean, then dirty and checks to see
        if getStatus returns the appropriate state for the given
        object and that the object is returned when you call
        getDirty() but isn't in the list when you call getClean()."""

        o = MyObject()
        pk = o._get_primaryKey()

        self.uow.registerClean(o)
        self.uow.registerDirty(o)
        assert self.uow.getStatus(pk) == UnitOfWork.DIRTY
        assert o in self.uow.getDirty()


    def testRegisterRemoved(self):
        
        """Registers one object for each of the other states first.
        Then attempts to remove each object , checks each objects
        status afterwards and verifies that the objects are only found
        in the list returned by getRemoved() and not in any of the
        lists returned by getNew(), getClean() or getDirty()."""

        o1 = MyObject()
        pk1 = o1._get_primaryKey()
        self.uow.registerNew(o1)
        
        o2 = MyObject()
        pk2 = o2._get_primaryKey()
        self.uow.registerClean(o2)
        self.uow.registerDirty(o2)

        o3 = MyObject()
        pk3 = o3._get_primaryKey()
        self.uow.registerClean(o3)

        self.uow.registerRemoved(o1)
        self.uow.registerRemoved(o2)
        self.uow.registerRemoved(o3)

        assert self.uow.getStatus(pk1) == UnitOfWork.REMOVED
        assert self.uow.getStatus(pk2) == UnitOfWork.REMOVED
        assert self.uow.getStatus(pk3) == UnitOfWork.REMOVED

        assert o1 not in self.uow.getNew()
        assert o2 not in self.uow.getDirty()
        assert o3 not in self.uow.getClean()
        
    def testGet(self):

        """Registers an object for each of the four states and calls
        get for each of them. Call get for an object that doesn't
        exist and verify that it returns None. Call get for an object
        that doesn't exist with an alternative default and verify that
        that value is returned."""

        o1 = MyObject()
        pk1 = o1._get_primaryKey()
        self.uow.registerNew(o1)
        
        o2 = MyObject()
        pk2 = o2._get_primaryKey()
        self.uow.registerClean(o2)
        self.uow.registerDirty(o2)

        o3 = MyObject()
        pk3 = o3._get_primaryKey()
        self.uow.registerClean(o3)

        o4 = MyObject()
        pk4 = o4._get_primaryKey()
        self.uow.registerClean(o4)
        self.uow.registerRemoved(o4)

        assert o1 == self.uow.get(pk1)
        assert o2 == self.uow.get(pk2)
        assert o3 == self.uow.get(pk3)
        assert o4 == self.uow.get(pk4)

        assert self.uow.get(12345) is None
        assert self.uow.get(12345, "default") == "default"

    def testClear(self):
        """ Registers an object in each status, then calls clear and
        verifies that all the get calls return empty lists. """
        
        o1 = MyObject()
        pk1 = o1._get_primaryKey()
        self.uow.registerNew(o1)
        
        o2 = MyObject()
        pk2 = o2._get_primaryKey()
        self.uow.registerClean(o2)
        self.uow.registerDirty(o2)

        o3 = MyObject()
        pk3 = o3._get_primaryKey()
        self.uow.registerClean(o3)

        o4 = MyObject()
        pk4 = o4._get_primaryKey()
        self.uow.registerClean(o4)
        self.uow.registerRemoved(o4)

        self.uow.clear()

        assert not self.uow.getNew()
        assert not self.uow.getClean()
        assert not self.uow.getDirty()
        assert not self.uow.getRemoved()

    def testInvalidStates(self):

        """Verify that exceptions are thrown when.
        - Attepmt to register an object as new if it's already
          registered as clean,dirty,removed
        - Attempt to register an object as clean if it's already
          registered as new, dirty, removed
        - Attempt to register an object as dirty if it's not currently
          registered as clean
        - Attepmt to register an object removed if it isn't registered
          as new, clean, dirty"""

        new = MyObject()
        clean = MyObject()
        dirty = MyObject()
        removed = MyObject()
        not_it = MyObject()

        self.uow.registerNew(new)
        self.uow.registerClean(clean)
        self.uow.registerClean(dirty)
        self.uow.registerDirty(dirty)
        self.uow.registerClean(removed)
        self.uow.registerRemoved(removed)


        self.assertRaises(ValueError, self.uow.registerNew,(new))
        self.assertRaises(ValueError, self.uow.registerNew,(clean))
        self.assertRaises(ValueError, self.uow.registerNew,(dirty))
        self.assertRaises(ValueError, self.uow.registerNew,(removed))

        self.assertRaises(ValueError, self.uow.registerClean,(new))
        self.assertRaises(ValueError, self.uow.registerClean,(clean))
        self.assertRaises(ValueError, self.uow.registerClean,(dirty))
        self.assertRaises(ValueError, self.uow.registerClean,(removed))

        self.assertRaises(ValueError, self.uow.registerDirty,(new))
        self.assertRaises(ValueError, self.uow.registerDirty,(dirty))
        self.assertRaises(ValueError, self.uow.registerDirty,(removed))
        self.assertRaises(ValueError, self.uow.registerDirty,(not_it))

        self.assertRaises(ValueError, self.uow.registerRemoved,(not_it))
        






def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))

    return suite

if __name__ == '__main__':
    unittest.main()
