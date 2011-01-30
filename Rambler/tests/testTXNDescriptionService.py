import unittest

from omniORB import importIDL
importIDL('Widget.idl', ['-I../idl'])

from Rambler import Server
Server.init("giop:tcp::6666")
Server.loadDescriptor('widget.xml')

from Rambler.tests.Widget import Widget

class Test(unittest.TestCase):
    def setUp(self):
        self.ds = Server.getService("TXNDescriptionService")

    def testGetTransAttribute(self):
        ds = self.ds
        mode = ds.getTransAttribute('widget', 'name')
        assert mode == ds.RequiresNew, "Mode should be %s, is %s" % (ds.RequiresNew, mode)

        mode = ds.getTransAttribute('widget', 'wedgie')
        assert mode == ds.Required, "Mode should be %s, is %s" % (ds.Required, mode)

        mode = ds.getTransAttribute('doodle', 'name')
        assert mode == ds.Mandatory, "Mode should be %s, is %s" % (ds.Mandatory, mode)


        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))

    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
