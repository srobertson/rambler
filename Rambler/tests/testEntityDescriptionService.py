import unittest

from omniORB import importIDL
importIDL('Widget.idl', ['-I../idl'])

from Rambler import Server
Server.init("giop:tcp::6666")
Server.loadDescriptor('widget.xml')

from Rambler.tests.Widget import Widget

class Test(unittest.TestCase):
    def setUp(self):
        self.ds = Server.getService("EntityDescriptionService")

    def testGetFields(self):
        fields = self.ds.getFields("Widget")

        cmrFields = []
        cmpFields = []
        for field in fields:
            if not field.isRelation():
                cmpFields.append(field)
            else:
                cmrFields.append(field)
        
        assert (len(cmpFields) == 3), "Widget should have 3 CMP fields.  Has %s" % len(cmpFields)
        assert (len(cmrFields) == 5), "Widget should have 5 CMR fields.  Has %s" % len(cmrFields)

        for field in cmpFields:
            assert field.getName() in ['primaryKey', 'wedgie', 'name'], "Invalid field name: %s" % field.getName()

        for field in cmrFields:
            assert field.getName() in ['other1', 'other2', 'parent', 'children', 'doodle'], "Invalid field name: %s" % field.getName()

    def testGetFieldsByClass(self):
        fields = self.ds.getFields(Widget)

        cmpFields = []
        for field in fields:
            if not field.isRelation():
                cmpFields.append(field)
        fields = cmpFields

        assert (len(fields) == 3), "Widget should have 3 fields.  Has %s" % len(fields)
        
        for field in fields:
            assert field.getName() in ['primaryKey', 'wedgie', 'name'], "Invalid field name: %s" % field.getName()


    def testGetField(self):
        field = self.ds.getField("Widget", "primaryKey")

        assert field.getName() == 'primaryKey'
        assert field.getType() == str, "Type should be str, is: %s" % field.getType()

        field = self.ds.getField("Widget", "wedgie")

        assert field.getName() == 'wedgie'
        assert field.getType() == int, "Type should be int, is: %s" % field.getType()

    def testGetFieldByClass(self):
        field = self.ds.getField(Widget, "primaryKey")

        assert field.getName() == 'primaryKey'
        assert field.getType() == str, "Type should be str, is: %s" % field.getType()

        field = self.ds.getField(Widget, "wedgie")

        assert field.getName() == 'wedgie'
        assert field.getType() == int, "Type should be int, is: %s" % field.getType()


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))

    return suite

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        Server.orb.destroy()
