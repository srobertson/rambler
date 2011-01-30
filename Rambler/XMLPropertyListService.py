from Rambler import outlet,option

from Money import Money

from dateutil.parser import parse
import datetime,sys

# blehq, it's annoying that this class would have to import
 
#from Rambler.QueryService import QueryService
try:
  from Rambler.ReportService import ReportHome
except ImportError:
  pass

# TODO: Change this class methods to mirror apple's NSCoder interface

class XMLPropertyListService(object):
    """Converts basic python and certain rambler objects to and from
    our PropertyList XML format
    
    >>> s = XMLPropertyListService()
    
    >>> s.convertToXML("blah")
    '<str>blah</str>'

    >>> s.convertToXML(1.1)
    '<float>1.1</float>'
    
    >>> s.convertToXML(['blah'])
    '<list><str>blah</str></list>'
    
    >>> s.convertToXML({'someKey':1})
    '<dict><key>someKey</key><int>1</int></dict>'
    
    >>> s.convertToXML(True)
    '<bool>True</bool>'

    >>> s.convertToXML(False)
    '<bool>False</bool>'
    
    >>> s.convertToXML(None)
    '<none/>'

    >>> s.convertToXML(Money(200.00))
    '<money>$200.00</money>'

    >>> s.convertToXML(datetime.date(2005,01,12))
    '<date>2005/01/12</date>'

    >>> s.convertToXML(datetime.datetime(2005,1,12, 12,0,0))
    '<datetime>2005/01/12 12:00:00</datetime>'

    >>> s.convertToXML(datetime.time(12,0,0))
    '<time>12:00:00</time>'

    >>> from xml.dom import minidom

    All the createXXXFrom statements expect a dom, we alias minidom's
    parseString furnction to keep this doctest from being to
    cluttered.
    
    >>> def toNode(xmlstr):
    ...   return minidom.parseString(xmlstr).documentElement
    

    >>> s.createStringFrom(toNode('<string>blah</string>'))
    'blah'

    >>> s.createIntFrom(toNode('<int>1</int>'))
    1

    >>> s.createFloatFrom(toNode('<float>1.50</float>'))
    1.5

    >>> s.createDateFrom(toNode('<date>2005/1/12</date>'))
    datetime.date(2005, 1, 12)

    >>> s.createDateTimeFrom(toNode('<datetime>2005/1/12 14:00:32</datetime>'))
    datetime.datetime(2005, 1, 12, 14, 0, 32)
    
    >>> s.createTimeFrom(toNode('<time>14:00:32</time>'))
    datetime.time(14, 0, 32)
    

    Money objects are an adaptabel format they can be specified with
    our without currency symbols and decimal places.

    >>> s.createMoneyFrom(toNode('<money>$1</money>'))
    Money('$1.00', 'USD')

    >>> s.createMoneyFrom(toNode('<money>2.05</money>'))
    Money('$2.05', 'USD')

    Money also has a special value NoneMoney which represents a field
    that has not been set or is null.
    >>> s.createMoneyFrom(toNode('<money>  </money>'))
    Money(None)
    
    >>> s.createMoneyFrom(toNode('<money></money>'))
    Money(None)

    >>> listXML='''
    ... <list>
    ...  <string>blah</string>
    ...  <string></string>
    ...  <int>1</int>
    ... </list>
    ... '''
    >>> s.createListFrom(toNode(listXML))
    ['blah', '', 1]

    >>> dictXML='''
    ... <dict>
    ...  <key>foo</key>
    ...  <string>blah</string>
    ...  <key>bar</key>
    ...  <string></string>
    ...  <key>baz</key>
    ...  <int>1</int>
    ...  <key>moola</key>
    ...  <money>$1.00</money>
    ... </dict>
    ... '''


    >>> results = {'foo':'blah', 'bar':'', 'baz': 1, 'moola': Money(1, 'USD')}
    >>> s.createDictFrom(toNode(dictXML)) == results
    True

    
    """
    compReg       = outlet('ComponentRegistry')
    eventChannel  = outlet('EventService')
    log           = outlet('LogService')
    configService = outlet('ConfigService')
    queryService = outlet('Query')

    mimeType = option('','mimeType', 'application/vnd.juncture-proplist+xml')

    def __init__(self):
        self.toXML = {
            int: lambda x: "<int>%s</int>" % x,
            float: lambda x: "<float>%s</float>" % x,
            str: lambda x: "<str>%s</str>" % x,

            # This assumes that we don't distinguish between strings
            # and unicode.
            unicode: lambda x: "<str>%s</str>" % x,
            bool: lambda x: "<bool>%s</bool>" % x,
            list: self.serializeList,
            type(None): lambda x: "<none/>",
            dict: self.serializeDict,
            Money: self.serializeMoney,
            datetime.date: lambda x: "<date>%s</date>" % x.strftime('%Y/%m/%d'),
            datetime.datetime: lambda x: "<datetime>%s</datetime>" % x.strftime('%Y/%m/%d %H:%M:%S'),
            datetime.time: lambda x: "<time>%s</time>" % x.strftime('%H:%M:%S'),
        }

        self.fromXML =  {
            'date':  self.createDateFrom,
            'datetime': self.createDateTimeFrom,
            'time': self.createTimeFrom,
            'int': self.createIntFrom,
            'float': self.createFloatFrom,
            'string': self.createStringFrom, # alias to str
            'email': self.createStringFrom, # alias to str
            'phone': self.createStringFrom, # alias to str
            'ssn': self.createStringFrom, # alias to str
            'str': self.createStringFrom,
            'list': self.createListFrom,
            'dict': self.createDictFrom,
            'bool': self.createBoolFrom,
            'money': self.createMoneyFrom,
            'zipcode': self.createStringFrom, # alias to str
            'none': self.createNoneFrom,
        }


    def assembled(self):

        # If the url dispatcher is available, let it know that we'll
        # handle encoding/decoding application/vnd.juncture-properties
        self.eventChannel.subscribeToEvent("Initializing", self, str, sys.maxint)
    def handleMessage(self,txnId):
        # TODO: I suppose this should be moved to some object titled xxxRPCxxxx
        try:
            urlDispatcher = self.compReg.lookup('URLDispatcher')
            if(urlDispatcher):

                
                for pythonType in self.toXML.keys():
                    self.log.debug('Registering to handle %s for %s', self.mimeType, pythonType)
                    urlDispatcher.registerMIMEAdaptor(self.mimeType, 
                                                      pythonType, 
                                                      self.encodeObject
                                                      )

                # Hack: I don't want this class to have knowledge of
                # higher level objects, but I don't have a good way to
                # register them yet. Case in port the QueryService
                urlDispatcher.registerMIMEAdaptor(self.mimeType, 
                                                  self.queryService, 
                                                  self.encodeObject
                                                  )
                
                urlDispatcher.registerMIMEAdaptor(self.mimeType, 
                                                  ReportHome, 
                                                  self.encodeObject
                                                  )


                    

                    
        except KeyError:
            # No URLDispatcher, no problem
            pass
        

    def encodeObject(self, obj):
        return "<response>%s</response>" % self.convertToXML(obj)

    def convertToXML(self, obj):
        serialize = self.toXML.get(type(obj), None)
        if serialize:
            return serialize(obj)
        else:
            # Not sure how to convert this object let's see if it support the Coder interface
            if hasattr(obj,'encodeWithCoder'):
                return obj.encodeWithCoder(self)
            else:
                raise TypeError, "%s does not support encoding!" % obj

    def convertFromXML(self, node):
        return self.createDictFrom(node)

        
    def serializeList(self,obj):
        inner = []
        for item in obj:
            inner.append(self.convertToXML(item))

        return "<list>" + "\n".join(inner) + "</list>"

    def serializeDict(self,obj):
        inner = []
        for key, item in obj.items() :
            inner.append("<key>%s</key>%s" % (key, self.convertToXML(item)))

        return "<dict>" + "".join(inner) + "</dict>"
    
    def serializeMoney(self, obj):
        return "<money>%s</money>" % obj


    def createDateTimeFrom(self, node):
        # not sure how to handle none, so if the node is empty we'll
        # let the exception raise.
        
        return parse(node.firstChild.data)

    def createDateFrom(self, node):
        return self.createDateTimeFrom(node).date()

    def createTimeFrom(self, node):
        return self.createDateTimeFrom(node).time()
            
    def createMoneyFrom(self, node):
        if node.firstChild is not None:
            return Money(str(node.firstChild.data))
        else:
            return Money(None)


        

    def createIntFrom(self, node):
        return int(node.firstChild.data)
    
    def createFloatFrom(self, node):
        return float(node.firstChild.data)
    
    def createListFrom(self, node):
        l = []
        for child in node.childNodes:
            if child.nodeType == node.ELEMENT_NODE:
                l.append(self.fromXML[child.tagName](child))
        return l


    def createDictFrom(self, node):
        d = {}

        node = node.firstChild

        # were going to iterate each child of the node. The XML should
        # look like this
        # <key>blah</key>
        # <string>foo</string>
        # <key>bar</key>
        # <int>0</int>

        # The following xml would be considered invalid.
        # <key>blah</key><key>bar</key> in fact it will be ignored

        # If two value tags are encountered in a row an error will be raised,
        # i.e. if you had xml like this
        # <key>blah</key>
        # <string>foo</string>
        # <string>Hello</string>



        key = None
        while node:
            if node.nodeType == node.ELEMENT_NODE:
                if node.tagName == 'key':
                    key = node.firstChild.data
                else:
                    if key is None:
                        raise RuntimeError("A key must be declared before a value!")

                    try:
                        convert = self.fromXML[node.tagName]
                    except KeyError:
                        raise RuntimeError("Uknown tag <%s>" % node.tagName)
                    value = convert(node)
                    d[str(key)] = value
                    key = None

            node = node.nextSibling
        return d


    def createBoolFrom(self, node):
        val = node.firstChild.data.lower()
        if val == 'true':
            return True
        elif val == 'false':
            return False
        else:
            raise ValueError('Invalid value for bool types %s' % val)

    def createStringFrom(self, node):
      if node.firstChild is not None:
         return str(node.firstChild.data)
      else:
         return ""  

    def createNoneFrom(self, node):
        return None

