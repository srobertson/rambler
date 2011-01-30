from xml.dom import minidom
import os
import pkg_resources

class Node(object):

    def __init__(self, node):
        self.node = node
        
    def getString(self, tagName, element=None):
        if element is None:
            element = self.node

        elements = element.getElementsByTagName(tagName)
        if len(elements) > 0:
            return str(elements[0].childNodes[0].data)
        else:
            return ''
    
    def getInt(self, tagName, element=None):
        value = self.getString(tagName,element)
        return int(value)

class AppConfig(Node):
    """Reads a config file and returns a configured instance.

    Given the following bit of xml

    >>> confStr='''
    ... <app>
    ...  <context>itbe</context>
    ...  <name>TestApp</name>
    ...  <port>4100</port>
    ...  <user>juncture</user>
    ...
    ...  <extension>
    ...   <name>storage/postgres</name>
    ...   <options>
    ...    <key>host</key>
    ...    <string>localhost</string>
    ...    <key>empty</key>
    ...    <!-- verifies that app config can have comments and blank values -->
    ...    <string></string>
    ...    <key>connections</key>
    ...    <int>5</int>
    ...
    ...    <key>user</key>
    ...    <string>bob</string>
    ...    <key>tags</key>
    ...    <list>
    ...     <string>Important</string>
    ...     <string>Normal</string>
    ...          
    ...     <string>Not Important</string>
    ...     </list>
    ...     <key>map</key>
    ...     <dict>
    ...
    ...     <key>CA</key>
    ...     <string>California</string>
    ...     <key>FL</key>
    ...     <string>Florida</string>
    ...     </dict>
    ...     <key>listOfMaps</key>
    ...     <list>
    ...     <dict>
    ...     <key>key1</key>
    ...     <int>0</int>
    ...     <key>key2</key>
    ...     <int>3</int>
    ...     </dict>
    ...     </list>
    ...   </options>
    ...  </extension>
    ...
    ...  <extension>
    ...   <descriptor>/tmp/t/widget/widget.xml</descriptor>
    ...   <name>widget</name>
    ...  </extension>
    ... </app>
    ... '''

    >>> appConf = AppConfig.parseString(confStr)
    >>> appConf.port
    4100
    >>> appConf.name
    'TestApp'
    >>> appConf.context
    'itbe'
    >>> appConf.user
    'juncture'
    >>> appConf.group
    ''
    >>> appConf.extensions[0].name
    'storage/postgres'
    >>> appConf.extensions[0].options['host']
    'localhost'
    >>> appConf.extensions[0].options['connections']
    5
    >>> appConf.extensions[0].options['tags']
    ['Important', 'Normal', 'Not Important']
    >>> appConf.extensions[0].options['map']
    {u'CA': 'California', u'FL': 'Florida'}
    >>> appConf.extensions[0].options['listOfMaps'] == [{u'key1': 0, u'key2':3}]
    True
    
    The default extension descriptor should be in the exetnsiondir/extension/descriptor.xml
    >>> appConf.extensions[0].descriptor
    '/usr/lib/Rambler/extensions/storage/postgres/descriptor.xml'
    >>> appConf.extensions[0].extensionDir
    '/usr/lib/Rambler/extensions/'

    >>> appConf.extensions[1].name
    'widget'

    The location and the name of the descriptors can be overridden with the descriptor tag
    >>> appConf.extensions[1].descriptor
    '/tmp/t/widget/widget.xml'

    When this is done that changes the extensionDir. The extension dir
    should should be added to the path, so that from python you could
    import widget
    
    >>> appConf.extensions[1].extensionDir
    '/tmp/t/'
    
    """

    def __init__(self, node, fname=None):
       super(AppConfig, self).__init__(node)
       self.extensions = []
       for ext in node.getElementsByTagName('extension'):
           self.extensions.append(Extension(ext, fname))

    def parse(file):
        return AppConfig(minidom.parse(file), file)
    parse = staticmethod(parse)

        
    def parseString(confStr):
        return AppConfig(minidom.parseString(confStr))
    parseString = staticmethod(parseString)

    
    def getPort(self):
        return self.getInt('port')
    port = property(getPort)

    def getName(self):
        return self.getString('name')
    name = property(getName)

    def getContext(self):
        return self.getString('context')
    context = property(getContext)

    def getUser(self):
        return self.getString('user')
    user = property(getUser)

    def getGroup(self):
        return self.getString('group')
    group = property(getGroup)

    def getOptions(self):
        options = {}
        optionNodes = self.node.getElementsByTagName('options')
        for optionNode in optionNodes:
            options.update(createDictFrom(optionNode))
    
        return options
    options = property(getOptions)


# Sigh, oh look another type conversion dictionary
def createListFrom(node):
    l = []
    for child in node.childNodes:
        if child.nodeType == node.ELEMENT_NODE:
            l.append(converters[child.tagName](child))
    return l
                     

def createDictFrom(node):
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
                    convert = converters[node.tagName]
                except KeyError:
                    raise RuntimeError("Uknown tag <%s>" % node.tagName)
                value = convert(node)
                d[key] = value
                key = None
                
        node = node.nextSibling
    return d
                   

def trueOrFalse(node):
    val = node.firstChild.data.lower()
    if val == 'true':
        return True
    elif val == 'false':
        return False
    else:
        raise ValueError('Invalid value for bool types %s' % val)
    
def createStringFrom(node):
  if node.firstChild is not None:
     return str(node.firstChild.data)
  else:
     return ""  

converters = {
    'int': lambda node: int(node.firstChild.data),
    'string': createStringFrom, 
    'list': createListFrom,
    'dict': createDictFrom,
    'bool':trueOrFalse,
   
    }



                              
class Extension(Node):

    def __init__(self, node, fname=None):
        super(Extension, self).__init__(node)
        self.options = {}
        if fname is not None:
            self.appConfigLocation = os.path.dirname(os.path.abspath(fname))
        else:
            self.appConfigLocation = None # Config was loaded from a string

        optionNodes = node.getElementsByTagName('options')
        for optionNode in optionNodes:
            self.options.update(createDictFrom(optionNode))

        
    def getName(self):
        return self.getString('name')
    name=property(getName)

    def getDescriptor(self):
        descriptor = self.getString('descriptor')
        if descriptor == '':
            #extDir = os.path.join(self.defaultExtensionDir, self.name)
            #descriptor = os.path.join(extDir, 'descriptor.xml')
	    descriptor = pkg_resources.resource_filename(self.name, 'descriptor.xml')


        if not os.path.isabs(descriptor) and \
               self.appConfigLocation is not None:

            # Descriptors are relative to the app config file not
            # the program running it

            descriptor = os.path.join(self.appConfigLocation, descriptor)
            descriptor = os.path.abspath(descriptor)
            


        return descriptor
    descriptor=property(getDescriptor)

    def getExtensionDir(self):

        """Normally the extension is two directories above the
        descriptor file. However extensions can be loaded frum sub
        directories under the extension dir. For example the extension
        directory when the extension name is storage/postgres is 3
        directories above the descriptor."""
        
        parents = len(self.name.split(os.sep)) + 1
        path = os.path.dirname(self.descriptor)
        return path[:len(path) - len(self.name)]

    extensionDir = property(getExtensionDir)



# Make these class funcions available to the module so developers can use it like this
# >>> from Rambler import AppConfig
# >>> conf = AppConfig.parse(someFile)

parse=AppConfig.parse
parseString=AppConfig.parseString
