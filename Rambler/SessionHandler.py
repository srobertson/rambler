from Rambler import outlet
from Rambler.utils import getClass
from CompBinder import Binding

from handlers import DefaultHandler

class SessionHandler(DefaultHandler):
    """A SAX handler used to load sessions from xml

    >>> class MyLocalSession(object):
    ...   pass


    >>> class MySession(object):
    ...   pass

    >>> class MyRemoteInterface:
    ...   pass

    >>> class MyRemoteHomeInterface:
    ...   pass

    Don't look, this is a stupid hack so that our getClass() function
    can find the classes we defined in this doctest

    >>> import sys
    >>> sys.modules['SessionHandler'].MySession = MySession
    >>> sys.modules['SessionHandler'].MyLocalSession = MyLocalSession
    >>> sys.modules['SessionHandler'].MyRemoteInterface = MyRemoteInterface
    >>> sys.modules['SessionHandler'].MyRemoteHomeInterface = MyRemoteHomeInterface
    
    >>> from xml.sax import parseString
    >>> xml = '''
    ... <extension>
    ... <!-- comments are ignored -->
    ... <session>
    ...   <name>MySession</name>
    ...   <description>Sample session</description>
    ...   <class>SessionHandler.MySession</class>
    ...   <remote>SessionHandler.MyRemoteInterface</remote>
    ...   <home>SessionHandler.MyRemoteHomeInterface</home>
    ... </session>
    ... <!-- local sessions, don't have remote or home interfaces -->
    ... <session>
    ...   <name>MyLocalSession</name>
    ...   <description>Sample session</description>
    ...   <class>SessionHandler.MyLocalSession</class>
    ... </session>
    ... <entity></entity>
    ... </extension>
    ... '''

    SessionH<entity></entity>andlers need a SessionRegistry which needs a
    ComponentRegistry to function properly
    
    >>> from Rambler.CompBinder import CompBinder
    >>> from Rambler.SessionRegistry import SessionRegistry
    >>> sh = SessionHandler()
    >>> sr = sh.sessionRegistry = SessionRegistry()
    >>> compReg = sh.sessionRegistry.componentRegistry = CompBinder()

    Once the SessionHandler has been properly assembeled, we can send
    it to the standard parsing libraries

    >>> parseString(xml, sh)
    >>> compReg.bind()

    After parsing our xml and binding the SessionRegistry should have
    our component.
    
    >>> 'MySession' in sr.getSessionNames()
    True
    >>> sr.getSession('MySession') == MySession
    True
    >>> sr.getInterface('MySession') == MyRemoteInterface
    True
    >>> sr.getHomeInterface('MySession') == MyRemoteHomeInterface
    True
    
    """

    sessionRegistry = outlet("SessionRegistry")
    app = outlet("Application")
    
    def __init__(self):
        self._data = []
        self._ignoreData = True

    def assembled(self):
      self.app.registerHandler(self)
      
    def startElement(self, name, attrs):
        if name == "session":
            self._name = ""
            self._class = ""
            self._remote = None #
            self._home = None # 
            self._description = ""
            self._bindings = []
            self._ignoreData = False
        elif name == "bind":
            # This tag is optional in the file.  If we didn't reset it
            # for each bind tag, we'd end up keeping the first
            # specified binding for each of the following tags until
            # another bind tag specified it.
            
            self._allow_unassembled = False


    def characters(self, ch):
        if self._ignoreData:
            return
        
        self._data.append(ch)

    def endElement(self, name):
        if self._ignoreData:
            return
        
        data = "".join(self._data).strip()
        
        if name == "session":
            if self._home is not None:
                self._home = getClass(self._home)

            if self._remote is not None:
                self._remote = getClass(self._remote)
            self.sessionRegistry.addSession(
                self._name,
                getClass(self._class), 
                self._home,
                self._remote,
                self._bindings)
            self._ignoreData = True

            
        elif name == "bind":
            self._bindings.append(
                Binding(self._component, object, self._attribute, self._allow_unassembled)
                )

        elif name == "allow-unassembled":
            # Singleton element with no data.  If present, means true
            self._allow_unassembled = True
                                                
        else:
            name = '_' + name.replace('-', '_')
            setattr(self, name, data)

        self._data = []
    
if __name__=="__main__":
    import doctest
    doctest.debug('SessionHandler','SessionHandler')
