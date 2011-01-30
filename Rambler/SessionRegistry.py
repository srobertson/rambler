import os
import sys
import types

from Rambler import outlet, option, load_classes



class SessionRegistry(object):

    """Keeps track of session objects, not to be confused with the
    SessionManager which keeps track of login sessions.

    A session is a component that shares features in common with both
    Services and Entities. A session is used to track a particular
    conversation with a client. Session's when created only live for
    the life of a transaction and are never shared whith any other
    clients. Clients typically start a transaction, create a session
    and then invoke one or more method calls on the object and finally
    commit the transaction. At the end of which the session instance
    is destroyed and any state that it was tracking is removed.
    They're information is not persisted in the database.

    Probably the most practical use for a session would be for streaming chunks
    of data to the server.

    For instance you might create an upload session, that opens a
    temporary file when it's created and appends data to it every time
    you call the write method on it. When close is called we copy it's
    contents to the real file name.
    

    >>> import tempfile

    >>> class UploadSession(object):
    ...  def create(fname):
    ...    return UploadSession(fname)
    ...
    ...  def __init__(self, fname):
    ...    self.fname = fname
    ...    self.tmpname = tempfile.mktemp()
    ...    self.tempfile = open(self.tmpname, 'w')
    ...
    ...  def write(self, data):
    ...    self.tempfile.write(data)
    ...
    ...  def close(self):
    ...    self.tempfile.close()
    ...    os.rename(self.tmpname, self.fname)

    Interfaces are typically defined in idl, but we'll create some
    bogus classes for demonstration purposes.

    >>> class UploadSessionInterface:
    ...   pass

    >>> class UploadSessionHomeInterface:
    ...   pass

    With a class like this a client can upload the file in chunks,
    which gives the client a chance to free up system resources or
    report progress etc... after every call to write().

    The session is registered with the session like this.

    >>> sr=SessionRegistry()

    This implementation needs a componentRegistry bound to it to work properly.
    >>> from Rambler.CompBinder import CompBinder
    >>> sr.componentRegistry = CompBinder()
     
    To add a session to the registry you call
    SessionRegistry.addSession(name, class, homeInterface,
    remoteInterface, bindings)

    name      - string representing the component name for the service
    class     - class that implements the componet
    homeInterface - interface the clients will use to create sessions
    remoteInterface - interface to preform operations
    bindings  - list of components that should be bound to this session
    
    >>> sessionName = "UploadSession"    
    >>> sr.addSession(sessionName, UploadSession,
    ...  UploadSessionHomeInterface, UploadSessionInterface, [])

    Later on after all the components have been bound you can find
    out information about the session if you know it's name

    >>> sr.componentRegistry.bind()

    >>> sessionHome = sr.getSession(sessionName)
    >>> sessionHome == UploadSession
    True

    >>> sr.getHomeInterface(sessionName) == UploadSessionHomeInterface
    True

    >>> sr.getInterface(sessionName) == UploadSessionInterface
    True

    >>> 'UploadSession' in sr.getSessionNames()
    True

    """
    componentRegistry = outlet("ComponentRegistry")
    configService = outlet('ConfigService')
    log = outlet("LogService")
    app = outlet('Application')
    
    app_name = option('application','name')

    def __init__(self):
        self.interfaces = {}
        self.homeInterfaces = {}
        
    def assembled(self):
      mod_names = ['Rambler', self.app_name] + [ext.name for ext in self.app.config.extensions]
      for mod_name in mod_names:
        #note: I'm considering renaming sessions to controllers
        mod_full_name = mod_name + ".controllers"
        home_interface = remote_interface = None
        for cls in load_classes(mod_full_name, object):
          if hasattr(cls, "provides"):
            name = cls.provides
          else:
            name = cls.__name__
          self.addSession(name, cls, home_interface, remote_interface, [])
              
    
    @staticmethod
    def __python_mods_in(path):
      mods = set()
      for item in os.dir(path):
        is_python.match(item)
        mods.add(os.splitext(item)[0])
      return mods
       
      
    def addSession(self, name, klass, home, remote, bindings):        
        name = str(name)
        self.log.debug('Adding session %s' % name)
        self.homeInterfaces[name] = home
        self.interfaces[name] = remote
        self.componentRegistry.addComponent(
            name,
            klass,
            bindings)

    def getSessionNames(self):
        return self.interfaces.keys()

    def getSession(self, sessionName):

        # Todo, rethink whether the compregistry should instantiate
        # classes, maybe make the ServiecRegistry handle that detail.
        
        return self.componentRegistry.get(sessionName)

    def getHomeInterface(self, sessionName):
        return self.homeInterfaces[sessionName]

    def getInterface(self, sessionName):
        return self.interfaces[sessionName]

    
