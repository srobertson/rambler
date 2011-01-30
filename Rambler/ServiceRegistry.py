from Rambler import load_classes,outlet
import types

class ServiceRegistry(object):

    """Provides information about the services have been registered, along
    with their class, and remote interface if any.
    
    >>> class MyService(object):
    ...   pass

    >>> sr=ServiceRegistry()

    This implementation needs a componentRegistry bound to it to work properly.
    >>> from Rambler.CompBinder import CompBinder
    >>> sr.componentRegistry = CompBinder()
    
    To add a service to the registry you call
    ServiceRegistry.addService(name, class, interface, bindings)

    name      - string representing the component name for the service
    class     - class that implements the componet
    interface - optional remote interface, if it's None this service
                is meant for local use only
    bindings  - list of components that should be bound to this service
    
    >>> serviceName = "MyService"    
    >>> sr.addService(serviceName, MyService(), None, [])

    Later on after all the components have been bound you can find
    out information about the service if you know it's name

    >>> sr.componentRegistry.bind()
    
    >>> service = sr.getService(serviceName)
    >>> type(service) == MyService
    True

    >>> remoteInterface = sr.getInterface(serviceName)
    >>> remoteInterface is None
    True

    Get serviceNames returns the list of currently registered services.
    >>> len(sr.getServiceNames()) == 1
    True
    >>> 'MyService' in sr.getServiceNames()
    True
    """

    componentRegistry = outlet("ComponentRegistry")
    #app = outlet('Application')
       
    def __init__(self):
        self.interfaces = {}
        # This should be set prior to this object being added to the componentRegistry
        # may want to introduce an event when a package(app or etxension) is added so that
        # the individual registries can load their components
        self.app_name = None

    def assembled(self):
      mod_names = ['Rambler', self.app_name] + [ext.name for ext in self.app.config.extensions]
      for mod_name in mod_names:
        #note: I'm considering renaming sessions to controllers
        mod_full_name = mod_name + ".services"
        home_interface = remote_interface = None
        for cls in load_classes(mod_full_name, object):
          if hasattr(cls, "provides"):
            name = cls.provides
          else:
            name = cls.__name__
          try:
            self.addService(name, cls(), None, [])
          except TypeError:
            # thrown if the constructor takes arguments, if so the class isn't
            # a service. We need a better way of identifying component types w/o
            # require an import of a class that makes a code dependency
            pass
      
    def addService(self, name, klass, interface, bindings):
        name = str(name)
        self.interfaces[name] = interface
        self.componentRegistry.addComponent(
            name,
            klass,
            bindings)

    def getServiceNames(self):
        return self.interfaces.keys()

    def getService(self, name):
        return self.componentRegistry.get(name)

    def getInterface(self, name):
        return self.interfaces[name]

