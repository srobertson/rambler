from Rambler import outlet
from Rambler.utils import getClass
from CompBinder import Binding
from handlers import DefaultHandler

class ServiceHandler(DefaultHandler):
    serviceRegistry = outlet("ServiceRegistry")
    log = outlet("LogService")
    
    def __init__(self):
        self._data = []
        self._ignoreData = True
    
    def startElement(self, name, attrs):
            
        if name == "service":
            self._name = ""
            self._class = ""
            self._remote = None # 
            self._local = ""
            self._description = ""
            self._bindings = []
            self._ignoreData = False
        elif name == "bind":
            # This tag is optional in the file.  If we didn't reset it
            # for each bind tag, we'd end up keeping the first
            # specified binding for each of the following tags until
            # another bind tag specified it.
            
            self._allow_unassembled = False

        #else:
        #    self._ignoreDate = True

    def characters(self, ch):
        if self._ignoreData:
            return
        
        self._data.append(ch)

    def endElement(self, name):
        if self._ignoreData:
            return
        
        data = "".join(self._data).strip()
        
        if name == "service":
            self.log.debug("Registering %s %s which depends on %s"
                     % (self._name, self._class,
                        [b.name for b in self._bindings]
                        )
                     )

            if self._remote is not None:
                self._remote = getClass(self._remote)
                
            self.serviceRegistry.addService(
                self._name,
                getClass(self._class)(), # instantiate the service before passing it to the compRegistry
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
        
