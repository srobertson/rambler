from Rambler import outlet
import sys,md5,sets,gc

class ComponentReloader(object):
  componentRegistry = outlet('ComponentRegistry')
  eventChannel      = outlet('EventService')
  log               = outlet('LogService')
  
  def __init__(self):
    self.modStats = None
    # key is the dotted mod path, suitable for finding the given
    # module from sys.modules and the value is a tuple of path to
    # the module and md5
  
  def assembled(self):
    self.eventChannel.subscribeToEvent("Initializing", self, str, sys.maxint)
  
  def handleMessage(self, tid):
    self.modStats = self.scanComponents()
  
  def scanComponents(self):
    """Scan through the bound components building an MD5 of each of there
    source files."""
    
    moduleStats = {}
    # TODO: Keep track of which component is associated with each
    # file
    for comp in self.componentRegistry.bound.values():
      if type(comp) != type:
        modName = comp.__class__.__module__
      else:
        modName = comp.__module__
      
      if modName not in moduleStats:
        module = sys.modules[modName]
        modPath = module.__file__
        if not modPath.endswith('.py'):
          # It could end with a 'pyo' or a 'pyc'
          modPath = modPath[:-1]
          # if modPath still doesn't end with .py it might not be a .pyo or a .pyc
          assert modPath.endswith('.py'), "Warning %s doesn't end with .py" % modPath
        
        
        modFile = open(modPath)
        m = md5.new()
        
        chunk = modFile.read(1024)
        while chunk:
          m.update(chunk)
          chunk = modFile.read(1024)
        modFile.close()
        
        moduleStats[modName] = (modPath, m.digest())
    
    return moduleStats

  
  def checkForChanges(self):
    """Reload all files that have changed"""
    
    newStats = self.scanComponents()
    
    for modName, pathAndHash in newStats.items():
      if pathAndHash[1] != self.modStats[modName][1]:
        self.reload(modName)
    
    self.modStats = newStats
  
  def reload(self, moduleName):
    # Module has changed reload it
    
    self.log.info('Reloading module %s' % moduleName)
    # Look in the module before we reload it
    module = sys.modules[moduleName]
    
    newclasses = sets.Set()
    for k,v in module.__dict__.items():
      if isinstance(v, type) and v.__module__ == module.__name__:
        # It's a new style class definition
        newclasses.add(v)
    
    reload(module)
    
    for nclass in newclasses:
      reloadedClass = getattr(module, nclass.__name__)
      
      # Find all the instance of the new class that are in memory
      for r in gc.get_referrers(nclass):
        
        if getattr(r,'__class__', None) is nclass:
          r.__class__ = reloadedClass
        
        
    

    


