#!/usr/bin/python

import inspect
import errno
import os
import sys


if sys.hexversion > 0x02070000:
  import unittest
else:
  # Use unittest2 for python versions older than 2.7
  import unittest2 as unittest


import yaml


# gahh, redundant from application
from Rambler.CompBinder import CompBinder
from Rambler.ServiceRegistry import ServiceRegistry
from Rambler.SessionRegistry import SessionRegistry
from Rambler.ciConfigService import ciConfigService, DictConfigSource
from Rambler.ErrorFactory import ErrorFactory
from Rambler.RunLoop import RunLoop, Port
#from Rambler.EventChannel import EventChannel
from Rambler.LoggingExtensions import LogService
from Rambler import Component, coroutine

 
class FakeApp(object):
  def __init__(self, extension_names):
    self.config = FakeConfig(extension_names)
    
class FakeConfig(object):
  
  def __init__(self, extension_names):
    self.extensions =[]
    for name in extension_names:
      self.extensions.append(FakeExtension(name))
      
class FakeExtension(object):
  def __init__(self, name):
    self.name = name


class TestCase(unittest.TestCase):
  # By default Rambler.TestCase assumes that if your TestCase is named TestBlah, you are
  # testing a componet named Blah. You can overide this behavior by setting 
  # componentName = 'SomeComponent'
  
  # if a component requires a specific extension to be loaded they should set it here
  # ex extra_extensions =  ['rambler.net']
  extra_extensions = []
  
  # Sometimes you want to test a component that has dependencies on another component
  # which shouldn't be used during the test. For example, you may not want to test your
  # current component with a fake database component rather than one that actually 
  # talks to an external databas. TestCases can set default_components to a dictionary 
  # of component name, component. These components will be used rather than the ones 
  # found in the extensions.
  # For example test_components = {'EventService': FakeEventService} would use the
  # FakeEventService rather than the one found in rambler.core
  test_components = {}
  
  test_options = {}
  
  # List of events this test component will publish, the TestCase will automatically register
  # to publish this event. An error will be thrown if some other component has registered
  # to exlusively publish the event.
  publishing = []
  
  _fixtures = {}
  
  app_dir = None
  
  
  def setUp(self):    
    os.environ['RAMBLER_ENV'] = 'test'
    compReg = CompBinder()
    self.compReg = compReg
    
    
    # add the default components before we load any other extensions
    for comp_name, comp in self.test_components.items():
      compReg.addComponent(comp_name, comp)
      
    # Gather list of extensions the test should load by default
    extensions = set(self.extra_extensions)
    
    # Locate the extension dir

    if self.app_dir:
      app_dir = self.app_dir + '/'
    else:
      app_dir = os.path.dirname(sys.modules[self.__class__.__module__].__file__)

    ext_dir = ''
    while app_dir != '/':
      app_dir = os.path.dirname(app_dir)
      ext_dir = os.path.join(app_dir, 'extensions')
      if os.path.isdir(ext_dir):
        extensions.update(os.listdir(ext_dir))
        break

    # These components are component loaders. They find other componets
    # to load.
    app = FakeApp(extensions)
    app.name = self.extension

    for comp in [SessionRegistry, ServiceRegistry, ErrorFactory]:
      name = comp.__name__
      inst = comp()
      # Fake the app object so our loaders know what extensions they should look for
      # components in
      inst.app = app
      inst.app_name = app.name
      compReg.addComponent(name, inst)
    
    ls = LogService()

    log_dir = os.path.join(app_dir, 'log')
    log_path = os.path.join(log_dir, 'test.log')
    try:
      log_file = open(log_path,'a')
    except IOError, e:
      os.makedirs(log_dir)
      log_file = open(log_path,'a')
        

    ls.useStreamHandler(log_file)
    compReg.addComponent('LogService', ls)
    compReg.addComponent('RunLoop', RunLoop)
    compReg.addComponent('PortFactory', Port)
    compReg.addComponent("Component", Component)
    compReg.bind()
    
    self.eventChannel = compReg.lookup('EventService')
    self.eventChannel.registerEvent("Initializing", self, str)
    
    for event in self.publishing:
      self.eventChannel.registerEvent(event, self, object)
    
    compReg.addComponent('EventService', self.eventChannel)
    
    
    serviceRegistry = compReg.lookup('ServiceRegistry')
    configService = ciConfigService()
    #self.test_options['application'] = {'name': app.name}
    configService.setAuthoritativeSource(DictConfigSource(self.test_options))
    
    configService.set_default('application.name', app.name)
    configService.set_default('application.path', app_dir)
    configService.set_default('system.hostname', 'localhost')
    
    self.configService = configService
    serviceRegistry.addService("ConfigService", configService, None, [])
    serviceRegistry.addService("Application", app, None, [])
    
    compReg.bind()
    
    if len(compReg.needsBinding):
      raise RuntimeError("Test could not run because " +
              compReg.analyzeFailures())
              
    self.comp = compReg.lookup(self.componentName, None)
    compReg.log.info('========= %s =========' % self)
    super(TestCase, self).setUp()
    
  def tearDown(self):
    self._fixtures.clear()
    self.compReg.unbind()
    pass
    
  
  @classmethod
  def coroutine(cls, func):
    def test_coroutine(self):
      op = self.CoroutineOperation(func(self),self.queue)
      self.wait_for(op)
      op.result
    return test_coroutine
    
  @property
  def componentName(self):
    """Returns the name of the component being tested. """
    return self.__class__.__name__[4:]
    

  
  @property
  def testDir(self):
    classFileName = inspect.getfile(self.__class__)
    return os.path.dirname(classFileName)

      
  @property
  def extension(self):
    """Guesses the extension name. Based on the location of the test."""

    dirName = os.path.dirname(self.testDir)
    # walk up the dir looking for __init__.py, when we run
    parts = []
    
    while os.path.exists(os.path.join(dirName, '__init__.py')):
      dirName, package = os.path.split(dirName)
      parts.insert(0, package)
    
    return ".".join(parts)

  def componentFor(self, name):
    """Returns the specified component name"""
    return self.compReg.lookup(name)
  
  def __getattr__(self, name):
    """Lookup a component as if they are attributes of the test"""
    if('compReg' in self.__dict__): # can't lookup components until setUp has been called
      try:
        return self.componentFor(name)
      except KeyError:
        # Object implementing __getattr__ need to raise attribute errors 
        pass
    raise AttributeError(name)
  
  def fixturePathFor(self, name):
    """Returns the path to the fixtures specified by name. Right now this should be a yaml
    file but in the future it maybe other formats depending on what's there"""
    
    return os.path.join(self.testDir,  'fixtures/%s.yml' % name)
    

  def fixtures(self, name):
    if name not in self._fixtures:
      fixturePath = self.fixturePathFor(name)
      self._fixtures[name] = yaml.load(open(fixturePath))
    return self._fixtures[name]
    
  def publishAppEvent(self, event, value):
    """Publish an event normally sent by the Application object like Intializing or Shutdown"""
    self.eventChannel.publishEvent(event, self, value)
  #publishAppEvent = publish_app_event


  # Useful methods for writing tests that interact with the RunLoop
  
  @property
  def run_loop(self):
    if '_run_loop' not in self.__dict__:
      self._run_loop = self.RunLoop.currentRunLoop()
    return self._run_loop

  @property
  def queue(self):
    return self.Scheduler.queue
  
  def quit(self, *args):
    # Stop the runloop while giving anything scheduled to run in the current
    # loop a chance to execute first.
    self.run_loop.waitBeforeCalling(0, self.run_loop.stop)
    
  def quit_with_error(self, failure):
    # Called to handle a defered failure
    self.quit()
    return failure
    
  def quit_with_result(self, result):
    self.quit()
    return result

  def wait_for(self, operation, timeout=5):
    """Test methods can use this method to execute an operation via the runLoop
    and wait for it to complete.
    
    Parameters:
      operation: Operation to be scheduled on the run loop
      [callback]: Method to invoke after operation has finished. Note if 
                  callback is used you must explicitly call self.run_loop.stop()
                  in your unit test.
      [timeout]: Max time in seconds to wait before giving up on the operation.
    Discussion:
    Operations are queued in the default scheduler and then the run loop is started.
    It is an error to use this method if the RunLoop is arleady active.
    
    
    """
      
    operation.add_observer(self, 'is_finished', 0, self.quit)
    self.queue.add_operation(operation)
    self.wait(timeout)
    return operation.result


  def wait(self, timeout=5):
    self.run_loop.waitBeforeCalling(timeout, self.quit)
    self.run_loop.run()
    
  def wait_for_call(self, method,  *args, **kw):
    """Invokes a generator and returns the result when complete"""
    timeout = kw.pop('timeout', 5)
    d = self.Scheduler.call(method, *args)
    d.addCallbacks(self.quit_with_result, self.quit_with_error)
    self.wait(timeout)
    
    if hasattr(d.result, 'raiseException'):
      d.result.raiseException()
    return d.result
    
  def observe_value_for(self, key_path, of_object, change, *args):
    args[0](of_object)
    of_object.remove_observer(self, key_path)
  
  def option_for(self, key):
    return self.configService.get(key)
    
 
if __name__ == '__main__':
    unittest.main()
    

             
                  
        
