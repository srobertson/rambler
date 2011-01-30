import os

import yaml
import pkg_resources

from Rambler import outlet

class AppConfigSource(object):
  """Overide options based on enviroments such as development, staging, production"""
  
  configService = outlet('ConfigService')
  
  def assembled(self):
    env = os.environ.get('RAMBLER_ENV', 'devel')
    app_name = self.configService.get('application.name')
    self.data = None
    try:
      stream = pkg_resources.resource_stream(app_name, "config/%s.yml" % env)
      self.data = yaml.load(stream)
    except IOError:
      pass
      
    self.data = self.data or {}
      
    self.configService.addConfigSource(self)
  
  def get(self, option):
    return self.data[option]
    
    
  def set(self,option, value):
    pass