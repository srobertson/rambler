import os

from Rambler import outlet

class OSConfigSource(object):
  configService = outlet('ConfigService')
  
  def assembled(self):
    self.configService.addConfigSource(self)
    
  def get(self, option):
    return os.environ[option]
