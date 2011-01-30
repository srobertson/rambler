import os
from optparse import OptionParser
  

from Rambler import outlet

class RamblerOptions(OptionParser):
  def error(self, msg):
    if not msg.startswith('no such option'):
      super(RamblerOptions, self).error(msg)

class CommandLineConfigSource(object):
  configService = outlet('ConfigService')
  
  def assembled(self):
    
    parser = RamblerOptions()
    parser.add_option("-o", action="append", dest="options",
                      default=[], help='Set extension option -o "section:key=value"')
    (options, args) = parser.parse_args()
    self.data = {}
    for option in options.options:
      key, value = option.split('=',1)
      self.data[key] = value

    self.configService.addConfigSource(self)
    
  def get(self, option):
    val = self.data[option]
    # TODO: create a way to specify in the config service
    if val.lower() == 'false':
      val = False
    elif val.lower() == 'true':
      val = True
    return val
