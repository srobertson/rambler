import os
import sys
import json
  

from Rambler import outlet


class CommandLineConfigSource(object):
  """Treats the commandline as a config source.
  
  Options can be overidden like so
  -<option> value
  
  Example:
  $ ramblerapp <app> -once true

  Would set the key 'once' to the value of true
  
  Values are parsed using the json module.
  """
  configService = outlet('ConfigService')
  
  def assembled(self):
    self.data = {}
    
    if sys.argv[0].endswith('ramblerapp'):
      argv = sys.argv[2:]
    else:
      argv = sys.argv[1:]
      
    while argv:
      token = argv.pop(0)
      if token.startswith('-'):
        key = token[1:]
        try:
          value = argv.pop(0)
        except IndexError:
          value = None
      elif token.startswith('--'):
        key = token[2:]
        value = argv.pop(0)
      else:
        if 'application.args' not in self.data:
          self.data['application.args'] = []
        self.data['application.args'].append(token)
        continue
        
      try:
        value = json.loads(value)
      except (ValueError, TypeError):
        pass
      self.data[key] = value
    

    self.configService.addConfigSource(self)
    
  def get(self, option):
    return self.data[option]
