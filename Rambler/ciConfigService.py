#NOTE: This module is old and crufty. It export several services that need
# to be loaded from the descriptor. These service such as BasicService
# and Entity need to have outlet for the config service so they can
# properly throw ciErrors, rather than the corba errors they have
# now. Also the configservice seems to be transactionally aware
# because at one point we gave it the ability to edit files. Not sure
# the state of this.

from Rambler import outlet, error
from optparse import OptionParser

import ConfigParser, os
from types import ListType

from Rambler.fstr import fstr

from Rambler.Events import Vote



#from epo import NoSectionError, NoOptionError
#from CosTransactions import VoteCommit, VoteReadOnly, VoteRollback

from copy import deepcopy
from tempfile import mktemp
from types import TupleType, ListType, StringType


# noDefault is an empty tuple because a default of None might be
# likely
_noDefault = ()


# hacky, this will be set in ciConfigService.assembled
log = None


class ciConfigService(object):
  """Allows the client to read server side configuration options"""
  eventChannel = outlet("EventService")
  log      = outlet("LogService")

  errorFactory = outlet("ErrorFactory")
  NoSectionError = error(0, description='No section')
  NoOptionError  = error(1, description='Section has no option')  

  
  #any time that the file is different than what's in memory - the file is dirty
  isDirty = False  
  
  def __init__(self):
    # This prebinding is uselful mostly for testing purposes
    DictConfigSource.configService = self

    #self._defaults = DictConfigSource({})
    self._defaults = {}
    self._authoritativeSource = None
    self._configSources = []


  def assembled(self):   
    global log
    log = self.log
    
    for option,default in self._defaults.items():
      self.eventChannel.registerEvent(option, self, type(default))

  def setAuthoritativeSource(self, source):
    """Insert the config source into the head of the list and set
    the authorativeSource attribute so that future add's will be
    inserted behind it."""

    self._configSources.insert(0, source)
    self._authoritativeSource = source
    
  def set_default(self, option, default):
    # todo don't raise an error if the defaults equal each other
    if option in self._defaults:
      if self._defaults[option] == default:
        self.log.warn("Duplicate default for %s", option)
      else:
        raise ValueError("Default for %s has already been set to %s" % (option, self._defaults[option]))
    else:
      try:
        self.eventChannel.registerEvent(option, self, type(default))
      except AttributeError:
        # thrown if event channel hasn't been bound
        pass
      self._defaults[option] = default



  def addConfigSource(self, source):
    # insert the source into the list of config sources to check,
    # the sources are checked in the opposit order that they're
    # added. I.e. sources added later are checked before sources
    # added sooner. If the authorativeSource is set, we ensure
    # that it is always checked fisrt
    
    # Keep CommandLineConfigSource first
    # TODO implement system for specfying order of ConfigSources
    if source.__class__.__name__ == "CommandLineConfigSource":
      self._configSources.insert(0, source)
    else:
      self._configSources.insert(1, source)
      
    if hasattr(source, 'keys'):
      for key in source.keys():
        self.log.info('registering '+ key)
        self.eventChannel.registerEvent(key, self, object)
      
  
  def get(self, key, default=_noDefault):
    if default is _noDefault:
      default = self._defaults.get(key, _noDefault)
    
    ret = _noDefault
    
    for source in self._configSources:
      try:
        ret = source.get(key)
        break
      except self.errorFactory.error, e:
        # Couldn't find requesed cat/key in this
        # source.  Ignore the error and try the next
        if e not in (self.NoSectionError, self.NoOptionError):
          # it's an error produced by the errorFactory, but we don't know what it is
          raise
      except KeyError:
        pass
      except:
        self.log.exception("Error checking config source %s for %s", source, key)
        raise
    
    if ret is _noDefault and default is not _noDefault:
      ret = default

    if ret is not _noDefault:
      if isinstance(ret, basestring):
        ret = fstr(ret)
        if hasattr(ret, 'render'):
          ret = ret.render(locals=self)
      return ret
    else:
      raise self.NoOptionError(description="Couldn't find specified option: %s" % key)

  def getint(self, option, default=_noDefault):
    val = self.get(option, default)
    return int(val)

  def getbool(self, category, key,default=_noDefault):
    val = self.getint(category, key, default)
    return bool(val)
    

  def keyiter(self):
    seen = set(self._defaults)
    for key in seen:
      yield key
    for source in self._configSources:
      if hasattr(source, 'keys'):
        for key in source.keys():
          if key not in seen:
            seen.add(key)
            yield key
            
  def items(self):
    for key in sorted(self.keyiter()):
      yield key, self.get(key)

  def set(self,  key, value):
    # Our system assumes we only have TWO config files, one of
    # which is a readonly, file based config, and the other is
    # an entity based config.  Setw will be done to the first
    # non-readonly config source, but in our system that will
    # always be the entity based config source.
    
    self.eventChannel.publishEvent(key, self, value)
    
    for source in self._configSources:
      try:
        source.set(key, value)
        return
      except (ReadOnlyError, AttributeError):
        pass

    # TODO: If we got here, we didn't find any config source
    # that was not readonly.  This would be an error. :)
    
  def __getitem__(self, item):
    # warning potential recursive method
    return self.get(item)

  def getList(self, category, key):
    """ Seperate list functions for the IDL """
    try:
      value = self.get(category, key)
      if type(value) == StringType:
        value = (value,)
      if value is None or (len(value) == 1 and value[0] is None):
        # We got back a placeholder value that the entity
        # config source uses.  Convert it to an empty tuple.
        value = ()
      return value
    except NoOptionError:
      return ()

  def setList(self, category, key, value):
    """ Seperate list functions for the IDL """
    self.set(category, key, value)

  def prepare(self, txnId):
    commit = 0
    for source in self._configSources:
      result = source.prepare()
      if result == self.txn.VoteRollback:
        self.eventChannel.publishEvent("vote", self, self.txn.VoteRollback)
        return
      elif result == self.txn.VoteCommit:
        commit += 1

    if commit:
      self.eventChannel.publishEvent("vote", self, self.txn.VoteCommit)
    else:
      self.eventChannel.publishEvent("vote", self, self.txn.VoteReadOnly)

  def commit(self, txnId):
    for source in self._configSources:
      source.commit()

  def rollback(self, txnId):
    for source in self._configSources:
      source.rollback()

class DictConfigSource(object):

  """Configsource based off of dictionaries. Each key in the
  dictioanry represents a section and the value is another
  dictionary whose keys and values are the section data.

  DictConfigSource objects need the ConfigService for error codes
  
  >>> DictConfigSource.configService = ciConfigService()

  And the configservice needs an errorFactory
  >>> from Rambler.ErrorFactory import ErrorFactory 
  >>> DictConfigSource.configService.errorFactory = ErrorFactory('ConfigService')

  Example Usage:

  First initialize our dictionary of dictionaries. 
  >>> defaults = {}
  >>> section1 = {'key1':'foo'}
  >>> defaults['section1'] = section1
  >>> section2 = {'key2': 'blah'}
  >>> defaults['section2'] = section2
  
  >>> configSource = DictConfigSource(defaults)

  We now have a config source with two sections, section1 and section2.
  
  >>> key1 = configSource.get('section1', 'key1')
  >>> key1
  'foo'

  section1 doesn't have a key2 so this should raise the NoOptionError
  >>> configSource.get('section1', 'key2')  # doctest: +ELLIPSIS
  Traceback (most recent call last):
    ...
  ciError: Section has no option...


  Asking for a section that does exist will raise the NoSectionError like this
  >>> configSource.get('section3', 'fake key') # doctest: +ELLIPSIS
  Traceback (most recent call last): 
    ...
  ciError: No section...
  
  """

  configService = outlet('ConfigService')
  

  def __init__(self, data):
    self.data = data

  def get(self,  option):
    return self.data[option]
    
  def keys(self):
    return self.data.keys()




class BasicConfigSource(object):
  # Note: Name is misleading should be ConfigParser config source or something.
  configService = outlet('ConfigService')
  
  def __init__(self, configFilePath):
    self._configFilePath = configFilePath
    self._config = cfg = ConfigParser.ConfigParser()
    cfg.read(configFilePath)

    # Used when a transaction is rolledback
    self._cleanConfig = deepcopy(self._config)
    #any time that the file is different than what's in memory - the file is dirty
    self._isDirty = False  
    self._tmpFile = None

  def get(self, section, option):
    try:
      ret = self._config.get(section, option)
    except ConfigParser.NoSectionError:
      raise self.configService.NoSectionError(
        description="No section named " + section)
    except ConfigParser.NoOptionError:
      raise self.configService.NoOptionError(
        description="No option %s in section %s" % (option, section))

    if len(ret.split('\n')) > 1:
      ret = tuple(ret.split('\n'))
    return ret
  
  def set(self, section, option, value):
    if type(value) == TupleType:
      value = '\n'.join(value)
    self._config.set(section,option,value)
    self._isDirty = True

  def prepare(self):
    if self._isDirty:
      # Try to save the file out to a temp file
      try:
        tmpFileName = mktemp()
        tmpFile = open(tmpFileName, 'w')
        self._config.write(tmpFile)
        tmpFile.close()
        self._tmpFile = tmpFileName
      except IOError, e:
        return VoteRollback
        self.log.exception("Got IOError attempting to write config file. %s" % e)
        return
      return VoteCommit
    else:
      return VoteReadOnly
      
  def commit(self):
    if self._isDirty:
      # copy the tmpfile over the old file, make
      # a new copy of the configuration in case we need to
      # rollback on the next transaction, and reset the
      # various flags and vairables that were set during editing
      try:
        os.rename(self._tmpFile, self._configFilePath)
      except:
        # Anything other than 0 means we got an error
        self.log.exception("Could not move new config file from %s to %s" % (self._tmpFile, self._configFilePath))
        self._isDirty = False
        self._tmpFile = None
        self._config = deepcopy(self._cleanConfig)
        return

      self._isDirty = False
      self._tmpFile = None
      self._cleanConfig = deepcopy(self._config)

  def rollback(self):
    self._isDirty = False
    self._tmpFile = None
    self._config = deepcopy(self._cleanConfig)

class ReadOnlyBasicConfigSource(BasicConfigSource):
  def set(self, section, option, value):
    raise ReadOnlyError()
  
class EntityConfigSource(object):
  settingHome = outlet('settingHome')
  eventChannel = outlet("EventService")
  configService = outlet("ConfigService")
  
  def assembled(self):
    self.eventChannel.subscribeToEvent("Initializing", self.init, str)


  def init(self, tid):
    # Pull in all Setting entities and store them
    # in a dictionary based lookup scheme so we can
    # retrieve them easier.  We only store the values
    # of a particular setting and its primary key.
    # This reduces lookup overhead but adds a bit more to
    # the less often used update commands.
    self._config = config = {}

    sh = self.settingHome
    settings = sh.getAll()
    for setting in settings:
      cat = config.get(setting._get_category(), {})
      key = setting._get_key()
      if cat.has_key(key):
        # Dealing with a list
        val = cat[key]
        if type(val) != ListType:
          val = [val]
        val.append((setting._get_value(), setting._get_primaryKey()))
      else:
        val = (setting._get_value(), setting._get_primaryKey())
      cat[key] = val
      #cat[setting._get_key()] = (setting._get_value(), setting._get_primaryKey())
      config[setting._get_category()] = cat

  def get(self, category, key):
    if self._config.has_key(category):
      cat = self._config[category]
      if cat.has_key(key):
        val = cat[key]
        if type(val) == ListType:
          ret = []
          for v, pKey in val:
            ret.append(v)
          return tuple(ret)
        else:
          return val[0] # The value is stored as (value,primaryKey)
      else:
        raise self.configService.NoOptionError("Unable to find key for category/key: %s/%s" % (category, key))
    else:
      raise self.configService.NoSectionError("Unable to find category/key: %s/%s" % (category, key))

  def set(self, category, key, value):
    if self._config.has_key(category):
        cat = self._config[category]
        if cat.has_key(key):
          if type(cat[key]) == ListType:
            # Lists are icky.  Nuke all the old entities and add new ones
            val = cat[key]
            for v, pKey in val:
              setting = self.settingHome.findByPrimaryKey(pKey)
              self.settingHome.remove(setting)
          elif cat[key][0] == None:
            # For now we're assuming that None is not a
            # valid value.  No normal setting can have a
            # value of None.  If we see None as a value,
            # we're assuming it's an empty list.
            setting = self.settingHome.findByPrimaryKey(cat[key][1])
            self.settingHome.remove(setting)
              
          if type(value) == ListType:
            val = []
            if len(value) > 0:
              for v in value:
                setting = self.settingHome.create(category, key, v)
                val.append((v, setting._get_primaryKey()))
            else:
              # If we have an empty list, we need to
              # store a placeholder element in the DB,
              # otherwise when the server is restarted,
              # we'll be unable to find this value
              # again.  The ciConfigService will convert
              # our placeholder record into an empty
              # list in getList.
              setting = self.settingHome.create(category, key, None)
              val.append((None, setting._get_primaryKey()))
            cat[key] = val
            return
          else:
            pKey = cat[key][1]
            setting = self.settingHome.findByPrimaryKey(pKey)
            setting._set_value(value)
            cat[key] = (value, pKey)
            return

    # If we got here, we don't have an entity to
    # store this cat/key.  Make a new one
    if type(value) == TupleType or type(value) == ListType:
      settings = []
      if len(value) > 0:
        for val in value:
          setting = self.settingHome.create(category, key, val)
          settings.append((val, setting._get_primaryKey()))
      else:
        setting = self.settingHome.create(category, key, None)
        settings.append((None, setting._get_primaryKey()))
      value = settings
    else:
      setting = self.settingHome.create(category, key, value)
      value = (value, setting._get_primaryKey())
      
    cat = self._config.get(category, {})
    cat[key] = value
    self._config[category] = cat

  def prepare(self):
    return VoteReadOnly
  
  def commit(self):
    pass
  
  def rollback(self):
    pass

#Q. Is this component even used?
import xml.dom.minidom
class XMLConfigSource:
  configService = outlet("ConfigService")
    

  def __init__(self, confFile):
    self.document = xml.dom.minidom.parse(confFile)


  def get(self, section, key):
    category = self.document.getElementsByTagName(section)
    if len(category) == 0:
      raise self.configService.NoSectionError, "No section named " + section
    category=category[0]

    items = category.getElementsByTagName(key)
    if len(items) == 0:
      raise self.configService.NoOptionError, "No option %s in section %s" % (
        key, section)
    
    values = []
    for item in items:
      # Make sure it's a text node
      if len(item.childNodes):
        assert item.childNodes[0].nodeType == 3 
        values.append(item.childNodes[0].data.strip())
      else:
        values.append('')

    if len(values) == 1:
      return values[0]
    else:
      return values

  def set(self, section, option, value):
    raise ReadOnlyError()


  def prepare(self):
    return VoteReadOnly
  
  def commit(self):
    pass
  
  def rollback(self):
    pass


class ReadOnlyError(Exception):
  pass
