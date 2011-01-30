import logging
import logging.handlers
import platform
SysLogHandler = logging.handlers.SysLogHandler

# NOTE: Logging probably only works with python2.5 now
class MultiLineSysLogHandler(SysLogHandler):
    def emit(self, record):

        message = self.format(record)
        message = message.split('\n')
        if len(message) > 1:
            # Break it up, break it down
            for msg in message:
                rec = logging.LogRecord(record.name,
                                record.levelno,
                                record.pathname,
                                record.lineno,
                                msg,
                                (), # args will have already been substituted
                                None,
                                None)

                # Stupid hack, because we have no legitamet way to get our hands
                # on the Logger instance so we can call logging.makeRecord

                for attribute, value in AppLogger._data.items():
                    setattr(rec, attribute, value)
                SysLogHandler.emit(self, rec)
        else:
            SysLogHandler.emit(self, record)

class AppLogger(logging.Logger):
    """Add aditional data tha can be ouputed with log messages.

    """
    _data = {}

    def addData(klass, attribute, value):
        # Add's additional data to the log record if it hasn't been
        # set.  this check is neccary to prevent instatiating anotther
        # Application from stomping over the log name.
        if not klass._data.has_key(attribute):
            klass._data[attribute] = value
    addData = classmethod(addData)


    def makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None):
        """
        Overides the default logging method to insert the name of the
        app into the record.
        """

        return logging.Logger.makeRecord(self, name, level, fn, lno, msg, args, exc_info, func, self._data)
        record = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func)

        # Copy the extra attributes to this log record
        for attribute, value in self._data.items():
            setattr(record, attribute, value)
        return record



class LogService(object):

    """Simplifies the setup of logging for components. Components can
    have the logging service bound to them. Then, from the components
    perspective, the logSevice behaves identically to the logging
    facility built directly into python with additional information
    already provided.

    So for instance we can define a component like this

    >>> import sys
    >>> from Rambler import outlet
    >>> class Component(object):
    ...   log = outlet('LogService')
    ...   def someFunction(self):
    ...      self.log.info('Some function called!')

    Which provides an outlet named 'log' for the LogService to be
    bound to, and defines one method named someFunction(). Whenever
    someFunction() is called, our component will now log the string
    'Some function called!' to the logging facility.

    >>> from Rambler.CompBinder import CompBinder
    >>> compReg = CompBinder()
    >>> compReg.addComponent('LogService', LogService())
    >>> compReg.addComponent('MyComponent', Component())
    >>> compReg.bind()

    We use the compBinder to bind the component together. This set's
    the LogService to MyComponent's log outlet. 
    
    >>> myComp = compReg.lookup('MyComponent')
    >>> logService= compReg.lookup('LogService')
    >>> logService.setLevel(logService.WARN)

    Before demoing the facilities we first need to configure our
    logging options. For this test we send our messages to stdout so
    that doctest can compare the output.
     
    >>> logService.useStreamHandler(sys.stdout)

    And we set the format string.
    
    >>> logService.setFormat('%(name)s:%(levelname)s:%(message)s')
    
    Once that's done we can look up our component and invoke the
    someFunction() method.

    
    >>> myComp.someFunction()
    
    Notice, nothing happend, that's because we're logging at the info
    level and by default the logging facilities hides those
    messages. Here's what happens when we increase the verbosity.

    >>> logService.setLevel(logService.INFO)

    And try it again, this time we should get some output.
    >>> myComp.someFunction()
    MyComponent:INFO:Some function called!

    That's more like it. The logService this time spat out a
    record. One thing you should notice right off the bat is that the
    log message contained both the name the name of the component that
    emited the log message. This handy feature let's you control the
    verbosity of log messages on a component by component basis. So
    for instance rather than setting the logLevel to debug for all
    components like we did in the previous example, we can set the
    level specifically for the MyComponent. Like this:

    >>> logService.setLevel(logService.WARN, 'MyComponent')

    Now when we call someFunction() no details are displayede.

    >>> myComp.someFunction()

    Because we only log messages at the INFO level or higher now for
    my comp. Where as every other component will still log at the DEBUG level.

    If we defined a second component and had it bound it's messages
    would still log at the DEBUG level.

    >>> class Component2(object):
    ...   log = outlet('LogService')
    ...   def anotherFunction(self):
    ...      self.log.info('Another function called!')
    
    >>> compReg.addComponent('OtherComponent', Component2())
    >>> compReg.bind()
    >>> comp2 = compReg.lookup('OtherComponent')
    >>> comp2.anotherFunction()
    OtherComponent:INFO:Another function called!

    One finale ability the LogService provides is a method to add
    aditional information to log record. This is mostly used by the
    application to have it's name outputed with the message.

    For instance if we add the 'app' attribute with a value of 'MyApp'
    to the log service.

    >>> logService.addData('app', 'MyApp')

    We can change the format to inclued the  toke
    >>> logService.setFormat('%(app)s:%(name)s:%(levelname)s:%(message)s')

    And now when we call a method that would normally log information
    out 'MyApp' is included in the output.
    
    >>> comp2.anotherFunction()
    MyApp:OtherComponent:INFO:Another function called!
    
    """
    
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARN = logging.WARN

    defaultLevel = INFO

    def __init__(self):
        #logging.basicConfig()
        logging.getLogger('').setLevel(self.defaultLevel)
        logging.setLoggerClass(AppLogger)
        logging.handlers.MultiLineSysLogHandler = MultiLineSysLogHandler
        self.formatter = None


    def __binding__(self, compName, outletProvider):
        return logging.getLogger(compName)

    def setLevel(self, level, compNames=[None]):
        if type(compNames) == str:
            compNames = [compNames]
            
        for compName in compNames:
            logging.getLogger(compName).setLevel(level)

    def removeHandlers(self):
        for handler in logging.root.handlers:
            logging.root.removeHandler(handler)

    def useStreamHandler(self, stream):
        self.removeHandlers()
        handler = logging.StreamHandler(stream)
        if self.formatter:
            handler.setFormatter(self.formatter)
        logging.root.addHandler(handler)

    def useSyslogHandler(self, facility=logging.handlers.SysLogHandler.LOG_LOCAL6):
      # NOTE: (Mac & Linux) best to add the following line to /etc/syslog.conf 
      # local6.*             /var/log/rambler.log
      self.removeHandlers()
      system = platform.system()
      if system == 'Linux':
        address = '/dev/log'
      elif system == 'Darwin':
        address = '/var/run/syslog'
      
      handler = MultiLineSysLogHandler(address, facility)
      if self.formatter:
        handler.setFormatter(self.formatter)
        logging.root.addHandler(handler)

    def setFormat(self, format):
        self.formatter = logging.Formatter(format)
        
        for handler in logging.root.handlers:
            handler.setFormatter(self.formatter)

    def addData(self, attribute, value):
        """Adds additional information to be displayed using log format strings.
        """
        AppLogger.addData(attribute, value)
