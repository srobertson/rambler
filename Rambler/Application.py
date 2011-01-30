import os, signal, sys,threading, socket, pickle, resource
import pwd, grp, select, time, traceback
import random
from hashlib import md5

import pkg_resources

from Rambler import outlet, Bundle, Component
from Rambler.CompBinder import CompBinder, Binding
from Rambler.ciConfigService import ciConfigService, DictConfigSource
from Rambler.ErrorFactory import ErrorFactory
from Rambler.Services import ServiceHandler
from Rambler.ServiceRegistry import ServiceRegistry
from Rambler.SessionRegistry import SessionRegistry
from Rambler.SessionHandler import SessionHandler
from Rambler.LoggingExtensions  import LogService
from Rambler.MessageBus import MessageBus, IMessageObserver
from Rambler.twistedlogging import  StdioOnnaStick
from Rambler.defer import Deferred

from zope.interface import classProvides, implements


from Rambler.RunLoop import RunLoop, Port


from Rambler.handlers import CompoundHandler
from Rambler import AppConfig


from xml.sax import make_parser,SAXParseException
from xml.sax.handler import feature_namespaces
import StringIO





class AppErrNeedsLoading(Exception):
    def __str__(self):
        return 'You need to either call load() or daemonize() '\
               'priror to using this function.'

class AppErrLoaded(Exception):
    def __init__(self, msg=''):
        self.msg = msg
        
    def __str__(self):
        return self.msg or 'You can not call this function while the application is running.'

class AppMisconfigured(Exception):
    def __init__(self, configError):
        self.configError = configError
        
    def __str__(self):
        return self.configError

class AppErrDuringLoad(Exception):
    def __init__(self, loadErr):
        self.loadErr = loadErr

    def __str__(self):
        msg = "Error during startup of the child process"
        if self.loadErr is not None:
            msg += "\n" + self.loadErr
        return msg

# try to find an unused port, useful for testing
                


class Application(object):
    implements(IMessageObserver)

    eventChannel = outlet('EventService')
    componentRegistry = outlet('ComponentRegistry')
    configService = outlet('ConfigService')
    txn = outlet('TransactionService')
    txnDescriptionService = outlet('TXNDescriptionService')
    log = outlet('LogService')
    timerService = outlet('TimerService')
    msgBus = outlet('MessageBusService')


    MISCONFIGURED = 0  # Config file can not be interpreted
    LAUNCHING     = 1  # App spawned but has not checked in yet
    STARTING      = 2  # App has begun to load itself
    STARTED       = 3  # App is fully loaded and can service requests
    STOPPING      = 4  # App has been asked to shutdown, no new request will be accepted
    STOPPED       = 5  # App successfully shutdown, it can be relaunched if desired
    CRASHED       = 6  # App crashed during startup, or unexpectantly died, additional details may be available
    UNATTACHED    = 7  # The app appears to be running but we're not controlling it
    ATTACHING     = 8  # App was found unattached, and it was notified that we want to cntroll it.
                       # waiting for it to reestablish connection to controller
    NOTRESPONDING = 9  # App was sent a sig HUP but didn't check back in the prorper time


    CONFIG_UNMODIFIED = 0
    CONFIG_MODIFIED   = 1
    CONFIG_MISSING    = 2

    # the name on the bus we send life cycle message to
    CONTROLLER_REF="com.codeit.rambler.controller"
    

    __slots__ = ['name', 'config','appDir', 'pidPath', 'configFile', 'context',
                 'status', 'configError', 'digest',
                 'controllerAddress', 'deferred', '_dh',
                 'appBundle','mainRunLoop',
                 ]



    ## Instance Methods ##

    def __init__(self, appDir, controllerAddress = "", authoritativeOptions = None, **defaultComps):
        self.appBundle = Bundle(appDir)
        self.configFile = self.appBundle.pathForResource('app.conf')
        self.status = Application.STOPPED
        self.controllerAddress = controllerAddress

        self.digest = ""
        self.name = os.path.basename(os.path.abspath(appDir))

        if not authoritativeOptions:
            authoritativeOptions = {}
        authoritativeOptions['application.name'] =  self.name
        authoritativeOptions['application.path'] =  os.path.abspath(appDir)
        authoritativeOptions['system.hostname'] =  socket.gethostname()

        self.loadConfig()

        logService = defaultComps.get("LogService")

        if logService is None:
            # everybody needs logging, especally  during devel. If one
            # isn't passed to us, set one up to go directly to syslog

            logService = LogService()
            logService.addData('app', self.name)

            logService.setFormat('[%(app)s:%(name)s:%(levelname)s] %(message)s')
            logService.useSyslogHandler()
            defaultComps["LogService"] = logService

        # load the module that adapts our logging to somthething that twisted deferred's and failuers likes
        defaultComps["StdioOnnaStick"] =  StdioOnnaStick()

        defaultComps["ErrorFactory"] = ErrorFactory()

        

        # these two are sessions so we put their classe rather than
        # instances in componentRegistry
        defaultComps["RunLoop"] = RunLoop
        defaultComps["PortFactory"] = Port

	# our os signal handling, has to be as light weight as
	# possible. so any signal this app receives will be processed
	# by the sameRunLoop that started this app (hopefully you
	# don't have it bogged down running long synchronous tasks)

	self.mainRunLoop = RunLoop.currentRunLoop()

        if not defaultComps.has_key("MessageBusService"):
            defaultComps["MessageBusService"] = MessageBus()


        self.componentRegistry = CompBinder()
        for compName, component in defaultComps.items():
            self.componentRegistry.addComponent(compName, component)

        serviceRegistry = ServiceRegistry()
        # blehq, serviceRegistry needs the application name in order to
        # load services that have been configured by convention 
        serviceRegistry.app_name = self.name
        serviceRegistry.app = self
        self.componentRegistry.addComponent("ServiceRegistry", serviceRegistry)
        # need to bind ServiceRegistry's before we can add the App as
        # a service.
        self.componentRegistry.bind()
        

        # Set up the config service with authorative options
        configService = ciConfigService()
        for option, value in authoritativeOptions.items():
          configService.set_default(option, value)

	try: # todo, remove this crap afte we pull the last bits of corba/omniorb from Rambler
	    from epo import ciIApplication, ciIConfigService
	    # Register the application service so that we can be remotely managed
	    serviceRegistry.addService("Application", self, ciIApplication, [])
	    serviceRegistry.addService("ConfigService", configService, ciIConfigService, [])
	except ImportError:
	    # corba isn't installed
	    serviceRegistry.addService("Application", self, None, [])
            # Note: ConfigService is placed into the componentRegistry
            # via the service registry. Not sure why I did this.
	    serviceRegistry.addService("ConfigService", configService, None, [])



        self.componentRegistry.bind()

    def _get_msgBusName(self):
        return "com.codeit.rambler.app." + self.name
    msgBusName = property(_get_msgBusName)


    def loadConfig(self):

        configStatus = self.getConfigStatus()
        # Check to see if our config file has changed
        if configStatus == Application.CONFIG_UNMODIFIED and self.configError:
            # Config file hasn't changed and we're still misconfigured
            raise AppMisconfigured(self.configError)
        elif configStatus == Application.CONFIG_MISSING:
            self.configError = 'Missing config file %s' % self.configFile
            raise AppMisconfigured(self.configError)
        elif configStatus == Application.CONFIG_UNMODIFIED:
            return
        
        assert configStatus == Application.CONFIG_MODIFIED
                
        self.configError = ""
        self.config = AppConfig.parse(self.configFile)


    def claimName(self, name, obj):
        # register an object on either the local or remote bus
        self.msgBus.claimName(name, obj)

    def sendMessage(self, toObjectOrRef, subject, *args, **kw):
        return self.msgBus.sendMessage(toObjectOrRef, subject, *args, **kw)
    

    def getConfigStatus(self):
        md5sum = md5()

        try:
            configFile = open(self.configFile)
        except IOError:
            return Application.CONFIG_MISSING
        
        for line in configFile:
            md5sum.update(line)
        configFile.close()

        digest = md5sum.digest()
        if self.digest == "":
            self.digest = digest
            return Application.CONFIG_MODIFIED
        
        elif self.digest == digest:
            return Application.CONFIG_UNMODIFIED
        else:
            self.digest = digest
            return Application.CONFIG_MODIFIED


     

    def load(self):
        
        """Sets up the message bus if we have a controller then set
        our status to starting.

        onStatusSent will be called once the controller acknowledges
        our start. 
        """

        if self.getStatus() not in (Application.MISCONFIGURED, Application.STOPPED, Application.CRASHED):
            raise AppErrLoaded()

        self.deferred = Deferred()
            
        old=signal.signal(signal.SIGINT, self.sighandler )
        old=signal.signal(signal.SIGTERM, self.sighandler )
        #signal.signal(signal.SIGCHLD, self.onSigChild)
        signal.signal(signal.SIGHUP, self.onSigHUP)
	# todo: it wolud be nice if we restored theses signals when
	# the App is done.


        # This won't fully assemble the App, that doesn't happen until
        # we load the core descriptor. But we should have just enough
        # to log and send LCP messages

        if self.controllerAddress:

            # if we have a controller we need to delay STARTING until
            # our MessageBus connects to the the message bus on the
            # other end
            

            # If we're told to communicate our state to a appManager,
            # retry every 30 seconds to connect if we get an error.
            
            deferred = self.msgBus.connect(self.controllerAddress, timeout=0, delay=random.random())
            deferred.addCallback(self.onConnect)
            

        else:
            RunLoop.currentRunLoop().waitBeforeCalling(0, self.setStatus, Application.STARTING)

        return self.deferred
                
        
    def _load(self, **defaultComps):

        try:
            self.switchUser()
            self.switchGroup()

            self._dh = CompoundHandler()

            #self.shutdownLock = threading.Event()

            # Here's were we'd load up the config file, the core services,
            # any extensions and register this object as a service, fire
            # off the Initilization event to tell the components that
            # registration is complete and finally signal our controlling
            # app (if any) that we've open for business.

            compReg = self.componentRegistry

            # this add's the ability to read services from a descriptor file
            compReg.addComponent("ServiceHandler", ServiceHandler())
            compReg.addComponent("Component", Component)


            # these add the ability to read sessions and register them
            # globaly from descriptors.
            
            compReg.addComponent("SessionRegistry", SessionRegistry())
            compReg.addComponent("SessionHandler", SessionHandler())
            
            compReg.bind()
            

            self.registerHandler(compReg.get("ServiceHandler"))
           
            
            # TODO: Get rid of descriptor all together
            coreDescriptor = pkg_resources.resource_stream('Rambler', 'descriptor.xml')
            

            self.__loadExtension(coreDescriptor)

            # Bind the core service to each other
            compReg.bind()
            self.eventChannel.registerEvent("Initializing", self, str)
            self.eventChannel.registerEvent("Shutdown", self, str)


            self.registerHandler(compReg.get("EntityHandler"))
            self.registerHandler(compReg.get("RelationHandler"))
            self.registerHandler(compReg.get("TXNHandler"))

            self.txnDescriptionService.setTransAttribute('Application', 'shutdown', self.txnDescriptionService.Never)
            self.txnDescriptionService.setTransAttribute('Application', 'noOp', self.txnDescriptionService.Supports)

            options = {}
            for extension in self.config.extensions:
                options[extension.name] = extension.options

            self.configService.addConfigSource(DictConfigSource(options))

            # TODO: Descriptors are depricated, the following loop is disabled to see if
            # we can operate w/o it. Remove for statement after 5/01/10 
            self.log.debug("loading %s extensions from %s" % (len(self.config.extensions), self.configFile))
            for extension in self.config.extensions:
                break
                self.log.debug("  " + extension.name)

                if os.path.exists(extension.descriptor):
                    self.log.debug("    reading " + extension.descriptor)

                    #if extension.extensionDir not in sys.path:
                    #    sys.path.append(extension.extensionDir)

                    self.__loadExtension(open(extension.descriptor))
                else:
                    self.log.warn("    missing " + extension.descriptor)

            compReg.bind()

            try:
                # we need a transaction service before continuing, right
                # now the real txnservice is in our corba extension,
                # so.. if the corba bridge wasn't loaded we need to use an
                # in memory one

                compReg.lookup("TransactionService")
            except KeyError:
                from Rambler.LocalTXNService import LocalTXNService
                compReg.addComponent("TransactionService", LocalTXNService())
                compReg.bind()

            if len(self.componentRegistry.needsBinding):
                raise RuntimeError, "Server could not start because\n" +\
                      self.componentRegistry.analyzeFailures()

            self.txn.set_timeout(0)
            self.txn.begin()
            
            self.eventChannel.publishEvent("Initializing", self, self.txn.get_transaction_name())
            self.txn.commit(0)
            self.txn.set_timeout(60)
                
            self.setStatus(Application.STARTED)


        except AppMisconfigured:
            # We don't want to report this as a crash, or do we?...
            raise
        except Exception:
            
            # We got an error while loading, notify our controller
            # with it then reraise the exception to kill this
            # application
            
            msg = "".join(traceback.format_exception(*sys.exc_info()))
            self.setStatus(Application.CRASHED, msg)
            raise

    def noOp(self):
        # needed to make corba clients happy
        pass
    

    
    def registerHandler(self, handler):

        """Adds a handler that will be called for each SAX event fired
        while reading the descriptor."""
        self._dh.addHandler(handler)

    def __loadExtension(self, extension):
        """Load the extension found in the xml.

        extension can either be a string containing vaild xml or an
        open file handle that containts valid xml.
        """
        # Create a parser
        parser = make_parser()
        
        # Tell the parser we are not interested in XML namespaces
        parser.setFeature(feature_namespaces, 0)

        # Tell the parser to use our handler
        parser.setContentHandler(self._dh)

        # Parse the descriptor file
        try:
            if type(extension) == str:
                extension = StringIO.StringIO(extension)

            parser.parse(extension)
        except IOError, e:
            self.log.exception("Error loading descriptor file.\n%s" % e)
            raise
        except SAXParseException,e:
            
            # Make it easier to see when you have malformed xml by
            # raising a runtime error to hide the sax stack trace
            
            raise RuntimeError, "Error parsing descriptor %s " \
                  "error: %s line: %s column: %s" %\
                  (e.getSystemId(), e.getMessage(),
                   e.getLineNumber(), e.getColumnNumber())

    
    def isRunning(self):
        return self.status == Application.STARTED

    def lookup(self, component):
        return self.componentRegistry.lookup(component)


    def getStatus(self):
        return self.status

    def setStatus(self, status, details=None):
        """Sends a life cycle notification to the controlling process, if any."""

        
        if self.msgBus.mode == self.msgBus.MODE_CONNECTED:

            # if we have a controller address, send our status
            # notification, to it and wait for the LCP to call either
            # onStatusSent() or onStatusError()
            deferred = self.msgBus.sendMessage(self.CONTROLLER_REF, status, self.name, details)
            deferred.addCallback(self.onStatusSent, status)
            deferred.addErrback(self.onStatusError, status) 
        else:
            # we don't have a controller so we pretend we heard
            # acknowledgements from it.
            self.onStatusSent(None, status)

    def reattach(self):
        if self.controllerAddress and self.msgBus.mode != self.msgBus.MODE_CONNECTED:
            # we were started controlled, but looks like the
            # controller went away and now is asking to reestablish
            # connection
            
            deferred = self.msgBus.connect(self.controllerAddress, timeout=0, delay=random.random())
            deferred.addCallback(self.onReconnect)
            


    def switchUser(self):

        if self.config.user: # The config file has a user, so attempt to switch to it
            user = self.config.user
            if os.getuid() != 0:
                 raise RuntimeError("Can't switch to user '%s' specified in %s "
                                   "because this application wasn't started as root." % 
                                   (user, self.configFile))
                
            try:
                uid = pwd.getpwnam(user)[2] # The UID.
            except KeyError:
                raise RuntimeError("Can't switch to user '%s' specified in %s "
                                   "because the user does not exist." % 
                                   (user, self.configFile))
            os.setuid(uid)

    def switchGroup(self):
        if self.config.group: # The config file has a group, attempt to switch to it
            group = self.config.group
            if os.getuid() != 0:
                 raise RuntimeError("Can't switch to group '%s' specified in %s "
                                   "because this application wasn't started as root." % 
                                   (group, self.configFile))
                
            try:
                gid = gwd.getgrgid(group)[2] 
            except KeyError:
                raise RuntimeError("Can't switch to group '%s' specified in %s "
                                   "because the group does not exist." % 
                                   (group, self.configFile))
            os.setgid(gid)


    def shutdown(self):
        runLoop = self.mainRunLoop
        if self.status ==  Application.STARTED:

            self.log.info("Shutdown requested")

            # the LCP will notify us in onStatusSent when the
            # controller receives STOPPING, at which point will start
            # the shutdown sequence
            
            runLoop.callFromThread(self.setStatus, Application.STOPPING)
        elif self.status in (Application.STOPPING, Application.STOPPED):
            self.log.warn("Application received shutdown request even though it's either stopped or stopping.")
        else:

            # hmm looks like we've been asked to shutdown when we're
            # in a state where we can't do that, yet, try it again in
            # runLoops next pass. This could happpen if we're started
            # just so we can be shutdown.
            
            self.log.info("Retrying shutdown request app status "
                          "is %s instead of STARTED(%s)" %
                          (self.status, Application.STARTED) )
            
            runLoop.callFromThread(self.shutdown)


    # ASYNC event listeners

    def sighandler(self, signum, frame):
	# important! signal handlers can be called at any time
	# inbetween any python instruction. we have to take great
	# pains to ensure that no locks are aquired by a signal
	# handler or we'll dead lock. That's why we wrap this shutdown
	# call in an extra callFromThread even though self.shutdown()
	# does the same thing. Reason for this is I believe the python
	# logging module uses locks, so we don't want to try acquiring
	# one of those while this is running. If someone knows whether
	# logging is signal safe, we could relax this and just call
	# self.shutdown()

        self.mainRunLoop.callFromThread(self.shutdown)


    # sigh, the message bus isn't smart enough to attempt resending
    # messages that we're queued for a foreign bus if an error was
    # encountered during connect, therefore we have to wait until
    # connect suceeds before we attempt to claimName and the same
    # thing goes for sending the first message.

    def onConnect(self, address):
        # connect succeeded, claim the name, when that succeeds, send our status
        deferred = self.msgBus.claimName(self.msgBusName, self)
            
        deferred.addCallback(lambda name: self.setStatus(Application.STARTING))
        deferred.addErrback(lambda failure: failure.printTraceback())

    def onReconnect(self, address):
        # make sure we reclaim our name
        deferred = self.msgBus.claimName(self.msgBusName, self)
        
        # send the message, don't wait for a response (which is what
        # we do in self.setStatus called from onConnect)
        
        deferred.addCallback(lambda name: self.sendMessage(self.CONTROLLER_REF,
                             Application.STARTED, self.name, None))


    def onSigChild(self, signum, frame):
        self.mainRunLoop.callFromThread(self.reapChildren)

    def reapChildren(self):
        while(1):
            # keep calling waitpid, until it throws error 10, which
            # means their are no more dead children to reap.
            try:
                os.waitpid(0,0)
            except OSError, e:
		if e.errno == 10:
		    break
		else:
		    raise

    def onSigHUP(self, signum, frame):
        self.mainRunLoop.callFromThread(self.reattach)


    def onMessage(self, context, subject):
        assert subject == "shutdown"
        self.shutdown()



    def onStatusSent(self, response, status):
        self.status = status

        if status == Application.STARTING:
            # our controller (if there was one ack our
            try:
                self._load()
                self.deferred.callback(self)
            except Exception, e:
                self.deferred.errback(e)
                raise
            
        elif status == Application.STOPPING:
            # our controller knows were stopping now and not crashing
            # so let's tell our components to shutdown
            
            try:
                self.eventChannel.publishEvent("Shutdown", self, '')
            except:
                # we died during shutdown, althought this should be
                # reported as a crash,we don't want a crash handler to try
                # and restart us, after all we died when we were stopping
                
                self.log.exception('Did not shutdown cleanly!')

            # Now that the components are offline, we'll tell the
            # Server that we're done, and then stop the RunLoop as
            # soon as the controller aknowledeges us
            self.setStatus(Application.STOPPED)
        

        elif status in (Application.STOPPED, Application.CRASHED):
            self.log.info("Shutdown Complete")
            RunLoop.currentRunLoop().stop()

    def onStatusError(self, failure, status):
        # Couldn't notify our controller, it could be temporarily
        # down, let's continue about our merry way.
        self.log.debug("%s got error %s while notifying controller of status %s,"
                      " continuing." % (self.name, failure.getErrorMessage(), status))
        
        self.onStatusSent(None, status)
    
                

def main():
    
    """Used by ramblerapp, a script that is created automatically for
    us by setuptools, to load an appBundle into it's own process."""

    import os,sys

    # Todo: this should really double fork (on posix platforms, not
    # sure about windows which doesn't have fork), I if you start appMan from the command line
    # and send it a ^C any sub app it starts also get's that ^C which
    # isn't what we want.

    from optparse import OptionParser
    parser = OptionParser()

    parser.add_option("-d", dest="daemonize", action="store_true", default=False,
		      help="Run this process as a background daemon")
    parser.add_option("-v", dest="logLevel",  action="count", default=0,
                      help="Increase logging verbosity by one for each -v specified on the commandline")

    parser.add_option("-b", dest="breakAt", default=None,
                      help="Use python debugger to break @ file:line, needs -f")

    parser.add_option("-o", action="append", dest="options",
                      default=[], help='Set extension option -o "section:key=value"')


    (options, args) = parser.parse_args()

    # app's can be started using the ramblerapp shell command, in that
    # case the first argument must be the path to the application
    # bundle. If the name of the script isn't ramblerapp say it's
    # "foo" then we assume this is an application whose bundle is
    # under /usr/lib/Rambler/extensions.

    # Todo: come up with a better way to specify the rambler
    # extensions directory. Perhaps check home directories as well,
    # that might make development eaiser

    scriptname = os.path.basename(sys.argv[0])
    controllerAddress=""
    appBundlePath=None
    if scriptname != "ramblerapp":


	# Depending on what platform we're on the scriptname could
	# either either foo or foo.exe, to add insult too injury the
	# scrip probably isn't installed in the bundle directory (the
	# directory containg the app.conf) file. So using setuptools
	# we'll first determine which egg this script came from, then
	# we'll use that to find the directory containing the app.conf

        # We may have more than one module that uses the same
        # appBundle, like appmanager and ramblercon, so consolt the
        # pkg_resources for that information. This only works if the script
        # wass installed as an egg 'console_script'

	for ep in pkg_resources.iter_entry_points('console_scripts',scriptname):
             
	    # Note, asking for the '' filename only works on unzipped
	    # packages. If I want to make ramblerapps out of eggs
	    # we'll need to redesign the "Bundle" concept. Heck we
	    # might be able to ditch Bundles in favor of eggs
	    # alltogether....

            # We may one or more scripts that 


	    appBundlePath = pkg_resources.resource_filename(ep.dist.project_name,'')

	    # Warning: There could be more than one script of the same
	    # name in setuptools database. Typically this means that
	    # two different projects installed the same console_script
	    # with the same name. Now the bad part is, who knows which
	    # script is actually installed.
	    break

        # if appBundlePath wasn't set, then the script name didn't
        # refer to a script installed by an egg. As a last ditch
        # effort and probably the most common case the scriptname
        # referes to a vanilla python package
        
        appBundlePath = appBundlePath or  pkg_resources.resource_filename(os.path.basename(scriptname),'')

        
    elif len(args) < 1:
        print >> sys.stderr, "Please specify the application directory."
        return 1
    else:
        appBundlePath = args[0]

        if len(args) > 1:
            controllerAddress = args[1]
            args = args[2:]
        else:
            args = args[1:]
                    
        

    # clear the options in sys.argv we didn't use and put the
    # positional ones back in. Right now we're donig this mostly for
    # ramblercon which is a commandline application. Might be nice to
    # have a way of getting positional arguments to a component that
    # doesn't involve munging the command line.

    #del sys.argv[1:]
    #sys.argv.extend(args)


    
    # build a list of option for the ConfigService that we're specified on the commandline

#    if options.options:
#        authoritativeOptions = {}
#        for option in options.options:
#            key, value = option.split("=",1)
#            authoritativeOptions[key] = value
#
#    else:
    authoritativeOptions = None



    # close any open file handles, have to do this before we load the
    # app cause who knows what files we may open next

    if not options.daemonize:
        # keep stderr, stdin and stdout open
        startfd = 3
    else:
        startfd = 3

    # wonder if closing files is neccesary now that we don't fork..
    import os
    try:
        maxfd = os.sysconf("SC_OPEN_MAX")
    except (AttributeError, ValueError):
        maxfd = 256       # default maximum

    for fd in range(startfd, maxfd):
        try:
            os.close(fd)
        except OSError:   # ERROR (ignore)
            pass

    app = Application(appBundlePath,
                      controllerAddress,
                      authoritativeOptions=authoritativeOptions)
  
    logService = app.lookup("LogService")
    level = logService.defaultLevel - (options.logLevel * 10)
    logService.setLevel(level)


    if not options.daemonize:
        logService.useStreamHandler(sys.stderr)
    else:
        # since we didn't pass anything in this should already be done for us

        logService.useSyslogHandler()
        pass


    try:
        app.load()
        
    except:
        app.log.exception("Exception encountred while loading as a subprocess")
        return 1

    try:
	RunLoop.currentRunLoop().run()
    except:
	app.log.exception("Unhandled exception encuntered in runLoop")
	return 255
    
    # if we didn't die with an exception, exit with a 0, no errors
    return 0
            
