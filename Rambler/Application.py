import pwd
import os
import signal
import socket
import sys
import traceback

from hashlib import md5

import pkg_resources

from Rambler import outlet, Bundle, Component
from Rambler.CompBinder import CompBinder
from Rambler.ciConfigService import ciConfigService, DictConfigSource
from Rambler.ErrorFactory import ErrorFactory
from Rambler.Services import ServiceHandler
from Rambler.ServiceRegistry import ServiceRegistry
from Rambler.SessionRegistry import SessionRegistry

from Rambler.LoggingExtensions  import LogService
from Rambler.RunLoop import RunLoop, Port


from Rambler.twistedlogging import  StdioOnnaStick
from Rambler.defer import Deferred


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


    
class Config(object):

    def __init__(self, extension_names):
        self.extensions = set([Extension('Rambler')])
        self.user  = None
        self.group = None
        
        for name in extension_names:
            self.extensions.add(Extension(name))

class Extension(object):
    def __init__(self, name):
        self.name = name
        self.options = {}
  
                


class Application(object):

    eventChannel = outlet('EventService')
    componentRegistry = outlet('ComponentRegistry')
    configService = outlet('ConfigService')
    txn = outlet('TransactionService')
    
    # TODO: Debug why binding a controller breaks the whole system
    # ... well I know why the it's because the SessionRegistery (which loads)
    # the controllers needs the Application object so it knows what extensions
    # to look for. Binding a controller then forces the SesionRegistery to not be loaded
    #Scheduler = outlet('Scheduler')

    log = outlet('LogService')




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

    

    __slots__ = ['name', 'config','appDir', 'pidPath', 'configFile', 'context',
                 'status', 'configError', 'digest',
                 'deferred', '_dh','scheduler',
                 'appBundle','mainRunLoop',
                 ]



    ## Instance Methods ##

    def __init__(self, appDir,  authoritativeOptions = None, **defaultComps):
        self.appBundle = Bundle(appDir)
        self.configFile = self.appBundle.pathForResource('app.conf')
        self.status = Application.STOPPED

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


    @property
    def run_loop(self):
      return self.mainRunLoop

    @property
    def queue(self):
      if not hasattr(self, 'scheduler'):
        self.scheduler = self.lookup('Scheduler')
      return self.scheduler.queue

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
        operation | defered: Operation to be scheduled on the run loop
        [callback]: Method to invoke after operation has finished. Note if 
                    callback is used you must explicitly call self.run_loop.stop()
                    in your unit test.
        [timeout]: Max time in seconds to wait before giving up on the operation.
      
      Discussion:
      Operations are queued in the default scheduler and then the run loop is started.
      It is an error to use this method if the RunLoop is arleady active.


      """
      
      # TODO: Remove this check when the Scheduler no longer useses deferred's
      if isinstance(operation, Deferred):
        operation.addCallbacks(self.quit_with_result, self.quit_with_error)
        self.wait(timeout)

        if hasattr(operation.result, 'raiseException'):
          operation.result.raiseException()
      else:
        operation.add_observer(self, 'is_finished', 0, self.quit)
        self.queue.add_operation(operation)
      
        self.wait(timeout)
        
      # Both deferred or an operation will return here
      return operation.result


    def wait(self, timeout=5):
      self.mainRunLoop.waitBeforeCalling(timeout, self.quit)
      self.mainRunLoop.run()

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
        
        extensions = set()
        ext_dir = self.appBundle.pathForResource('extensions')
        if os.path.isdir(ext_dir):
          extensions.update(os.listdir(ext_dir))
        
        
        self.config = Config(extensions)
    

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
            
        old=signal.signal(signal.SIGINT, self.sighandler )
        old=signal.signal(signal.SIGTERM, self.sighandler )
        #signal.signal(signal.SIGCHLD, self.onSigChild)
        signal.signal(signal.SIGHUP, self.onSigHUP)
        # todo: it wolud be nice if we restored theses signals when
        # the App is done.


        try:
            self.switch_user()
            self.switch_group()

            #self._dh = CompoundHandler()

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

            
            compReg.bind()
            

            #self.registerHandler(compReg.get("ServiceHandler"))
           
            
            # TODO: Get rid of descriptor all together
            #coreDescriptor = pkg_resources.resource_stream('Rambler', 'descriptor.xml')
            #self.__loadExtension(coreDescriptor)

            # Bind the core service to each other
            compReg.bind()
            self.eventChannel.registerEvent("Initializing", self, str)
            self.eventChannel.registerEvent("Shutdown", self, str)



            options = {}
            for extension in self.config.extensions:
                options[extension.name] = extension.options

            self.configService.addConfigSource(DictConfigSource(options))

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
        
    def isRunning(self):
        return self.status == Application.STARTED

    def lookup(self, component):
        return self.componentRegistry.lookup(component)


    def getStatus(self):
        return self.status

    # TODO: I think setStatus is defunc
    def setStatus(self, status, details=None):
        """Sends a life cycle notification to the controlling process, if any."""
        self.onStatusSent(None, status)


    def switch_user(self):

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

    def switch_group(self):
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
      pass
      #self.mainRunLoop.callFromThread(self.reattach)


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
        args = args[1:]
                    
        

    # clear the options in sys.argv we didn't use and put the
    # positional ones back in. Right now we're donig this mostly for
    # ramblercon which is a commandline application. Might be nice to
    # have a way of getting positional arguments to a component that
    # doesn't involve munging the command line.

    #del sys.argv[1:]
    #sys.argv.extend(args)


    
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
            
