"""

Structure of all messages are as follows. The first 8 byte indicates the
protocol version, message type, message . The rest of the message depends on the
protocol version.


For Protocol version =
+---+---------------------------------+
|0  | Protocol Version (always 1)     |
+---+---------------------------------+
|1  | Message Type 1 call, 2 response,|
|   |  3 error, 4 event               |
+---+---------------------------------+
|2-3| Message Id  Random int 0- 65535 |
+---+---------------------------------+
|4-7| Message Length long 0-4294967295|
+---+---------------------------------+
|8-x| Message Body pickled string     |
+---+---------------------------------+


LCP has 4 different message types which are indicated in the second byte of the message

1 call - Used to send a message where a response/acknowledgement/error is expected 
2 response - This is a response to a call
3 error - If an error was thrown during a call the exception is returned
4 event - Used to send a message where no response is expected



Bytes 2-3 are the message id. In the case of call message this will be
a unique generated number greater than 0 upto 65535. Clients can reuse
message ids as soon as they receive a response or an error. Events
ignore the message id since they do not expect a result. Typcially
this value is set to 0.

Bytes 4-7 are used to indicate the length of the remaining
message. Because this is 32bit unsigned int the remaining message body can
theoritaclly be up to 4294967296 bytes long.

Version 1 of our protocol, the message body is a pickled representation
of a 3 part tuple.

appname, topic, data

The interpretation of these three values are left to the delegate to
do something useful with. See the AppManager for an example of how we
use thes values.
"""



from Rambler.RunLoop import RunLoop, IPort
from Rambler import outlet, Interface, Attribute, implements
from Rambler.defer import Deferred, failure, succeed

import time, socket

import pickle, struct

class CommFailure(Exception):
    pass



class IMessageObserver(Interface):
    """You can register an object with the message bus by calling
    MessageBus.claimName(name, object). When you do this any object
    can route a message to you through the bus by calling
    MessageBus.sendMessage(objectOrRef, methodName, *args, **kw)
    
    """
    
    def onMessage(context, subject, *args, **kw):
        
        """Called by the MessageBus whenever a new message arives. The
        return value of this object will be passed back to the
        caller. The only requirement for the return value is it must
        be pickable. So generally you should stick with sending basic
        python types unless you know that the both parties on the bus
        have the same classes.
        """

    def onDisconnect(self, msgBus):
        """If the observer is registered with a slave bus, then it can
        receive a notification if the master is disconnected.
        
        Observers might use this to automatically reestablish the
        connection should it be lost.
        """



class MessageBus(object):
    """This is an implementation of our AppMessageBus. To use create
    instatiate this object with the address to either listen on or connect to.



    For severs call listen passing the backlog and an observer to handle status messages

    >>> class StatusObserver(object):
    ...    def onMessageNotification(self, appName, status, details):
    ...       print 'got', appName, status
    ...    def onMessageSent(self, appName, status, details):
    ...       print 'sent', appName, status
    ...    def onMessageError(self, error, appName, status, details):
    ...       print 'error %s while sending %s %s' % (error, appName, status)


    >>> observer = StatusObserver()
    
    >>> lcpServer = MessageBus("/tmp/lcptest.soc", observer)
    >>> lcpServer.listen(5)
    
    
    For clients instatiate with the same address and call
    sendStatus(appName, status, [details])

    >>> lcpClient = MessageBus("/tmp/lcptest.soc", observer)
    >>> lcpClient.sendStatus("myapp", "starting")

    Of course like all protocols you need to ensure the RunLoop is
    runnig in order to use it.

    >>> runLoop = RunLoop.currentRunLoop()
    >>> runLoop.runUntil(seconds=1)
    >>> runLoop.run()
    got myapp starting
    sent myapp starting

    Notice that in the same process we get the status notification
    before we're alerted that we sent the notification. That's because
    the protocol sends the notification, then waits until the server
    closes the connection notifying us that it got it a-ok


    Now that the runLoop is stopped and cleared out let's test the error handling by
    sending another status message with no server listening


    >>> lcpClient.sendStatus("myapp", "stopping")

    >>> runLoop.run()
    error port timed out while sending myapp stopping
    """
    implements(IMessageObserver)


    #MAX_CONTROLLER_DELAY = 5
    
    PICKLE_ERR        = 0
    COMM_FAILURE_ERR  = 1

    PROTO_VERSION     = 1
    HEADER_SIZE = 8
    
    MSG_TYPE_CALL     = 1
    MSG_TYPE_RESPONSE = 2
    MSG_TYPE_ERROR    = 3
    MSG_TYPE_EVENT    = 4

    MODE_UNKNOWN    = 0 # not connected/not listening
    MODE_LISTENING = 1 
    MODE_CONNECTED = 2 

    log = outlet("LogService")
    errorFactory = outlet("ErrorFactory")
    portFactory = outlet("PortFactory")

    def __init__(self):
        # used for generating uniq id's, it rolls back to 0 when it
        # reaches the size of an unsigned short which
        # 65535. MessageBus users sholud never look at this value
        # directly, instead they should call newMsgId() which does the
        # book keeping
        
        self.__callCount = 1
        
        # buffer the incoming data on a port by port basis until the
        # client closes the connection.
        self.mode = self.MODE_UNKNOWN
        self.listeningAddresses = {}

        # keep track of the ports that are connecting, the key is the
        # port, the value is the defered object
        
        self.pendingConnects = {}

        self.waitingForHeader = {}
        self.pendingMessage   = {}


        self.namesToObjects = {}
        self.objectsToNames = {}


        # key is a port, value is a dictionary message id to defferred objects.
        self.outstandingCalls = {}

        # two item list we setup when we're waiting to close, the
        # first value is an int rerpresenting the number of ports
        # we're waiting to hear onClose from and the second item is
        # the deferred object whose callbacks will be run when all the
        # ports have checked back in.
        
        self.pendingCloses = None


    def claimName(self, name, object):
        """Register an object to receive events directed at the given
        name on both sides of the message bus."""
        
        self.__localClaimName(name,object)
        master = self.namesToObjects['']

        if self.mode == self.MODE_CONNECTED:
            d = self.sendMessage(master, 'claimName', name)
        else:
            # Didn't go out over the wire so fake the callback in the next pass
            d=succeed(name)
        return d

    def createCallMessage(self, msgId, remoteRef, subject, args, kw):

        """Translates the body to a header and pickled data sutiable
        for sending out over the wire. This method is usually not
        called directly."""

        msgType = self.MSG_TYPE_CALL
        body = pickle.dumps((remoteRef, subject, args, kw))

        msgLength = len(body)
        header = struct.pack("BBHI", self.PROTO_VERSION, msgType, msgId, msgLength)

        return header + body

    def createResponseMessage(self, msgId, body):
        msgType = self.MSG_TYPE_RESPONSE
        body = pickle.dumps(body)
        msgLength = len(body)
        header = struct.pack("BBHI", self.PROTO_VERSION, msgType, msgId, msgLength)
        return header + body

    def createErrorMessage(self, msgId, err):
        msgType = self.MSG_TYPE_ERROR
        body = pickle.dumps(err)
        msgLength = len(body)
        header = struct.pack("BBHI", self.PROTO_VERSION, msgType, msgId, msgLength)

        return header + body

    def createEventMessage(self, remoteRef, subject, args, kw):
        msgType = self.MSG_TYPE_EVENT
        msgId = 0 # events don't use the msgId field, since they don't expect a response
        body = pickle.dumps((remoteRef, subject, args, kw))
        msgLength = len(body)
        header = struct.pack("BBHI", self.PROTO_VERSION, msgType, msgId, msgLength)
        return header + body


    def __localClaimName(self, name, object):
        """Claims the name on the local message bus, used internally
        don't call directly."""
        
        if not (IMessageObserver.providedBy(object) or
                IPort.providedBy(object)):
            raise TypeError("%s must support either the IMessageObserver or IPort interface" % object)

        if name not in self.namesToObjects:
            self.namesToObjects[name] = object
            self.objectsToNames[object] = name
        elif self.namesToObjects[name] == object:
            # ignore dupe calls
            return
        else:
            raise RuntimeError("%s is already claimed by %s" % (name, self.namesToObjects))
        

    def removeName(self, object):
        name = self.objectsToNames.pop(object)
        del self.namesToObjects[name]


    def listen(self, address):
        if address in self.listeningAddresses:
            return

        if self.mode == self.MODE_CONNECTED:
            raise RuntimeError("Can not listen for incoming connections if we're already connected to a foreign message bus.")

        self.mode = self.MODE_LISTENING
        port = Port(address, self)
        self.listeningAddresses[address] = port
        port.listen(5)
        self.__localClaimName('', self)


    def connect(self, address, timeout=None, delay=.2):
        """Connects to the remote messag bus and attempts to claim the given name.
        The deferred will be passed the connected address as the first object.
        

        An optional timeout variable can be passed in wich instructs
        the message bus to keep trying to connect if it gets a
        failure. In the event that it does get a failure it will
        schedule itself with the message bus based on the delay argument (wihch defaults to .1 seconds)

        If timeout is set to 0 it will attempt to connect indefinatly,
        otherwise it will attempt to connect every x seconds
        (specified by delay) until the timeout has been reached. At
        which point it will signal an error through the defered.

        """

        if self.mode == self.MODE_CONNECTED:
            raise RuntimeError("Can only be connected to one foreign bus at a time.")
        elif self.mode == self.MODE_LISTENING:
            
            # this is an active bus, we're already listening for
            # incoming connections, therefore we don't allow you (yet)
            # to connect to any addresses other than ones we're
            # currently listening on

            if address not in self.listeningAddresses:
                raise RuntimeError("Once the message bus is listening to connections you can only connect to local addresses.")
            else:
                # local connection
                return succeed(address)
        else:
            connectDeferred = Deferred()
          
            try:
		int(timeout)

                # timeout is not none, so set us up to retry on
                # failures. We play the three card monety with the our
                # defereds so that the reconnect method can handle
                # timeouts and what not properly.

                returnDeferred = Deferred()
                

                connectDeferred.addBoth(self.reconnect, returnDeferred, time.time(), address, timeout, delay)
            except TypeError:
                # no time out set, so the first failure will call our
                # deferred imediatly
                returnDeferred = connectDeferred

                pass
            
            port = Port(address, self)


            self.pendingConnects[port] = connectDeferred

            # start reading message from the connection
            self.mode = self.MODE_CONNECTED
            # we'll route all messages to objects we don't know about to
            # message bus we connected to

            self.__localClaimName('', port)

            
            # we need to give the code calling connect() a chance to wire
            # up callbacks and errbacks to this deferred, so we schedule
            # the actual connect call to be done in the next pass of the
            # RunLoop. Wonder if the port should do this with connects by
            # default

            RunLoop.currentRunLoop().callFromThread(port.connect)
            return returnDeferred
        
    def reconnect(self, result,  deferred, startTime, address,  timeout, delay):
        
        if result == address:
            # it's an address, we've connected so run the deferred and bail
            deferred.callback(address)
            return result

        # else it's a failure we need to recconect, but only if we failed on a socket error

        result.trap(socket.error) # make sure we failed because of a socket error

        # if at first you don't succeed, try try again. Until we run out of patience of course

        if timeout > 0 and (time.time() - startTime > timeout):
            # that's it we give up so notify the original deferred
            deferred.errback(result)
        else:
            # result should be the address
            RunLoop.currentRunLoop().waitBeforeCalling(delay, self._reconnect,
                                                       deferred, startTime,
                                                       address, timeout, delay)
            

    def _reconnect(self, deferred, startTime, address, timeout, delay):
        # called by the RunLoop after the appropriate delay to preform
        # the actual recconect and wire up our results to our defered.
        d = self.connect(address)
        d.addBoth(self.reconnect, deferred, startTime, address, timeout, delay)
            
        


    def close(self):
        """Stop accepting connections if we're listening for them, or
        shut down the remote side if we're connected."""

        d = Deferred()

        portCount = 0
        for obj in self.objectsToNames.keys() + self.listeningAddresses.values():
            if IPort.providedBy(obj):
                # close the port
                obj.close()
                portCount += 1

        self.pendingCloses = [portCount, d]
        return d

        # now we wait for the port's to call our onClose() call back
        # before removing them from our naming

    def newMsgId(self):
        """Returns the next message id."""
        self.__callCount += 1
        if self.__callCount == 65535:
            self.__callCount = 1
        return self.__callCount

    def sendMessage(self, recipient, subject, *args, **kw):

        remoteRef = ''
        if isinstance(recipient , basestring):
            # it's a reference to an objcet or port that has claimed a name
            remoteRef = recipient

            reg = self.namesToObjects

            # get the object for the reference or get the object that
            # handles unknow refs
            recipient = reg.get(recipient, reg[''])

        msgId = self.newMsgId()

        if IPort.providedBy(recipient):
            #notification = (appName, status, details)
            d = Deferred()
            msg = self.createCallMessage(msgId, remoteRef, subject, args, kw)
            recipient.write(msg)
            self.outstandingCalls.setdefault(recipient, {})
            self.outstandingCalls[recipient][msgId] = d
            return d
        else:
            # local call
            context = {'TARGET': remoteRef} 

            results = recipient.onMessage(context, subject, *args, **kw)
                
            return succeed(results)
            #RunLoop.currentRunLoop().callFromThread(self.doLocalCall, recipient, {}, subject, args, kw)
            


    def sendEvent(self, recipient, subject, *args, **kw):
        """Sends a async message to an object. If the object returns a
        value it's not returned to the caller."""
        remoteRef = ''
        # this code is identicaly to sendMessage, might want to refactor
        if isinstance(recipient, basestring):
            remoteRef = recipient
            reg = self.namesToObjects
            recipient = reg.get(recipient, reg[''])

        if IPort.providedBy(recipient):
            msg = self.createEventMessage(remoteRef, subject, args, kw)
            # write and forget it
            recipient.write(msg)
        else:
            # local call, events don't return resones
            recipient.onMessage(context, subject, *args, **kw)
        

    def doLocalCall(self, recipient,  context, subject, args, kw, deferred=None):
        # Preforms the call and notify our callbacks that it's complete
        # todo deprecate this method
        results = recipient.onMessage(context, subject, *args, **kw)
        if deferred:
            deferred.callback(results)


        

    def sendResponse(self, results, port, msgId, subject):
        # note sendResponse signatur is different from the other send
        # calls to accomidate the deferred callback
        
        assert IPort.providedBy(port)
        name = self.objectsToNames.get(port, '')
        msg = self.createResponseMessage(msgId, (name, subject, results))
        port.write(msg)

        


    def sendError(self, port,  msgId, err):
        assert IPort.providedBy(port)
        msg = self.createErrorMessage(msgId, err)
        port.write(msg)
        #print msgId, err

    def readHeader(self, port):
        self.waitingForHeader[port] = ''
        port.read(self.HEADER_SIZE)
    
    def onAccept(self, port):
        # someone connected first setup an area that will received the
        # incoming data until the port is closed
        #self.log.debug("Accepting %s" % port)

        self.readHeader(port)
        

    def onConnect(self, port):

        #self.log.debug("Connected %s" % port)

        # create a special message and send it out directly on the port in order to exchange names with the server.
        #msg = self.createCallMessage(('', 'register', self.name))

        deferred = self.pendingConnects.pop(port)

        deferred.callback(port.address)
        
        self.readHeader(port)


    def onClaimNameSuccess(self, remoteName, port):
        """Called when we succesful claimed an name on the remote bus."""
        # Set our local name, now all messages we send will no longer be anonymous
        self._name = self.requestedName
        # The remote bus responds with it's own name, so we want to
        # associate the port with it on the local bus
        self.claimName(remoteName, port)

        # Return the remoteName in case any one waiting for connect
        # want's to know what it was
        return remoteName
        

    def onClaimNameErr(self,failure, port):
        """Called when there was an error claiming the remote name."""
        self.log("Error claming name.")
        failure.printTraceback()

        port.close()
        return failure


    def onMessage(self, context, subject, *args, **kw):
        # The message bus listens on the bus for messages directed at
        # '' with a subject of claimName. It also is the default
        # handler for sendMessage when the object is not in the local
        # cache of names. So sometimes we accidently get messages sent
        # to us to an object we don't know about.



        if subject != "claimName":
            raise RuntimeError("Message either does not know about an object "
                               "named %s  or it does not handle messages with "
                               "the subject %s\n Known Names: %s" %
                               (context.get('TARGET','UNKNOWN?!?!'), subject, self.namesToObjects))
            
        else:
            remoteName = args[0]

        port = context['REMOTE_PORT']
        assert IPort.providedBy(port)


        self.__localClaimName(remoteName, port)
        
        return remoteName
        

    def onRead(self, port, data):

        if port in self.waitingForHeader:
            data = self.waitingForHeader[port] + data
            if len(data) != self.HEADER_SIZE:
                self.waitingForHeader[port] = data

                # We had a short read, returning true tells the port
                # to wait for it before processing anythore requests
                # to the port
                
                return True
            else:
                del self.waitingForHeader[port]
                # unpack the header
                protoVersion, msgType, msgId, msgLength = struct.unpack("BBHI", data)
                assert protoVersion == self.PROTO_VERSION, "Invalid protocol version expected 1 got %s" % protoVersion
                assert 1<= msgType <= 4, "Unknown message type %s" % msgType

                buffer = []
                self.pendingMessage[port] = [protoVersion, msgType, msgId, msgLength, buffer, 0]
                port.read(msgLength)
        elif port in self.pendingMessage:
            protoVersion, msgType, msgId, msgLength, buffer, bytesRead = self.pendingMessage[port]
            bytesRead += len(data)
            buffer.append(data)
            if bytesRead < msgLength:
                self.pendingMessage[port][-1] = bytesRead
                # more content coming keep reading
                return True
            else:
                del self.pendingMessage[port]
                data = "".join(buffer)
                try:
                    msgTuple = pickle.loads(data)
                except Exception, e:
                    userInfo = {self.errorFactory.REASON: "Error unpickling message",
                                self.errorFactory.DESCRIPTION: "Error was encountered while unpickling the message. %s" % e,
                                self.errorFactory.SUGGESTION: ("Bad packet perhaps?!?!")}

                    err = self.errorFactory.newError(self.PICKLE_ERR, userInfo)
                    self.sendError(msgId, err)

                context = {'REMOTE_PORT':port,
                           'MSG_ID': msgId,
                           'MSG_LENGTH': msgLength,
                           } 


                if msgType in (self.MSG_TYPE_CALL, self.MSG_TYPE_EVENT):
                    #todo: make this self.log.debug
                    #print "got a call to:%s  from: %s (%s, %s, %s)" % (self.name,name, msgId, status, details)
                    name,subject,args,kw = msgTuple
                    context['TARGET'] = name

                                        
                    try:
                        recipient = self.namesToObjects[name]
                        assert IMessageObserver.providedBy(recipient)
                        
                        # todo: the object could be either a port or
                        # message observer, consider the implications
                        # of forwarding call messages if the object is
                        # a port
                        
                        results = recipient.onMessage(context, subject, *args, **kw)
                        if msgType == self.MSG_TYPE_CALL:
                            if isinstance(results, Deferred):
                                # messgae observer wants us to delay the results
                                results.addCallback(self.sendResponse, port, msgId, subject)
                            else:
                                self.sendResponse( results, port, msgId, subject)

                    except Exception, e:
                        if not self.errorFactory.isError(e):
                            e = self.errorFactory.unexpectedError()
                        self.sendError(port, msgId, e)

                elif msgType == 2:
                    # todo: self.log.debug
                    #print "got response to:%s from:%s (%s, %s, %s)" % (self.name, name, msgId, status, details)
                    name,subject,results = msgTuple

                    deferred = self.outstandingCalls[port].pop(msgId)
                    if len(self.outstandingCalls[port]) == 0:
                        del self.outstandingCalls[port]
                        
                    RunLoop.currentRunLoop().callFromThread(deferred.callback, results)

                elif msgType == 3:
                    # this might not be the right unpack of the error
                    failure = msgTuple

                    deferred = self.outstandingCalls[port].pop(msgId)

                    if len(self.outstandingCalls[port]) == 0:
                        del self.outstandingCalls[port]
                        
                    RunLoop.currentRunLoop().callFromThread(deferred.errback, failure)

                else:
                    
                    raise RuntimeError("Uknown message type received %s" % msgType)


                # we proccesed one message wait for the next one
                self.readHeader(port)


    def onWrite(self, port, bytesWritten):
        pass
            

    def onError(self, port, error):
        # got an error on a port, notify any outstanding call that they'll never complete.

        self.log.debug("Error %s on %s", error, port) 
        if self.mode == self.MODE_CONNECTED:
            # ensures that we can call connect again on the message
            # bus if there was an error
            
            self.mode = self.MODE_UNKNOWN

        # remove any names that the port may be associated with
        self.removeName(port)



        if self.outstandingCalls.has_key(port):

            for msgId in self.outstandingCalls[port].keys():
                self.outstandingCalls[port].pop(msgId).errback(error)
            del self.outstandingCalls[port]

        if port in self.pendingConnects:
            deferred = self.pendingConnects.pop(port)
            deferred.errback(error)

    
    def onTimeOut(self, port):
        #self.log.debug("%s timed out", port)
        # this probably should be an error code or exception
        self.onError(port, "port timed out")
        

    def onClose(self, port):
        try:
            self.removeName(port)
        except KeyError:
            
            # joy looks like this port got closed before negotiating
            # names, see if there's any outstanding calls on that port
            # let their deffered know that the port is closed.

            outstandingCalls = self.outstandingCalls.pop(port, {})
            for msgId, deferred in  outstandingCalls.items():
                deferred.errback(CommFailure)

        # if pendingCloses is None then the other side closed it's
        # connectino to us. So simply removing it's ref should be good
        # enough.
        
        if self.pendingCloses is not None:
            portCount, deferred = self.pendingCloses
            portCount -= 1
            if portCount == 0:
                # No more outstanding call our defered in the next pass of the runLoop
                RunLoop.currentRunLoop().callFromThread(deferred.callback, None)
            else:
                self.pendingCloses = portCount, deferred

        if self.mode == self.MODE_CONNECTED:
            # the master went away, notify all our local observes the
            # bus is down and remove them
            self.mode = self.MODE_UNKNOWN
            for obj in self.objectsToNames.keys():
                # there shouldn't be any ports left, since slave buses
                # can only be connected to one remote
                assert IMessageObserver.providedBy(obj)
                # just cause the object says it's an IMessageObserver doesn't mean it wants to hear about disconnects
                if  hasattr(obj, 'onDisconnect'):
                    obj.onDisconnect(self)
                    
                self.removeName(obj)

            
        elif self.mode == self.MODE_LISTENING:
            # We've closed one of the listenining sockets so remove it
            # from our listening addresse. THere shouldn't be that
            # many so we iterate through all them to find which one
            # died

            for address, lport in self.listeningAddresses.items():
                if lport == port:
                    del self.listeningAddresses[address]

                if len(self.listeningAddresses) == 0:
                    self.mode = self.MODE_UNKNOWN
            


if __name__ == "__main__":
    import doctest, sys
    doctest.debug(__name__,  "MessageBus")
    
