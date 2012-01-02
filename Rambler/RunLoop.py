"""The vast majority of code is this file is either based on or
downright copied from the Twisted library (www.twistedmatrix.com)
which has been provided under the following terms.


Copyright (c) 2001-2006
Allen Short
Andrew Bennetts
Apple Computer, Inc.
Benjamin Bruheim
Bob Ippolito
Canonical Limited
Christopher Armstrong
David Reid
Donovan Preston
Eric Mangold
Itamar Shtull-Trauring
James Knight
Jason A. Mobarak
Jonathan Lange
Jonathan D. Simms
Jp Calderone
Jurgen Hermann
Kevin Turner
Mary Gardiner
Matthew Lefkowitz
Massachusetts Institute of Technology
Moshe Zadka
Paul Swartz
Pavel Pergamenshchik
Ralph Meijer
Sean Riley
Travis B. Hartwell

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import os, select, thread, threading, time, heapq, fcntl, datetime
import socket, errno

from dateutil.relativedelta import relativedelta
from dateutil import rrule

from Rambler import outlet
from Rambler.ThreadStorageService import ThreadStorageService
from zope.interface import Interface, Attribute, implements


if not hasattr(socket, 'SHUT_RD'):
   # prior to python 2.4 we don't have these handy constants
   # defined, lets add those as well
   socket.SHUT_RD=0
   socket.SHUT_WR=1
   socket.SHUT_RDWR = 2
   
import sys
class Stream(object):
   def __init__(self, fd, observer=None):
         # list of ints, each items represents one outstanding call to Stream.read
         self.readRequests = []
         # list of raw data to send to the file
         self.writeBuffer = []
         
         #sys.stderr.write('\033[0;32m')
         #sys.stderr.write('New stream with %s\n' % fd)
         #if type(fd) != int:
         #   sys.stderr.write("%s\n" % fd.fileno())
         #sys.stderr.write('\033[m')
         
         if type(fd) == int:
               self.fd = fd
         else:
               self.fd = os.dup(fd.fileno())

         flags = fcntl.fcntl(self.fd,fcntl.F_GETFL)
         fcntl.fcntl(self.fd, fcntl.F_SETFL, flags | os.O_NDELAY)

         self.observer = observer
         
   # TODO: For some reason __del__ is firing before the object is being deleted
   # which in turn is closing the file to soon. It may be do to a deepcopy issue.
   # idea. Log out the ID of this object
   # def __del__(self):
   #   self.close
     
   def __repr__(self):
     return "<stream %s>" % self.fd
     
   def fileno(self):
         return self.fd

   def read(self, bytes):
         self.readRequests.append(bytes)
         RunLoop.currentRunLoop().addReader(self)
         
   def read_to_end(self):
     """Keeps reading and notifying observer until the end of stream has
     been reached.
     
     Discussion:
     This function is synchronous and will block the current thread until
     the end of file is reached.
     """
     
     while 1:
       data = os.read(self.fd, 1024)
       if data:
         self.observer.onRead(self, data)
       else:
         break
         
   def close(self):
     """Removes the stream from the currentRunLoop and closes the file descriptor"""
     
     #sys.stderr.write('\033[0;32m')
     #sys.stderr.write('%s %s %s closing %s\n' % (os.getpid(),  threading.currentThread(),sys._getframe(1).f_code.co_name, self.fd))
     #sys.stderr.write('\033[m')
     
     
     if self.fd is not None:
       if hasattr(self.observer, 'onClose'):
         self.observer.onClose(self)
         # There will be no more data
       del self.readRequests[:]
       
       self.removeFromRunLoop(RunLoop.currentRunLoop())
       os.close(self.fd)


       self.fd=None
     
   def canRead(self, stream):

         requestcount = len(self.readRequests)
         while requestcount:
               requestcount -= 1
               # read until we get as much data as we've been
               # waiting for, or the socket would block.
               bytes2Read = self.readRequests[0]

               try:
                     data = os.read(self.fd,bytes2Read)
                     if data == '':
                       if hasattr(self.observer,'end_of_data_for'):
                         self.observer.end_of_data_for(self)
                       return
                         
                         
               except OSError, e:
                     if e.errno == errno.EAGAIN:
                           RunLoop.currentRunLoop().addReader(self)
                     else:
                           raise

               # notify our observer that data's been returned
               waitIfShort = self.observer.onRead(self, data)
               bytesRead =  len(data)
               if bytesRead < bytes2Read and waitIfShort:
                     self.readRequests[0] -= bytesRead
               else:
                     # we're done with this request
                     del self.readRequests[0]


   def write(self, data):
      self.writeBuffer.append(data)
      RunLoop.currentRunLoop().addWriter(self)


   def canWrite(self, data):
      bytessent = 0
      while self.writeBuffer:
            try:
                  data = self.writeBuffer[0] 
                  # While we have data in our out goin buffer try to send it
                  sent = os.write(self.fd, data)

                  if len(data) == sent:
                        # we sent all the data in one shot
                        del self.writeBuffer[0]
                  else:
                        self.writeBuffer[0] = data[sent:]
                  bytessent += sent
            except OSError, e:
                  if e.errno == errno.EAGAIN:

                        # other end of the socket is full, so
                        # ask the runLoop when we can send more
                        # data

                        RunLoop.currentRunLoop().addWriter(self)
                        break
                  else:
                        raise

      # notify our observer of how much we wrote in this pass
      if bytessent > 0:
            self.observer.onWrite(self, bytessent)
            
   def onError(self, error):
     self.observer.onError(self, error)
            
   def removeFromRunLoop(self, runLoop):
     try:
       runLoop.removeReader(self)
     except KeyError:
       pass
       
     try:
       runLoop.removeWriter(self)
     except KeyError:
       pass

   def delegate(self):
     return self.observer
     
   def set_delegate(self, delegate):
     self.observer = delegate
   delegate = property(delegate, set_delegate)
    


class IPort(Interface):
   """This interface represents a port object that preforms async read and writes."""
   address = Attribute("The ip address and port or path to a unix socket.")
   connected = Attribute("True if the port is connected to another port.")
   
   
   def write(data):
      
      """Queues the data for write to the port. When the data is
      actually written Port's obesrver.onWrite() is invoked notifying
      the observer how much data was actually written.

      Data is actually written to the underlying transport in same
      order that it was queued and it is ok to write to this port
      before the connection has actually been established.

      """

   def read(bytes2Read):
      """Queues a read request for up to but no more than the given
      number of bytes on the port. It will be preformed at some other
      time. When the data is actually read the port's
      observer.onRead() method will be invoked. Note that not all the
      data requested may have been read and returned to the
      obserever's onRead() method. This is called a short read. It's
      the observer's responsability to determine whether the Port
      should continue waiting for more data for the request or not. If
      the observer wants to continue wating for data from the short
      read it will return True.
            
      """
   



class Port(object):
   implements(IPort)

   """Port objects utilize the RunLoop to make working with sockets easier.

   Port objects are used by both client and servers. You start
   things off by instantiating a port with the address to either
   connect to or listen on. The current implementation expects
   either a single string which is the path to a unix socket or a
   tuple containg the ip address and port.

   >>> class PortObserver(object):
   ...   def onConnect(self, port):
   ...     print "Connection made"
   ...   def onAccept(self, port):
   ...     print "Accepted new connection"
   ...   def onRead(self, port, data):
   ...     print "I just read "  + data
   ...   def onWrite(self, port, bytesWritten):
   ...     print "I just wrote %s" % bytes
   ...   def onClose(self, port):
   ...     print "Connection closed"

   >>> observer = PortObserver()
   >>> serverPort = Port("/tmp/mysocket", observer)

   To listen for incoming connections you call
   Port.listen(backlog,[runLoop]) with the ammount of connections
   that can be backloged. 

   >>> serverPort.listen(5)

   This will automatically add the Port to the RunLoop for
   monitoring. So to actually start accepting connections we need
   to start the RunLoop. But before we do that's let's create a
   port that will connect to this one.

   >>> clientPort = Port("/tmp/mysocket", observer)

   Notice the arguments are identical to our server port, the only
   thing that's different is that we call connect() rather than
   listen.

   >>> clientPort.connect()
   >>> runLoop = RunLoop.currentRunLoop()


   Just incase something is messed up in this test, stop the runLoop after 1 second.
   >>> def stop():
   ...  RunLoop.currentRunLoop().stop()

   >>> timer = runLoop.addTimer(DelayedCall(1, False, stop))
   >>> runLoop.run()
   Connection made
   Accepted new connection

   Note I don't think we can guarntee the orde that the above messages are printed out


   Rather than starting a socket directly you instantiate a Port

   """
   log = outlet('LogService')
   
   # A note I saw in twisted implies that you get better
   # preformance if your listening socket wakes up from select you
   # accept multiple connections or until you receive
   # EWOULDBLOCK. They make at most 40 attempts so we'll try the
   # same

   NUM_ACCEPT_AT_ONCE = 40

   SHUT_RD=0
   SHUT_WR=1
   SHUT_RDWR = 2

   # Todo, replace the init method with a class methods something like createUnixSocket, createInetSocket...

   def __init__(self, address, delegate=None):

         self.address = address
         self.delegate = delegate
         self._socket = None
         self.writebuffer = []
         self.readrequests = []
         self.timer = None

         # We should probably colapse all these variables into a
         # single state variable

         self.listening = None
         self.connected = False
         self.closing = False # set to true by close() when we want this port to, well close duh...
         self.userInfo = {}

   def __repr__(self):
         if self.listening:
               state = "listening"
         else:
               state = "connecting"
         return "<%s %s(%s) object at %s delegate=%s >" % (self.__class__.__name__, self.address, state, id(self),self.delegate)

   def __del__(self):
         if self._socket:
           self._socket.close()
           self._socket = None

   def fileno(self):
         if self._socket is None:
               raise RuntimeError("You must call connect or listen on this Port"
                                  "before it can be scheduled in a RunLoop")
         return self._socket.fileno()


   def canRead(self, stream):

         if self.listening:
               for x in range(self.NUM_ACCEPT_AT_ONCE):
                     try:
                           s, addr = self._socket.accept()
                           # with the new socket in hand create a new
                           # port to handle the communication.

                           # use __class__ incase I decide to change the class Name
                           port = self.__class__(addr, self.delegate)
                           port.connectionAccepted(s) 
                           port.scheduleInRunLoop(self.runLoop)
                     except socket.error, e:
                           if e[0] == errno.EWOULDBLOCK:
                                 break
                           else:
                                 raise

               # wait for more connections
               RunLoop.currentRunLoop().addReader(self)
         elif not self.connected:
               assert False, "How did we get here?"
               self.connected = True
         else:
               # if any readrequests that come in, should be read
               # in the next iteration

               requestcount = len(self.readrequests)
               while requestcount:
                     requestcount -= 1
                     # read until we get as much data as we've been
                     # waiting for, or the socket would block.
                     bytes2Read = self.readrequests[0]

                     try:
                           data = self._socket.recv(bytes2Read)
                           if data == '':
                              # Read connection was closed
                              self._reset()
                              self.delegate.onClose(self)
                              break

                     except socket.error, e:
                           if e[0] == errno.EWOULDBLOCK:
                                 break
                           else:
                                 # if we receive any other socket
                                 # error we close the connection
                                 # and raise and notify our delegate

                                 self._reset()
                                 self.delegate.onError(self, e)
                                 return


                     # notify our observer that data's been returned
                     waitIfShort = self.delegate.onRead(self, data)
                     bytesRead =  len(data)
                     if bytesRead < bytes2Read and waitIfShort:
                           self.readrequests[0] -= bytesRead
                     else:
                           # we're done with this request
                           del self.readrequests[0]

   def canWrite(self, stream):
      if not self.connected:
            self.connected = True
            self.delegate.onConnect(self)

      bytessent = 0

      # keep track of how much data was in the buffer at the time we
      # start writing, that way if more is added during the callbacks
      # it will be delayed to the next pass in the runloop

      bufLen = len(self.writebuffer)

      for x in range(bufLen):
            try:
                  data = self.writebuffer[0] 
                  # While we have data in our outgoing buffer try to send it
                  try:
                    sent = self._socket.send(data)
                  except Exception, e:
                    if  isinstance(e, socket.error):
                      # Q: why is there a blanket ecxception here?
                      raise

                    self.log.exception('Error writing %s to socket', data)
                    raise
                  #print 'w>>>', data[:sent],
                  if len(data) == sent:
                        # if we sent all the data in one shot
                        del self.writebuffer[0]
                  else:
                        self.writebuffer[0] = data[sent:]
                  bytessent += sent
            except socket.error,e:
                  if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):

                        # other end of the socket is full, so
                        # ask the runLoop when we can send more
                        # data

                        break
                  else:
                        # if we receive any other socket
                        # error we close the connection
                        # and raise and notify our delegate

                        self._reset()
                        self.delegate.onError(self, e)
                        return

      # notify our observer of how much we wrote in this pass
      if bytessent > 0:
            self.delegate.onWrite(self, bytessent)

      if self.writebuffer:
         # if we still have data reschedule our selves
         RunLoop.currentRunLoop().addWriter(self)
      elif self.closing:
         self._reset()
         self.delegate.onClose(self)

   def _reset(self):

      # called by close or when an error is encountered to reset the
      # port for future use
      
      # the port shouldn't be in the runLoop at this point but just in
      # case

      del self.writebuffer[:]
      del self.readrequests[:]

      self.removeFromRunLoop(RunLoop.currentRunLoop())
      self.connected = False
      self.listening = False
      self.setTimeOut(None)

      self._socket.close()
      self._socket = None


   def listen(self, backlog, runLoop = None):
         """Listen for incoming connections on this port.

           backlog - the maximum number of queued connectinos

           runLoop - the runLoop that will monitor this port for
                     incomming connections. Defaults to the
                     currentRunLoop if none is specified.  
         """

         if type(self.address) == tuple:
            serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM )
            socketPath = None
         else:
               socketPath = self.address
               serversocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM )

               if os.path.exists(socketPath):
                     # possable stale socket let's see if any one is listning
                     err = serversocket.connect_ex(socketPath)
                     if err == errno.ECONNREFUSED:
                           os.unlink(socketPath)
                     else:
                           serversocket._reset()
                           raise RuntimeError("Socket path %s is in use" % socketPath )

         
         serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
         serversocket.bind(self.address)

         if socketPath: # ensure the world can read/write this socket
            os.chmod(socketPath, 666)

         serversocket.listen(backlog)

         serversocket.setblocking(0)

         self._socket = serversocket
         self.listening = True
         self.connected = True

         if runLoop is None:
               runLoop = RunLoop.currentRunLoop()

         runLoop.addReader(self)
         self.runLoop = runLoop

   def connect(self):
         # todo: this looks redundant to listen...
         if type(self.address) == tuple:
            # listen on an internet socket
            clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM )
            clientsocket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY,1)
         else:
            clientsocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM )

         clientsocket.setblocking(0)
         self._socket = clientsocket

         # sigh, looks like how nonblocking connects are handled
         # differently on unix sockets than they are on inet sockets,
         # Inet sockets return EINPROGRESS

         e = self._socket.connect_ex(self.address)
         if e not in (0, errno.EINPROGRESS):
            self._reset()
            self.delegate.onError(self, socket.error(e, os.strerror(e)))
         else:
            RunLoop.currentRunLoop().addWriter(self)


   def close(self):
      # As soon as the write buffer is empty we'll tear down the
      # socket and notify our observer that we're closed.
      runLoop = RunLoop.currentRunLoop()
      self.closing = True
      if self.listening:
         # note, I added this code here because it was ok to call
         # select() on a listening socket on Linux, however when you
         # do this on MacOSX it won't return from the select loop.., so now my codes a bit hacky..

        self._reset()
        # wonder if we should have connect and closing methods that are specific to listening sockets
        runLoop.callFromThread(self.delegate.onClose,self)
      else:
         # if we're a normal port, schedule us in the runLoop just in
         # case we have any out going data

         runLoop.addWriter(self)
      
   def shutdown(self, how):
         self._socket.shutdown(how)
         #self.read(1)

   def setTimeOut(self, seconds):

         """Calling this function will cause the RunLoop to call
         timeout() on this port after the specified number of
         seconds are called.

         Protocols can implement onTimeOut(port) to determine if
         the other end is taking to long to preform an
         opperation. It's the protocols responsability to
         periodically call setTimeOut() if it determines the other 
         end should be given more time.

         Calling setTimeOut(None) will clear the timeout all together.

         If the socket is closed, the timer will automatically be removed.

         """


         if seconds is None:
               if self.timer:
                     # cancel the previous timer
                     self.timer.cancel()
               return

         if self.timer:
               self.timer.reset(seconds)
         else:
               self.timer=DelayedCall(seconds, False, self.onTimeOut)
               RunLoop.currentRunLoop().addTimer(self.timer)


   def onTimeOut(self):
         # socket took to long, notify our observer, then tear the
         # socket down.
         self.delegate.onTimeOut(self)
         self.close()


   def connectionAccepted(self, socket):
         self._socket = socket
         self.connected = True
         self.delegate.onAccept(self)


   def read(self, bytes):
         """Queues a equests for x number of bytes to be read from the port
         when data becomes available. When data is available the
         port object will read up to the ammount of data requested
         then notify it's observer by calling onRead(port,
         bytesread). It's important to note that when you read from
         a socket it might return less data than you
         requested. This may or may not be perfectaly acceptable
         depending on what protocol you are implmenting. So in the
         event of a short read, the port will consult the result of
         calling it's observer.onRead().. if it returns True the
         port will wait for more data before attepmitng to service
         the next read attempt.

         For examlpe, calling read twice in a row queues up two read requests
         Port.read(8000)
         Port.read(8000)

         The port will read as much as it can (up to 8000 bytes)
         for each read request. If the read is short say only 4000
         bytes was available at the time of the read, then the
         observer can return true from onRead() to signafy that the
         port should continue wating for more data before servicing
         the second request.

         In practice I'd assume this isn't that useful...


         """

         self.readrequests.append(bytes)
         RunLoop.currentRunLoop().addReader(self)


   def write(self, data):
      """Writes the data to the port and notifies our observer when we're
      done (or partially done). If we can't get all the data to
      the other port we reschedule ourselves back in the RunLoop
      until we can.

      Its ok to write data to this port before it's
      connected. When the connection is accepted the buffer
      should be written to.
      """

      # Add the data to the buffer and ensure that we're in the runLoop
      self.writebuffer.append(data)
      if self.connected:
         RunLoop.currentRunLoop().addWriter(self)

   def shutdown(self, how):
         self._socket.shutdown(how)


   # these probably shouldn't be called when we're listening
   def scheduleInRunLoop(self, runLoop):
         runLoop.addReader(self)
         runLoop.addWriter(self)

   def removeFromRunLoop(self, runLoop):
       self.setTimeOut(None)
       try:
         runLoop.removeReader(self)
       except KeyError:
           pass

       try:
         runLoop.removeWriter(self)
       except KeyError:
           pass


class DelayedCall:
    def __init__(self, secondsOrRRule, func, *args, **kw):
       self.repeatRule = None
       try:
          self.time = time.time() + secondsOrRRule
       except TypeError:
          # it's not a number of seconds hopefully it's an rrule that's been converted to a generator
          self.repeatRule = secondsOrRRule
          dt = self.repeatRule.next()
          self.time = time.mktime(dt.timetuple()) #time.time() + self.delay
       
       self.cancelled = self.called = False
       self.func = func
       self.args = args
       self.kw = kw
       self.delayed_time = 0

    def __cmp__(self, other):
       return cmp(self.time, other.time)

    def onTimeout(self):
       if self.delayed_time:
          # this timer has been delayed, so reschedule it
          self.time += self.delayed_time
          self.delayed_time = 0
          RunLoop.currentRunLoop().addTimer(self)
          return
          
       if not self.cancelled:
          self.func(*self.args, **self.kw)
          if self.repeatRule:
             try:
                dt = self.repeatRule.next()
                self.time = time.mktime(dt.timetuple()) #time.time() + self.delay
                RunLoop.currentRunLoop().addTimer(self)
             except StopIteration: # rule has been exhausted
                pass

          else:
             # This allows us to reset the time
             # Q, does it? Or is it cruft from twisted that we don't need?
             self.called = True

    def cancel(self):
        """Unschedule this call

        @raise AlreadyCancelled: Raised if this call has already been
        unscheduled.

        @raise AlreadyCalled: Raised if this call has already been made.
        """
        if self.cancelled:
           raise RuntimeError("Already Cancelled")
        elif self.called:
           raise RuntimeError("Already Called")
        else:
           self.cancelled = True
           del self.func, self.args, self.kw

    def pushBack(self, secondsLater):
       """Reschedule this call for a later time
       
       @type secondsLater: C{float}
       @param secondsLater: The number of seconds after the originally
       scheduled time for which to reschedule this call.
       
       @raise AlreadyCancelled: Raised if this call has been cancelled.
       @raise AlreadyCalled: Raised if this call has already been made.
       """

       # if this variable is set when onTimeOut is called, the
       # function won't be called, instead the timer will readd itself
       # back to the runloop at a greater time
       
       if self.cancelled:
          raise RuntimeError("Already Cancelled")
       elif self.called:
          raise RuntimeError("Already Called")

       else:
          self.delayed_time += secondsLater
 


 
   


class RunLoop(object):
      """An event loop that provides edge-triggered notifications.

      This RunLoop monitors a series of file descriptors, timers and
      OS signals for events.

      Each pass through the RunLoop we check to see if any timer has
      expired at which point we call the timer's timeout() method
      giving the Timer an oprotunity to preform any neccary actions.

      After notifying each expired Timer we calculate how long until
      the next timer (if any) will expire.

      We then ask the OS to put us to sleep until one or more of
      our file descriptors has data ready to be read or written to; or
      our timeout has expired.

      When we wake up it's because  one of our descriptors
      are in the ready state, a timer has expired or both.
      
      If one of our descriptors is ready we remove it from the list of
      descriptors to be monitored and then notify the apportiate
      callback/delegate that it can now read or write the descriptor
      without blocking. Note: it's the responsabilty of the delegate
      to ask the runLoop to remonitor a descriptor

      And that's it the loop starts over if there are any timers or
      descriptors left to be monitored.
      
      You do not need to instatiate a RunLoop, there should only be
      one per thread. To get the RunLoop for the current thread simply
      call the class method currentLoop()

      >>> runloop = RunLoop.currentRunLoop()
      
      To determine if the runLoop is running you can examine it's
      running property, in this paticular case we're not running

      >>> runloop.running 
      False

      To start the runLoop you must call run(), this will block the
      thread until the runLoop runs out of things to montior. Since we
      have nothing to montior calling run() will return right away.
      >>> runloop.run()
      >>> runloop.running
      False

      That's pretty boring, let's create a class that support's the
      Timer interface and have our object called imideiatly. Timer's
      need two attributes a timeout value, which is the in seconds (as
      typically returned by time.time()) after which the timer's
      timeout() method should be called.
      
      >>> class MyTimer:
      ...   def __init__(self):
      ...     self.time = time.time()
      ...     self.cancelled = self.called = False
      ...     self.timeOutCalled = False
      ...   def onTimeout(self):
      ...     self.timeWhenCalled = time.time()
      ...     self.timeOutCalled = True

      >>> myTimer = MyTimer()

      So we have a Timer, it has an attribute called timeOutCalled
      which is currently false

      >>> myTimer.timeOutCalled
      False

      We add it to the runloop then run the runloop
      >>> timer = runloop.addTimer(myTimer)
      >>> runloop.run()
      
      And when the runloop completes our timer's timeout value should
      have been called.
      >>> myTimer.timeOutCalled
      True
      
      Noticed that the code returned imediatly because after signaling
      this timer there was nothing else to monitor. Typically
      applications that use a RunLoop will always ensure that there's
      something to monitor. For instance we can make a component that
      get's called once every millisecond for 10 miliseconds by simply
      readding the Timer back to the RunLoop in the Timer's timeout
      method like this.

      >>> class HeartBeat:
      ...   def __init__(self):
      ...     self.time = time.time() + .01
      ...     self.cancelled = self.called = False
      ...     self.ticks = 0
      ...   def onTimeout(self):
      ...     self.ticks += 1
      ...     if self.ticks < 10:
      ...       self.time = time.time() + .01
      ...       RunLoop.currentRunLoop().addTimer(self)

      Notice in this example a couple of things, for one we set
      HeartBeat.time to be the current time plus ".01". In other words
      we want are timeout() method to be called 1 milisecond from
      now. We keep track of how many times we're called, if it's less
      than 10 we reschedule ourselves back in the current
      runLoop. This demonstrates how an object doesn't need to keep a
      reference to the runLoop to use it.

      >>> timer = HeartBeat()
      >>> timer.ticks
      0
      
      >>> timer = runloop.addTimer(timer)
      >>> runloop.run()
      >>> timer.ticks
      10
      
      Normally you wouldn't implement your own Timer class because
      most of the basic ones that you'd need have already been
      implemented for you like a DeferedCall which will run a specific
      method after a certain delay and optionally repeat if neccesary.


      """
      log = outlet("LogService")
      
      def __init__(self):
            self.threadCallQueue = []
            self.readers = {}
            self.writers = {}

            reader, writer = os.pipe()
            self.wakerStream = Stream(reader, self)
            
            
            # no need to use an output stream because a pipe is almost
            # always going to be be ready to be written to
            
            self.waker = writer
            self.running = False
            self.timers = []

            

      def currentRunLoop(klass):
            # class method that returns the runLoop for the current
            # thread. Use this rather than creating your own thread.
            try:
                  runLoop = ThreadStorageService.getFromCurrent('RunLoop')
            except KeyError:
                  runLoop = klass()
                  ThreadStorageService.addToCurrent('RunLoop', runLoop)

                  # async read one, byte which will schedules in the
                  # runLoop, can't do this in init because we'd
                  # recures back to this currentRunLoop call
                  runLoop.wakerStream.read(1)

            return runLoop
      
      currentRunLoop = classmethod(currentRunLoop)

      def runLoopForThread(klass, threadName):
            """Return the RunLoop from the specified thread. If the
            RunLoop does not exist, an error will be raised.
            
            """
            return ThreadStorageService.getFromThread(threadName, 'RunLoop')
      runLoopForThread = classmethod(runLoopForThread)

      
      def addReader(self, source): 
            self.readers[source.fileno()] = source

      def removeReader(self, source):
            del self.readers[source.fileno()]

      def addWriter(self, source):

            self.writers[source.fileno()] = source

      def removeWriter(self, source):
            del self.writers[source.fileno()]
            

      def reset(self):
         self.running = False
         self.readers = {}
         self.writers = {}
         self.timers  = []
         self.threadCallQueue = []
         self.wakerStream.read(1)

      def _shouldRun(self,timerCapacity):
         # Internal method, determines if the runLoop should be stooped.

         # runLoop.run() will call this method with a value of 0,
         # indicating that if there are any timers, then the runLoop
         # should continue until they fire

         # runLoop.runUntil() will call this method with a value of
         # one, indicating that there must be more than 1 timer, or
         # else the runLoop should quit. This is because runUntil()
         # adds one timer to stop the runLoop at the specified time,
         # but this timer shouldn't be considered something that keeps
         # the runLoop going if there is no other activity to monitor.


         # Keep calling the runLoop until some one stops us, we
         # have no timers or the readers and writers drops to 1
         # (runLoops keep one reader around to wake
         # themselves up from a sleep)
         return self.running and (len(self.readers) + len(self.writers)  > 1 or
                                  len(self.timers) > timerCapacity or self.threadCallQueue)


      def quitOnExceptionHandler(self, exception):
        raise exception
      #handleException = quitOnExceptionHandler  
        
      def ignoreExceptionHandler(self, exception):
        pass
      handleException = ignoreExceptionHandler  
      
      def run(self, reset_on_stop=True):
         """Keeps the runLoop going until it's explicitly stoped or it runs out
         of things to monitor."""
         
         if self.running:
            raise RuntimeError("RunLoop is already running.")
         else:
            self.running = True
            
         while self._shouldRun(0):
            try:
               self.runOnce()
            except Exception, e:
               self.log.exception("Caught unexpected error in RunOnce.")
               self.handleException(e)
         
         if reset_on_stop:      
           self.reset()

      def runUntil(self, stopDate=None, **kw):
         """Runs the runLoop until the given time plus interval have been
         reached or it runs out of things to monitor. This method
         should not be called when the runLoop is already running.

         The current time is assumed, if no date time is passed in.

         Examples:(note, these aren't real doctests yet)

         Run until a given date, say St. Patty's day
         >> date=datetime.datetime(2007, 03,17, 17,00)
         >> RunLoop.currentRunLoop().runUntil(dateAndTime)

         Additionally you can pass in any keyword argument normally
         taken by daetutilse.relativedelta to derive the date. These
         include:

         years, months, weeks, days, hours, minutes, seconds, microseconds

         These are moste useful when you want to compute the relative
         offset from now. For example to run the runLoop for 5 seconds
         you could do this.

         >> RunLoop.currentRunLoop().runUntil(seconds=5)

         Or, probably not as practical but still possible, wait one
         year and 3 days

         >> RunLoop.currentRunLoop().runUntil(years=1, days=3)
         

         """

         if self.running:
            raise RuntimeError("RunLoop is already running.")
         else:
            self.running = True

         delta = relativedelta(**kw)
         now = datetime.datetime.now()
         
         if stopDate is None:
            stopDate = now

         stopDate = now + delta

         # convert the time back into seconds since the epoch,
         # subtract now from it, and this will then be the delay we
         # can use

         seconds2Run = time.mktime(stopDate.timetuple()) - time.mktime(now.timetuple())
         self.waitBeforeCalling(seconds2Run, self.stop)
         
         while self._shouldRun(1):
            try:
               self.runOnce()
            except:
               self.log.exception("Caught unexpected error in RunOnce.")
               
         self.reset()

            

      def runOnce(self):

         # call every fucnction that was queued via callFromThread up
         # until this point, but nothing more. If not we could be
         # stuck doing this forever and never getting to the other calls
         
         pending = len(self.threadCallQueue)
         tried   = 0
         try:
            for (f, a, kw) in self.threadCallQueue[:pending]:
               tried += 1
               f(*a, **kw)
               
         finally:
            # it's possible that more calls could have came in since we
            # started, bu they should be on the end of the list
            del self.threadCallQueue[:tried]


         # we sleep until we either receive data or our earliest
         # timer has expired.


         currentTime = time.time()
         # fire every timer that's expired

         while self.timers:
               timer   = heapq.heappop(self.timers)
               if timer.cancelled:
                     continue

               timeout = timer.time - currentTime
               if timeout <= 0:
                     # it's expired call it
                     timer.onTimeout()
               else:
                     # this timer hasn't expired put it back on the list
                     heapq.heappush(self.timers, timer)
                     break

         else:
               if (len(self.readers) + len(self.writers)) <= 1:
                     # we don't have any timers, if we're not monitoring
                     # any descriptors we need to bail
                     return
               else:
                     # no timed events but we have file descriptors
                     # to monitor so sleep until they have
                     # activity.

                     timeout = None 

         try:
               ready2Read, ready2Write, hadErrors =\
                           select.select(self.readers.keys(), 
                                         self.writers.keys(), 
                                         [], timeout)
         except (select.error, IOError), e:
               if e.args[0] == errno.EINTR:

                     # a signal interupted our select, hopefully
                     # someone eles is handling signals and using
                     # callFromeThread to do the right thing.
                     return
               elif e.args[0] == errno.EBADF:
                 # ugh
                 self.clear_bad_descriptor()
                 return
               else:
                     raise

         while ready2Read or ready2Write or hadErrors:
               # note the popping alows us not get hung up doing all reads all writes
               # at once, not sure how useful this is.
               if ready2Read:
                     fileno = ready2Read.pop()
                     stream = self.readers.pop(fileno)
                     stream.canRead(stream)
                     #stream.handleEvent(stream,Stream.HAS_BYTES_AVAILABLE)

               if ready2Write:
                     writer = ready2Write.pop()
                     # writers, when ready will always be ready. To
                     # avoid an infinite loop an app that wishes to
                     # read the data they must call addWriter()
                     # again
                     stream = self.writers.pop(writer, None)
                     # stream will be none if a method called during ready2read removed
                     # it prior to checking the writers.
                     if stream: 
                       stream.canWrite(stream)
                        #stream.handleEvent(stream, Stream.HAS_SPACE_AVAILABLE)
      def stop(self):
            self.running = False # this will drop us out of the runLoop on it's next pass
            self.wakeup()

      def addTimer(self, timer):
            heapq.heappush(self.timers, timer)
            self.wakeup()
            # we return the timer for convienance sake
            return timer

      def wakeup(self):
            os.write(self.waker, 'x') # write one byte to wake up the runLoop

      def onRead(self, stream, data):

            # we've been woken up, ignore the data and readAgain which
            # should schedule us once more back in the runLoop
            stream.read(1)

      def callFromThread(self, f, *args, **kw):
            assert callable(f), "%s is not callable" % f
            self.threadCallQueue.append((f, args, kw))
            self.wakeup()

      def waitBeforeCalling(self, seconds, method, *args,  **kw):
            # Create a non repeating event
            dc = DelayedCall(seconds, method, *args,  **kw)
            self.addTimer(dc)
            return dc

      def intervalBetweenCalling(self, secondsOrRRule, method, *args, **kw):
         # Create a repeating event, this method can be called
         # either with the number of seconds between each call or
         # it can be passed a string or dateutil.rrule

         t = type(secondsOrRRule)
         # Convert to an RRULe if it's a string or a number
         if t in (int, float):
            rule = rrule.rrule(rrule.SECONDLY, interval=secondsOrRRule)
         elif isinstance(secondsOrRRule, basestring):
            rule = rrule.rrulestr(secondsOrRRule)
         else:
            # hopefully it's an object that returns an iteration of datetime objects
            rule = secondsOrRRule
            
         dc = DelayedCall(iter(rule), method, *args,  **kw)
         self.addTimer(dc)
         return dc
         
      def clear_bad_descriptor(self):
        # ugh not pretty when this happens
        
        for key in self.readers.keys():
          try:
            select.select([key],[],[], 0)
          except Exception, e:
            bad = self.readers.pop(key)
            bad.onError(e)

        for key in self.writers.keys():
          try:
            select.select([],[key],[], 0)
          except Exception, e:
            bad = self.writers.pop(key)
            bad.onError(e)
        
            


                  
if __name__ == "__main__":
   # All this code should be moved to the test directory

      class Echo(object):
            def canWrite(self, stream):
                  print "Writer: Some one's listening say hello."
                  stream.write("Why Hello, there.")

            def canRead(self, stream):
                  data = stream.read(1024)
                  print "Reader: I just got this message %s" % data

      class EchoProtocol(object):
            """This demonstartes the use of a Port to implment a protocol. In this
            case, as soon as we establish a connection, we write
            something to the socket and wait until it get's echoed
            back to us. Once done we tear down the connection."""
            
            msg = "Hello server!"
            
            def __init__(self):
                  # Since we're using the same observer for both the
                  # client and the server we need to keep track of
                  # whose who
                  self.serverPort = None
                  self.clientPort = None


            def onConnect(self, port):
                  print "Connection made %s" % port
                  self.clientPort = port
                  port.write(self.msg)
                  port.read(len(self.msg))

            def onAccept(self, port):
                  self.serverPort = port         
                  print "Accepted new connection %s" %  port
                  # Wait for the client to send us some data
                  port.read(4096)
                  #port.write("Welcome! Name?\n")

            def onRead(self, port, data):
                  # Keep reading until we get closed
                  if port is self.serverPort:
                        # Echo the data back to our client
                        port.write(data)
                        # wait for more data
                        port.read(4096)
                  else:
                        # it's our client port
                        print "I said %s and I got %s back!" % (self.msg, data)
                        # close the port down, which sholud shut the port down on the other side as well
                        port.close()

            def onWrite(self, port, bytesWritten):
                  if port is self.serverPort:
                        print "Server just wrote",
                  else:
                        print "Client just wrote",

                  print " %s bytes" % bytesWritten

            def onClose(self, port):
                  if port is self.serverPort:
                        print "Server side closed!"
                  else:
                        print "Client side closed!"
                  

      def showTime(interval, lastCall=[0]):
            current = time.time()
            print "Interval: %s Current time: %s, Time Between Last Call %s" % \
                (interval, current, current - lastCall[0])
            lastCall[0] = current


#       reader, writer = os.pipe()
#       print "Echo pipe %s %s" % (reader, writer)
#       inputStream = InputStream(reader)
#       outputStream = OutputStream(writer)
#       outputStream.delegate = inputStream.delegate =  Echo()

      runLoop = RunLoop.currentRunLoop()

#       inputStream.scheduleInRunLoop(runLoop)
#       outputStream.scheduleInRunLoop(runLoop)
#       # chatter back and fourth until we stop the runLoop
#       #runLoop.addTimer(DelayedCall(5,False,runLoop.stop))
#       runLoop.addTimer(DelayedCall(1, False, showTime, 1))
#       runLoop.addTimer(DelayedCall(1.5, False, showTime, 1.5))
#       runLoop.run()


      # Since our Echo example made sure to unregister itself after
      # chatting we didn't need to call stop

      # neet well that was with a pipe let's chatter with a socket
      observer = EchoProtocol()
      serverPort = Port("/tmp/foo.soc", observer)
      serverPort.listen(5)
      clientPort = Port("/tmp/foo.soc", observer)
      clientPort.connect()

      runLoop.addTimer(DelayedCall(.01, False, runLoop.stop))
      runLoop.run()

