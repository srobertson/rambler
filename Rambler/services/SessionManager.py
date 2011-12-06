from Rambler import outlet, option
from Rambler.Synchronized import synchronized
from Rambler.Events import LoginEvent


import threading
from threading import Thread
from time import sleep


from Rambler.Credentials import getCredentials, registerCallback, newClientToken


# TODO: Need away for this to be configurable, and possibly
# untamperable. Although if we control the box maybe it's not a big
# deal.

class SessionManager(object):

    """Keeps track of each open transaction and times it out after a
    preconfigured experation time."""

    maxIdle = option('application', 'idle', 7200)
    maxConnections = option('application', 'maxconnections', 10)
    inactiveScanPeriod = 10

    eventChannel = outlet('EventService')
    configService = outlet('ConfigService')

    txn = outlet("TransactionService")
    log = outlet("LogService")
    configService = outlet("ConfigService")
    errorFactory = outlet("ErrorFactory")
    runLoop = outlet('RunLoop')
    
    def __init__(self):

        #self.maxConnections = None
        
        #registerCallback(self.resetIdleTimeForUser)
        
        self._sessions = {}
        self._tokens = {} # Maps tokens to usernames
        self.scanner = None
        #self.setTimeout(30)

    def assembled(self):
        # Subscribe after our applications bootstrapper

        self.eventChannel.subscribeToEvent(
            "login",self.newSession, LoginEvent)


        self.eventChannel.subscribeToEvent(
            "logout",self.removeSession, str)

        self.eventChannel.subscribeToEvent(
            "Shutdown",self.logoutAllSessions, str)

        # We publish logout whenever the same user tries to login
        # twice in a row.
        self.eventChannel.registerEvent("logout", self, str)
      

    def newSession(self, username, password, callback):
        if self.scanner is None:
          self.scanner = self.runLoop.currentRunLoop().intervalBetweenCalling(self.inactiveScanPeriod, self.bootInactiveSessions)
      

        clientToken = newClientToken()

        oldsession = self._sessions.get(username)

        if oldsession:

            self.log.info("%s has previously authenticated." % (username))

            if oldsession._callback:
                self.log.debug("Attempting to contact %s's client" % username)
                try:
                    oldsession._callback.onLogout("You have been logged out because you logged in at another location.")
                except (OBJECT_NOT_EXIST, TRANSIENT):
                    self.log.debug("newSession couldn't contact %s's client :(" % username)
                    # Couldnt' contact client, ignore
                    pass

            self.removeSession(oldsession.getToken())
            
        elif len(self._sessions) >= self.maxConnections:
            msg = "Can't login %s. Maximum number of licensed logins (%s) "\
                     "reached." % (username, self.maxConnections)
            self.log.info(msg)
            raise NO_PERMISSION(msg)            

        # At this point we need to create a new session
        self.log.info('%s has logged in' % username)

        self._sessions[username] = LoginSession(clientToken, username, password, callback)
        self._tokens[clientToken] = username

        return clientToken

    newSession = synchronized(newSession)


    def removeSession(self, token=None):
        """Removes the session when the user logs out."""
        if not token:
            token = getCredentials()
        username = self._tokens[token]
        del self._sessions[username]
        del self._tokens[token]
        self.log.info('%s has logged out' % username)
        return token

    def resetIdleTimeForUser(self, token):
        try:
            username = self._tokens[token]
        except KeyError:
            # Most likely the client was logged out for being idle but didn't
            # get a notification.
            raise NO_PERMISSION('SessionManager: Invalid security token.')

        self._sessions[username].resetIdleTime()
    resetIdleTimeForUser = synchronized(resetIdleTimeForUser)
        
    
    def listSessions(self):

        """ Returns a list of all the sessions that have logged in."""
        return self._sessions.values()
        
    def bootInactiveSessions(self):

        """Removes all users that have been idle for longer than max
        allowed time. """

        sessions = self._sessions.items()

        for sid, session in sessions:
            if session.getIdleTime() > self.maxIdle:
                # HACK: omniOTS needs to initialize it's perthread
                # data, which happens whenever you call a method on
                # the current object.
                
                self.txn.get_timeout()
                username = session.getUserName()
                if session._callback:
                    self.log.debug("Attempting to contact %s's client" % username)
                    try:
                        session._callback.onLogout("You have been logged out by the server for being idle.")
                        pass
                    except (OBJECT_NOT_EXIST, TRANSIENT):
                        # Couldnt' contact client, ignore
                        self.log.debug("bootInactiveSessions couldn't contact %s's client :(" % username)
                        pass


                self.removeSession(session.getToken())

                
    bootInactiveSessions = synchronized(bootInactiveSessions)
            

    def getTimeout(self):

        """Returns the current number of seconds a user is allowed to
        be idle."""
        
        return self.maxIdle
        
    def setTimeout(self, timeout):

        """Set's the number of seconds a user is allowed to be idle
        before logging out and rolling back their Transaction."""

        self.maxIdle = timeout

    def getCredentials(self):

        """Returns the User entity associated with the current
        transaction. NO_PERMISSION is
        raised if the user has yet to be authenticated in the given
        transaction."""

        if threading.currentThread().getName() == 'MainThread':
            # it's the main thread so return the System user
            return Credentials('system', '')

        token = getCredentials()
        
        if token is None:
            raise NO_PERMISSION()

        username = self._tokens[token]
        session = self._sessions[username]
        return Credentials(username, str(session.getPassword()))

    def logoutAllSessions(self, message):
        self.log.info('Logging out all sessions')
        sessions = self._sessions.items()

        for sid, session in sessions:

            # omniots, thread hack, probably should fix omniots
            self.txn.get_timeout()
            
            username = session.getUserName()
            try:

                self.log.info('notifying %s of server shutdown' % username)
                session._callback.onLogout("The server has been shutdown and you have been logged out.")
                self.log.info('succesfully notified %s of server shutdown' % username)
            except (OBJECT_NOT_EXIST, TRANSIENT):
                # Couldnt' contact client, ignore
                self.log.debug("logoutAllSessions couldn't contact %s's client :(" % username)
                pass
            except AttributeError:
                # No callback registered
                pass

    def sendMessage(self, message):
        sessions = self._sessions.items()

        for sid, session in sessions:
            self._sendMessageToSession(session, message)

    def sendMessageToUser(self, username, message):
        session = self._sessions.get(username)
        self._sendMessageToSession(session, message)


    def _sendMessageToSession(self, session, message):
        try:
            session._callback.displayMessage(str(message))
        except (OBJECT_NOT_EXIST, TRANSIENT):
            self.log.debug("sendMessage couldn't contact %s's client :(" % session.getUserName())
            pass
        except AttributeError:
            # No callback registered
            pass

import time
class LoginSession:

    def __init__(self, token, username, password, callback):
        self._token    = token
        self._username = username
        self._password = password
        self._callback = callback

        if self._callback is not None:
            omniORB.setClientCallTimeout(self._callback, 500)
            
        self.resetIdleTime()

    def getToken(self):
        return self._token

    def getUserName(self):
        """Returns the username"""
        return self._username

    def getPassword(self):
        return self._password
    
    def getIdleTime(self):

        """Returns the length of time the user has been idle in
        seconds."""
        return time.time() - self._lastTime

    def resetIdleTime(self):

        """Set's the length the time the user has been idle back to
        0. Called when the user has invoke a method."""

        self._lastTime = time.time()
        

"""

Goals:

* Limit the user from being logged in more than once.

* Limit the total number of users logged in.

* An admin always needs to be able to login, yet he should Keep at
  least one connection open for an admin, or allow the admin to knock
  a non privledgeded user offline when he logs in.

* Allow the Admin to log users off from the console.

* Reclaim resources and rollback transaction if the user has been
  idle, manually logged off, or booted because they have logged in
  twice.




Notes on the session manager.

Currently a transaction needs to be started.

Then authenticate needs to be called to associate the with the transaction.

There is no logout procedure, so the Identity manager never cleans-up the transaction.

The HomeBase keeps track of all observers, that stay indefinatly matched.

If a client dies before commiting or rollingback, some resources like
the unit of work, ReportService and SimpleEnumerator are never reclaimed.


Scenario one, have the SessionManager logout the user if the user attempts to login twice.

Cons:

 - The user always logs in after every transaction, solution would be
 to have the IM listen for commit/rollback messages and clean-up it's
 data after that. The problem with that 
"""
