"""juncture
\brief Juncture client library

This module defines classes and functions used to create Juncture
clients. Using this library programers can create programs that
connect to a Juncture Application server and execute one or more
buisness methods.

An overview of the process is as follows.

<ol>
<li>First, a program establishes a connection to the server by specifying
either it's hostname or ip address using the \em connect() method
and the database the connection wishes to use.
</li>

<li>Next, the program can lookup() one or more remote objects, either a
home or a service.
</li>

<li>Then before using any of these objects \em Connection.login() must be
called with a valid username and password.
</li>

<li>Depending on the remote object you may want to start a
transaction. A transaction groups a series of operations together as
an atomic unit. All the operations need to succeed or the
transaction will be rolled back. Most methods, if called without a
transaction, will automatically start and commit one for you. Some
methods, however, require a transaction. For instance the QueryService
requires that you manually start a transaction prior to invoking
methods on the objects it returns. To do this you can call
Connectin.begin() and optionally pass a timeout parameter. By default
you have 60 seconds to complete your transaction or it will be rolled
back. You can pass an optional argument to Connection.begin() to adjust this time
out as neccesary. To make your changes permanent you must call
Connection.commit(). To discard all your changes call
Connection.rollback()
</li>
</ol>

Here's a small example that connects to a server and looks up all
leads assigned to a specific user.

\code
import juncture

conn = juncture.connect("myserver.juncture.com", "Test")
conn.login("admin", "admin")

# The lead session home provides the business methods for working with
# leads.
LeadSessionHome = conn.lookup("LeadSessionHome")

try:
  conn.begin()
  leads = LeadSessionHome.create("new", "lead")
  conn.commit()
except:
  # If there was any error roll the transaction back
  conn.rollback()
  # Reraise the error
  raise

# Now that we're done logout
conn.logout()
\endcode

Look at the class list to see all the different Home and Session
objects available for your use.

\todo Remove xpcom library
\todo implement the connection object so that the look-up method uses the right name server
\todo move the initialization of the orb to be at the module level
\todo move this module to Rambler this should become the client library

\todo add ability to load other CORBA namespaces (like epo) to this
module and have the code that morphs the _get_ and _set_ on them

"""

import os

from omniORB import CORBA
import CosTransactions
import sys
from types import ClassType
import new

import epo
import Credentials


class Server:
    def _setConn(self, conn):
        self._conn = conn

    def __getattr__(self, attr):
        return getattr(self._conn, attr)
Server = Server()

orb = None

def connect(host, name):
    """\brief Open a connection to a database on a given juncture server.
    \param host \c str the ip address or hostname to connect to
    \param name \c str the instance name on the host to use
    \return a new Connection object
    \todo implement this function
    
    """

    global orb
    if orb is None:
        args = []
        #args.append("NameService=corbaname::1.2@%s:49000" % dest)
        #args.append("NameService=corbaname::1.2@localhost:49000")
        args.append("-ORBofferBiDirectionalGIOP")
        args.append("1")
        args.append("-ORBclientTransportRule")
        args.append("* tcp,unix,bidir")

        orb = CORBA.ORB_init(args, CORBA.ORB_ID)
        poa = orb.resolve_initial_references("RootPOA")
        poa._get_the_POAManager().activate()
        
    return Connection(host, name, orb)
        
class Connection:
    """\brief Represents a connection to a paticular Juncture server and database.

    Connection objects are obtained by calling juncture.connect()
    """
    _orb = None

    def __init__(self, serverIp, instanceName, orb):
        self._objs = {}
        self._orb = orb
        self.serverIp = serverIp
        self.instanceName = instanceName

        self._txn = self._orb.resolve_initial_references("TransactionCurrent")
        
        ior = "corbaname:iiop:1.2@%s:49000/NameService#omniTransactionFactory"\
              % self.serverIp
        self._txnFactory = orb.string_to_object(ior)
        self.clientToken = None

    def lookup(self, oid):
        """\brief Find the remote object specified by oid.

        This method will conntact the naming service on the host that
        the Connection was opened to and will attempt to resolve the
        name of the object that was sepcified. If it is succesful it
        will return the corresponding object. If unsuccsesful an
        exception will be raised. Once an object has been found it's
        reference is cached for the life of the connection to avoid
        additional network look ups in the future.
        
        \param oid \c str the name of a remote home or session object.
        \return 
        """
        obj = self._objs.get(oid)
        if not obj:
            #obj = self._orb.string_to_object("corbaname:rir:#%s" % oid)
            ior = "corbaname:iiop:1.2@%s:49000/NameService#itbe/%s/%s" % (self.serverIp,
                                                                           self.instanceName,
                                                                           oid)
            try:
                obj = self._orb.string_to_object(ior)
            except:
                raise RuntimeError, "Unable to find remote reference for %s" % ior
            
            self._objs[oid] = obj

        return obj

    def login(self, username, password, allThreads=False, callBack=None):
        """Attempts to log the given user into the server with the
        username and password specified.

        \param username: \c str The user's name (duh)
        \param password: \c str The user's password
        \param allThreads: \c bool Optional argument. If true then the
        login will be effective for all threads in
        the client. If false then the login is only
        good for the current thread.
         
        \exception NO_PERMISSION: Raised if the usernam and password are invalid
        

        Examples:
        \code
        # Login as 'admin' for the current thread with the password of 'admin'.
        >> server.login("admin", "admin")
        
        # Login as 'admin' on all threads
        >> server.login("admin", "admin", True)
        \endcode
        """
        
        IM = self.lookup("IdentityService")
        self.clientToken = token = IM.login(username, password, callBack)
        Credentials.setCredentials(token, allThreads)

    def logout(self, allThreads=False):
        """\brief logout the user

        Logs out the user on the current thread, or optionally all
        users on all threads. You must commit or rollback any
        transactions prior to calling this method.


        \param allThreads \c bool optional argument.  If True then
        logs all users on all threads out. If false only logs the user
        on the current thread out. Defaults to false.
                      
        \exception TransactionForbidden: Raised if a transaction is
        active when this method is invoked.

        
        Examples:
        \code
        # Logout the user that's logged in on the current thread
        >> server.logout()

        # Logout all users on all threads
        >> server.logout(True)
        \endcode
        """
        IM = self.lookup("IdentityService")
        try:
            token = IM.logout()
            Credentials.removeCredentials(token, allThreads)
        except:
            pass
        
        if self.clientToken:
            Credentials.removeCredentials(self.clientToken, allThreads)

        self.clientToken = None

    def begin(self, timeout=60):
        """\brief Begin a transaction.
        
        Begin a transaction, optionaly setting a
        timeout. server.commit() or server.rollback() must be called
        within the timeout that is specified or the transaction will
        automatically be rolled back. The default timout is 60 seconds.


        \param timeout: \c int Optional argument. Time in seconds before a
        transaction is automatically rolledback. Setting
        this to a value of 0 will prevent the transaction
        from being automatically rolled back. Disabeling the
        timeout is not recommended since resources will not be
        reclaimed on the server until the transaction is finished.

                   
       Example:
        \code
        # Start a transaction using the default timeout
        >> server.begin()
        # Start a transaction setting the timeout to 2 minutes
        >> server.begin(120)
        # Start a transaction with no timeout
        >> server.begin(0)
        \endcode
        """

        # We don't use TransactionCurrent.begin() because it's hard
        # coded to look-up the TransactionFactory that's published at
        # omniTransactionFactory in the NameService that was provided
        # with initRef.
        
        cntrl = self._txnFactory.create(timeout)
        self._txn.resume(cntrl)

    def commit(self):
        """Commits the current transaction.
        """
        self._txn.commit(1)

    def rollback(self):
        """Abort the current transaction.
        """
        self._txn.rollback()

    def suspend(self):
        self._txn.suspend()

    def getTXNStatus(self):
        return self._txn.get_status()




