import socket, os, base64, time, thread
import commands

def makeToken():
    """Returns a token that should be unique based on the server name,
    the process id and the current time base64 encoded."""
    
    return base64.encodestring("%s:%s,%s" %
                               (socket.gethostname(),
                                os.getpid(),
                                time.time()))

# for the life of the app this token has to be the same and unique.

serverToken=  makeToken()

def newClientToken():
    return '%s:%s' % (serverToken, makeToken())

threads = {}
def setCredentials(token, allThreads=False):
    removeCredentials(token, allThreads)
    
    if allThreads:
        threadKey = None
    else:
        threadKey = thread.get_ident()
    
    tokens = threads.get(threadKey, [])
    
    tokens.append(token)
    threads[threadKey] = tokens

        

def removeCredentials(token, allThreads=False):
    serverToken = token.split(':')[0]

    if allThreads:
        threadKey = None
    else:
        threadKey = thread.get_ident()

    tokens = threads.get(threadKey, [])

    newTokens = []
    if tokens:
        for storedToken in tokens:
            if not storedToken.startswith(serverToken):
                newTokens.append(storedToken)

    if not newTokens:
        try:
            del threads[threadKey]
        except:
            pass
    else:
        threads[threadKey] = newTokens


        
def _insertToken(op, srvctx):
    if threads.has_key(None):
        threadKey = None
    else:
        threadKey = thread.get_ident()

    tokens = threads.get(threadKey, [])
    if tokens:
        tokens = omniORB.cdrMarshal(epo._tc_stringList, tokens)
        srvctx.append((0x52434301, tokens))
    
callbacks = []
def getCredentials():
    if threads.has_key(None):
        threadKey = None
    else:
        threadKey = thread.get_ident()

    tokens = threads.get(threadKey, [])
    for token in tokens:
        if token.startswith(serverToken):
            return token

    return None

def registerCallback(callback):
    callbacks.append(callback)

def _extractToken(op, contexts):
    threadId = thread.get_ident()
    for k, v in contexts: 
        if k == 0x52434301:
            try:
                tokens = omniORB.cdrUnmarshal(epo._tc_stringList, v)
            except CORBA.MARSHAL:
                # Backwards compatibility with pre 1.26 clients
                tokens = [omniORB.cdrUnmarshal(CORBA._tc_string, v)]
            
            #print "Recieved request for %s on thread %s for token %s" % (op, threadId, token)
            if threads.has_key(threadId):
                #log.error("Already have a token for thread %s" % threadId)
                raise CORBA.SystemException

            clientToken = None
            for token in tokens:
                if token.startswith(serverToken):
                    clientToken = token
                    break
                
            if clientToken:
                for callback in callbacks:
                    callback(clientToken)

                threads[threadId] = tokens

def _deleteToken(*args, **kw):
    token = getCredentials()
    if token:
        removeCredentials(token)

# Register everything
try:
  #TODO Remove all referenecs of corba, to the corba/bridge module
  import omniORB, epo
  from omniORB import interceptors, CORBA

  interceptors.addClientSendRequest(_insertToken)
  interceptors.addServerReceiveRequest(_extractToken)
  interceptors.addServerSendReply(_deleteToken)
  interceptors.addServerSendException(_deleteToken)
except ImportError:
	pass