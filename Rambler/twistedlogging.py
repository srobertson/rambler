from Rambler import outlet
import sys
# class stolen from twisted, and made to work with our logging service

logerr = None


class StdioOnnaStick(object):
    """Class that pretends to be stout/err."""
    log = outlet("LogService")

    closed = 0
    softspace = 0
    mode = 'wb'
    name = '<stdio (log)>'

    def __init__(self, isError=1):
        self.isError = isError
        self.buf = ''


    def assembled(self):
        # as soon as we're assembled store a ref in a global in this module
        global logerr
        logerr = self
        
        if self.isError:
            self.logMethod = self.log.error
        else:
            self.logMethod = self.log.info


    def close(self):
        pass

    def fileno(self):
        return -1

    def flush(self):
        pass

    def read(self):
        raise IOError("can't read from the log!")

    readline = read
    readlines = read
    seek = read
    tell = read

    def write(self, data):
        d = (self.buf + data).split('\n')
        self.buf = d[-1]
        messages = d[0:-1]
            
        for message in messages:
            self.logMethod(message)
            #msg(message, printed=1, isError=self.isError)

    def writelines(self, lines):
        for line in lines:
            self.logMethod(line)

def err(msg):
    # don't call this method until the above class is fully assembled as a service
    if logerr is not None:
	logerr.log.error(msg)
    else:
	# logerr isn't setup yet, so dump to standard error
	print >> sys.stderr, msg

def msg(msg, isError):
    if logerr is not None:
	if isError:
	    logerr.log.error(msg)
	else:
	    logerr.log.info(msg)
    else:
	# logerr isn't setup yet, so dump to standard error
	print >> sys.stderr, msg

