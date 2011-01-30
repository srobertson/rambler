# NOTE: Now this is some sweet code, when there's more docs
# then their is code, beautiful.

import sys
from traceback import format_stack, format_exception
import pprint


class ciError(Exception):
    def __init__(self, domain, errorcode, userInfo):

        # because the error might be sent over the net by a bridge, be
        # picky and verify that the arguments are of the correct type
        # now.
        
        assert type(domain) == str
        assert type(errorcode) == int
        # verify that all the keys and values are strings
        
        for key, value in userInfo.items():
            if type(key) == str(value) == str:
                del userInfo[key]
                userInfo[str(key)] = str(value)

            
        self.domain = domain
        self.errorcode = errorcode
        self.userInfo = userInfo

    def __eq__(self, other):
	return self.domain == other.domain and self.errorcode == other.errorcode

    def __str__(self):

        info = {}
        info.update(self.userInfo)
        del info['error.reason']
        del info['error.description']
        del info['error.suggestion']
        additionalInfo = pprint.pformat(info)
        
        return ("%(error.description)s:\n"
                "    %(error.reason)s\n\n"
                "    %(error.suggestion)s\n\n"
                "    Additional Info:\n" % self.userInfo + additionalInfo)

    def __repr__(self):
        return self.userInfo.get('error.description', '')

    def __call__(self, description=None, reason=None, additionalInfo = None ):
	# return a new instance of ourselves, useful for objects using
	# the error() descriptor that want to use the intial error
	# object as a template but wish to overide some more info

	userInfo = self.userInfo.copy()
	if additionalInfo is not None:
	    userInfo.update(additionalInfo)
	
	if description is not None:
	    userInfo['error.description'] = description

	if reason is not None:
	    userInfo['error.reason'] = reason

	return ciError(self.domain, self.errorcode, userInfo)


    def _get_description(self):
        return self.userInfo.get(ErrorFactory.DESCRIPTION, "")
    description = property(_get_description)

    def _get_reason(self):
        return self.userInfo.get(ErrorFactory.REASON, "")
    reason = property(_get_reason)

    def _get_suggestion(self):
        return self.userInfo.get(ErrorFactory.SUGGESTION, "")
    suggestion = property(_get_suggestion)


    
class ErrorFactory(object):

    """Provides methods for creating exceptions for non fatal errors
    (i.e. erros that client programs should know how to recover from)

    This is a useful way to standardize our errors. These
    exceptions represent non fatal errors. I.e. when they are thrown,
    typically the user is given a suggestion (and a chance) to recover
    from the error.
    

    Each error object created by the factory has the following attributes

    int errorcode - Represents the specific error that occured. These
                    vaules are only unique within the domain . 
                    Typically each component defines it's own set of
                    error codes.  For instance the QueryService uses
                    error code 0 to represent an error while creating
                    a lead. Where as the PersistenceService might use
                    error code 0 to represent a different error
                    specific to itself. You'll need to check the
                    documentation (if any exists) for each component
                    to see what the specific error code means.

    str domain    - An arbritary string that signifies the domain of
                    the error code belongs to. The domain is typically
                    set to the name of the component that threw the
                    error, however should you need to the factory
                    provides methods to create errors with arbitrary
                    error domains.

    dict userInfo - Additional domain specific information stored in a
                    dictionary. A componet can set whatever they want
                    and users of the compoent can use the information
                    to attempt to recover from the error or express
                    more detail. For instance some components that
                    participate during QueryService.create() might
                    throw an error whith each field that it had a
                    problem with.

                    NOTE: Because errors are transfered across the
                    network you must ensure that both the keys and
                    values of the dictonary are strings

    To use the error factory you'd start by obtaining a refernce to
    it. Normally you'd bind it to one of your components, but for the
    purposes of this example we'll instantiate it directly
 
    >>> errorFactory = ErrorFactory()

    With the ErrorFactory in hand you can create a new error, using
    the newErrorWithDomain() method. You pass it the domain of the
    error, the error code and a dictionary of strings.

    >>> err = errorFactory.newErrorWithDomain('mydomain', 0,
    ...      {'error.description': 'My Generic Error'})

    Examing the err object we find
    >>> err.domain
    'mydomain'
    >>> err.errorcode
    0
    >>> err.userInfo['error.description']
    'My Generic Error'

    Now you can raise this as an exeception and hopefully the calling
    component or client will do the right thing with it.

    >>> raise err
    Traceback (most recent call last):
    ...
    ciError: My Generic Error
    
    The ErrorFactory provides a convienance function newError() that
    components can use to create errors without having to specify the
    domain. The domain will be set to name of the component that
    called the method. This is done via the binding protocol so if you
    call this method on reference to the ErrorFactory that wasn't
    bound to a component you'll get an error.

    >>> err = errorFactory.newError(0, 'My Generic Error')
    Traceback (most recent call last):
    ...
    RuntimeError: newError() can only be called when the ErrorFactory is bound to a component.


    # Q? should this exception be a fatal exception, being that it's a
    # programing error to call this on an unbound error fatory?
    

    The proper way to use newError is to define a component and bind
    the ErrorFactory to it. Also notice in this example the use of a
    constant to represent the errorcode, not technically neccesary but
    it's a good practice. Ideally you would have also documented the
    error with a comment so that users of your component would know
    it's purpose.
    
    >>> from Rambler import outlet
    >>> class MyComponent(object):
    ...   MY_ERROR_CODE = 0 
    ...   errorFactory = outlet('errorFactory')
    ...   
    ...   def someMethod(self):
    ...     err = self.erroryFactory(self.MY_ERROR_CODE,
    ...        {'description':'My Generic Error'})
    ...     raise err


    CONSTANTS and CONVENTION:
    By convention the userInfo should contain the following information

    error.description: A description of the error
    error.reason: the reason the error occured
    error.suggestion: A suggestion for recoving from the problem
    
    The keys for this are defined as constants on the errorFactory so
    you can create a dictionary like this.

    userInfo = {errorFactory.DESCRIPTION: 'Could not open image because the file exeeds the size limit',
                errorFactory.REASON: 'File exeeds the size limit',
                errorFactory.SUGGESTION: 'Upload a smaller file.'}
             


    TODO/FUTURE CONSIDERATIONS

    Most componets should use constants to document they're
    errorcodes, one thing to make this convieant is to create a python
    descriptor similar to Rambler.outlet that components can use in
    they're class descriptions. With this descriptor there would be
    no need explicitly bind the ErrorFactory and the descriptor maybe
    able to detect the use of dupplicate error codes. 
    
    For example

    class MyComponent(object):
        FILE_TO_BIG = error(0, 'Could not open image because the file exeeds the size limit', # 
                               'Could not open image', # reason
                               'Please upload a smaller image.') # recovery suggestion

        def myMethodThatThrowsAnError(self):
            raise self.FILE_TO_BIG

        def myMethodThatThrowsAnErrorWithMoreInfo(self):
            err = self.FILE_TO_BIG.withMoreInfo({'path': 'c:\\blah\\foo'})
            raise err
                               

    """
    # userInfo key contstants
    REASON='error.reason'
    DESCRIPTION='error.description'
    SUGGESTION='error.suggestion'
    TRACEBACK='error.traceback'

    # components can catch ciErrors by binding the ErrorFactory to themselves then 
    # doing a:
    # try:
    #   some code ..
    # except component.errorFactory.error

    error = ciError

    def __binding__(klass, compName, outletProvider):

        """Method called whenever a component asks the ErrorFactory to
        be bound to it. It returns a new instance of the error factory
        instantiated with the component's name as the error domain."""
        
        return klass(compName)
    __binding__ = classmethod(__binding__)


    def __init__(self, domain=None):
        self.domain = domain

    def isError(self, exception):

        """Given an exception determine wether the object supports our
        generic error interface."""

        domain = getattr(exception, 'domain', None)
        errorcode = getattr(exception, 'errorcode', None)
        userInfo = getattr(exception, 'userInfo', None)
          
        if type(domain) == str  and \
           type(errorcode) == int and \
           type(userInfo) == dict:

            # so far so good examin the userInfo dictionary
            for key, value in userInfo.items():
                if not (type(key) == type(value) == str):
                    break

            # if we get here it's good to throw
            return True
        else:
            return False

    def newError(self, errorcode, userInfo):
        if self.domain is None:
            raise RuntimeError, ("newError() can only be called when the "
                                 "ErrorFactory is bound to a component.")
        else:
            return self.newErrorWithDomain(self.domain, errorcode, userInfo)

    def newErrorWithDomain(self, domain, errorcode, userInfo):
        err = ciError(domain, errorcode, userInfo)
        return err

    # not sure if I can doc test this
    
    def unexpectedError(self):
        tb = "".join(format_exception(*sys.exc_info()))
        userInfo = {}
        userInfo[self.TRACEBACK]  = tb

        userInfo[self.REASON]     =    "An error was encountered that we were not expecting."
        userInfo[self.DESCRIPTION] = ("Unexpected error")
        userInfo[self.SUGGESTION] = ("See traceback for details. If the problem persits please notify support.")
        domain = self.domain or "Uknown"
        return self.newErrorWithDomain(domain, 0, userInfo)

    
if __name__ == "__main__":
    ef = ErrorFactory('test')
    try:
        x=1 / 0
    except:

        err = ef.unexpectedErorr()

    print err



