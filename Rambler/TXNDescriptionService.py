from handlers import DefaultHandler


class TXNDescriptionService(object):
    """
    The Transaction Description Service is responsible for
    determining transaction policy for various objects in
    the system.  The service offers both coarse and fine
    grain control over objects, allowing you to set the policy
    for all methods on an object, or for each method
    individually.
    
    
    >>> tdsh = TDSHandler()
    >>> txnd = TXNDescriptionService()

    Fake the binding that normally occurs via the Component
    Binder
    
    >>> tdsh.txnDescService = txnd
    
    >>> from xml.sax import parseString
    >>> xml = '''
    ... <assembly-descriptor>
    ...  <container-transaction>
    ...   <method>
    ...    <ejb-name>AnObject</ejb-name>
    ...    <method-name>*</method-name>
    ...   </method>
    ...   <trans-attribute>Supports</trans-attribute>
    ...  </container-transaction>
    ...  <container-transaction>
    ...   <method>
    ...    <ejb-name>AnObject</ejb-name>
    ...    <method-name>noTransMethod</method-name>
    ...   </method>
    ...   <trans-attribute>Never</trans-attribute>
    ...  </container-transaction>
    ...  <container-transaction>
    ...   <method>
    ...    <ejb-name>AnObject</ejb-name>
    ...    <method-name>requiresMethod</method-name>
    ...   </method>
    ...   <trans-attribute>Required</trans-attribute>
    ...  </container-transaction>
    ...  <container-transaction>
    ...   <method>
    ...    <ejb-name>AnObject</ejb-name>
    ...    <method-name>mandatoryMethod</method-name>
    ...   </method>
    ...   <trans-attribute>Mandatory</trans-attribute>
    ...  </container-transaction>
    ...  <container-transaction>
    ...   <method>
    ...    <ejb-name>AnotherObject</ejb-name>
    ...    <method-name>supportsMethod</method-name>
    ...   </method>
    ...   <trans-attribute>Supports</trans-attribute>
    ...  </container-transaction>
    ... </assembly-descriptor>
    ... '''
    
    >>> parseString(xml, tdsh)

    >>> txnd.getTransAttribute('AnObject', 'noTransMethod')
    5

    >>> txnd.getTransAttribute('AnObject', 'requiresMethod')
    2

    >>> txnd.getTransAttribute('AnObject', 'mandatoryMethod')
    4

    Confirm that the default handling of unspecified methods
    on objects with a '*' is set 
    
    >>> txnd.getTransAttribute('AnObject', 'bar')
    1

    >>> txnd.getTransAttribute('AnotherObject', 'supportsMethod')
    1

    Confirm that the default handling for unspecified methods
    on objecst with no '*' is Mandatory
    
    >>> txnd.getTransAttribute('AnotherObject', 'foo')
    4
    """

    

    NotSupported = 0
    Supports = 1
    Required = 2
    RequiresNew = 3
    Mandatory = 4
    Never = 5
    
    def __init__(self):
        self._objects = {}
    
    def getTransAttribute(self, name, methodName):
        """Returns how the container/server will manage the
        transaction for the given method.

        Possible values are:


        Required - If no transaction has been specified the server
        will start one before invoking the method and end the
        transaction imediatly after.
        
        RequiresNew - If a trnsaction is in progress the server will
        suspend and start a new one before invoking the
        method. Afterward, the server will commit the transaction it
        started and restore the previous transaction if it existed.

        Mandatory - The server will raise TRANSACTION_REQUIRED if the
        method was invoked without a transaction.

        Never - Raises TransactionForbidden if a transaction is in progress.

        If nothing is known about a particular name/method pair, this
        method will return Mandatory."""
        # Make EPO Object name case insensitive
        name = name.lower()
            
        data = self._objects.get(name)
        mode = None
        if data:
            mode = data.get(methodName)
            if not mode:
                mode = data.get("*")
            if not mode:
                mode = self.Mandatory

        if not mode:
            mode = self.Mandatory

        return mode
            

    def setTransAttribute(self, name, method, mode):        
        """Set's how the server manages the transaction before
        invoking a given method. You can set this on a method by
        method basis for each Enterprise Python Object, and/or set the
        default mode for all methods of a given EPO by specifying * as
        the value of method."""
        # Make EPO Object name case insensitive
        name = name.lower()

        data = self._objects.get(name, {})
        data[method] = mode
        self._objects[name] = data
        



class TDSHandler(DefaultHandler):
    
    """Observes the parsing of the xml descriptor, and initializes the
    TDS accordingly."""
    def __init__(self):
        self._data = []
        self._ignoreData = True
    
    def startElement(self, name, attrs):
        if name == "assembly-descriptor":
            self._ignoreData = False
            
        if name == "container-transaction":
            self._entityName = ""
            self._methodName = ""
            self._transMode = ""

    def characters(self, ch):
        if self._ignoreData:
            return
        
        self._data.append(ch)

    def endElement(self, name):
        if name == "assembly-descriptor":
            self._ignoreData = True
        elif self._ignoreData:
            return
        
        if name == "container-transaction":
            tm = self._transMode.lower()
            if tm == "notsupported":
                transMode = self.txnDescService.NotSupported
            elif tm == "supports":
                transMode = self.txnDescService.Supports
            elif tm == "required":
                transMode = self.txnDescService.Required
            elif tm == "requiresnew":
                transMode = self.txnDescService.RequiresNew
            elif tm == "mandatory":
                transMode = self.txnDescService.Mandatory
            elif tm == "never":
                transMode = self.txnDescService.Never
            else:
                raise ValueError("Unknown trans-attribute: %s, for ejb-name %s, method-name %s" % (self._transMode,
                                                                                                   self._entityName,
                                                                                                   self._methodName))
            self.txnDescService.setTransAttribute(self._entityName,
                                                    self._methodName,
                                                    transMode)
        else:
            data = "".join(self._data).strip()
            if name == "ejb-name":
                self._entityName = data

            elif name == "method-name":
                self._methodName = data

            elif name == "trans-attribute":
                self._transMode = data
            
        self._data = []

