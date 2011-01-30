from xml.sax import handler

class DefaultHandler(handler.EntityResolver, handler.DTDHandler,
                     handler.ContentHandler, handler.ErrorHandler, object):
        """Default base class for SAX2 event handlers. Implements empty methods
        for all callback methods, which can be overridden by application
            implementors. Replaces the deprecated SAX1 HandlerBase class."""

class CompoundHandler(DefaultHandler):
    """Chains multiple handlers together."""
    
    def __init__(self):
        self._handlers = []

    def addHandler(self, handler):
        self._handlers.append(handler)

    def startElement(self, name, attrs):
        for handler in self._handlers:
            handler.startElement(name, attrs)

    def characters(self, ch):
        for handler in self._handlers:
            handler.characters(ch)

    def endElement(self, name):
        for handler in self._handlers:
            handler.endElement(name)
