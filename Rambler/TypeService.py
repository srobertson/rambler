
class TypeService(object):
    
    """Provides the ability to register converters for different
    domains to and from Rambler.

    A 'domain' in this case is any system outside of rambler. For
    example a remote client might send a request via CORBA to a
    Rambler application. The corba bridge recieves this request and
    uses the typeservice to convert the values into types that Rambler
    components understand. While handling these request some other
    component, say the persistence service might need to talk to
    postgres. In doing so the persistence service might have to
    convert a datatime object to a string that thep postgres service
    understands. In this case the Persistence service would then
    lookup the appropriate converter to format the datetime object to
    the right string.


    
    +--------+    +-------+    +---------+    +------------+
    | client | -> | Corba | -> | Rambler | -> | PostgreSQL |
    +--------+    +-------+    +---------+    +------------+

    """
