from Rambler import Server
from Rambler.ciHomeBase import Entity
from omniORB import CORBA, ir_idl
from epo import _tc_EntityInfo, _tc_EntityInfos
import mx.DateTime
from DateTime.DateTime import DateTimeType
from DateTypes import Date, Timestamp
from Money import Money

# Why is this imported here?
#from _GlobalIDL import roleList

mxDateTimeType = type(mx.DateTime.DateTime(0))
DateType = type(Date())
TimestampType = type(Timestamp())

def _createSequenceTC(seq):

    """Construct's a type code for the given sequence. It assumes that
    all the elments in the list are the same."""

    if len(seq) == 0:
        # We don't know what's  in the list so we assume it's a list of entities
        #return Server.orb.create_sequence_tc(0, _tc_EntityInfo)
        return _tc_EntityInfos

    typeCodeFunc = typesDict.get(type(seq[0]))
    if typeCodeFunc is None:
        # No converter registered, assume that it's a sequence of EntityInfo
        return _tc_EntityInfos

    typeCode = typeCodeFunc(seq[0])
    return Server.orb.create_sequence_tc(0, typeCode)


# This is a list of functions that will convert a given value. 
typesDict = {int            : lambda value: CORBA._tc_ushort,
             str            : lambda value: CORBA._tc_string,
             float          : lambda value: CORBA._tc_float,
             unicode        : lambda value: CORBA._tc_wstring,
             bool           : lambda value: CORBA._tc_boolean,
             list           : _createSequenceTC,
             tuple          : _createSequenceTC,
             mxDateTimeType : lambda value: CORBA._tc_string,
             DateType       : lambda value: CORBA._tc_string,
             TimestampType  : lambda value: CORBA._tc_string,
             Money          : lambda value: CORBA._tc_string}


def getCorbaType(pType):
    """
    Returns the corba typecode for the given
    python type/class
    """
    if issubclass(pType, Entity):
        corbaType = _tc_EntityInfo
    else:
        typeCodeFunc =typesDict.get(pType)
        if typeCodeFunc is None:
            corbaType = CORBA.TypeCode(pType)
        else:
            corbaType = typeCodeFunc(pType)

    return corbaType
       
