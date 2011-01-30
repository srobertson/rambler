from Rambler import outlet


from UserDict import UserDict
import time


from pickle import loads
from cgi import parse_qs
from datetime import datetime

from Money import Money


reportsDict = {}

DateTimeType = datetime

def createSequenceTC(seq):

    """Construct's a type code for the given sequence. It assumes that
    all the elments in the list are the same."""

    if len(seq) == 0:
        # We don't know what's  in the list so we assume it's a list of entities
        return _tc_EntityInfos

    typeCodeFunc = typesDict.get(type(seq[0]))
    if typeCodeFunc is None:
        # No converter registered, assume that it's a sequence of EntityInfo
        return _tc_EntityInfos

    typeCode = typeCodeFunc(seq[0])
    # Server global is set in ReportHome.assembled
    return Server.orb.create_sequence_tc(0, typeCode)


# This is a list of functions that will convert a given value. 
typesDict = {int     : lambda value: CORBA._tc_ulong,
             long    : lambda value: CORBA._tc_ulong,
             str     : lambda value: CORBA._tc_string,
             float   : lambda value: CORBA._tc_float,
             unicode : lambda value: CORBA._tc_wstring,
             bool    : lambda value: CORBA._tc_ushort,
             list : createSequenceTC,
             tuple : createSequenceTC,
             datetime: lambda value: CORBA._tc_string,
             DateTimeType: lambda value: CORBA._tc_string}

class ReportHome(object):
    log = outlet("LogService")

    _reports = {}

    def assembled(klass):

        # Nasty hacks, but this whole module needs to be reimplemented
        
        Row.eds = klass.eds
        Row.corbaBridge = klass.corbaBridge
        Table.corbaBridge = klass.corbaBridge
        global Server
        Server = klass.corbaBridge
    assembled=classmethod(assembled)
    
        
    def registerReport(klass, reportName, reportClass):
        if klass._reports.has_key(reportName):
            raise ValueError, 'Report "%s" has already been registered' % reportName
        klass._reports[reportName] = reportClass
    registerReport = classmethod(registerReport)
     

    def runReportString(klass, qs):

        args = dict(parse_qs(qs))

        for key, value in args.items():
            if len(value) == 1:
                args[key] = value[0]

        rp = args['report']

        del args['report']
        if not klass._reports.has_key(rp):
            raise ValueError, 'Report "%s" has not been registered' % rp
        r = klass._reports[rp]()
        klass.log.debug("Starting to run.")
        start = time.time()
        res = r.run(**args)
        klass.log.debug("Running report %s too %s seconds to run." % (rp, (time.time() - start)))
                  
        return klass(res)
    runReportString = classmethod(runReportString)

    def runReport(klass, reportName, args=None, **kw):

        if type(args) == str and args != "":

            # Args should unpickle to a dictionary
            args = loads(args)
            if type(args) == dict:
                kw.update(args)
            
        if not klass._reports.has_key(reportName):
            raise ValueError, 'Report "%s" has not been registered' % reportName
        r = klass._reports[reportName]()
        res = r.run(**kw)
        return klass(res)
    runReport=classmethod(runReport)

    def getReports(klass):
        return klass._reports.keys()
    getReports=classmethod(getReports)

    def __init__(self, resultSet):
        # delegate all instance method access to the resultSet
        self.resultSet = resultSet

    def __getattr__(self, name):
        return getattr(self.resultSet, name)
        

        

        
####################################################################

class Recordset(object):
    """ A record set is the result of query """

    interface = "Recordset"
    
    def __init__(self):
        self._tables = {}

    def addTable(self, name, in_table):
        self._tables[name] = in_table

    def fetchOne(self, name):
        return self._tables[name].fetchOne()

    def fetchMany(self, name, num):
        # Returning the list as a tuple prevents the CORBA wrapper
        # from attempting to convert the list to a sequence.

        #return tuple(self._tables[name].fetchMany(num))
        return self._tables[name].fetchMany(num)

    def hasMoreElements(self, name):
        return self._tables[name].hasMoreElements()

    def getTables(self):
        return self._tables.values()

    def getTableNames(self):
        return tuple(self._tables.keys())

    def getCount(self, name):
        return len(self._tables[name])

    def encodeWithCoder(self, coder):
        #TODO: 
        return coder.serializeDict(self._tables)



class Row(UserDict, object):
    # These attributes are set on the Row class by ReportHome when it's assembled
    
    eds=None
    corbaBridge=None
    def __init__(self, **kw):
        UserDict.__init__(self, kw)

        for k, v in kw.items():
            if  hasattr(v, "_get_primaryKey"):
                # Convert Entities into EntityInfo
                self.data[k] = EntityInfo(v._get_home()._get_homeId(),
                                          self.eds.getName(type(v)),
                                          v._get_primaryKey())
            
            elif v is None:
                self.data[k] = EntityInfo("","", "")
  
            elif type(v) in (DateTimeType, datetime):
                self.data[k] = str(v)

            elif type(v) == Money:
                self.data[k] = str(v)
                
            elif type(v) in (list, tuple) and \
                     len(v) and \
                     hasattr(v[0], "_get_primaryKey"):
                # Convert list of entities, and burn up CPU time
                entityInfos = []
                for entity in v:
                    ei =  EntityInfo(entity._get_home()._get_homeId(),
                                     self.eds.getName(type(entity)),
                                     entity._get_primaryKey())
                    entityInfos.append(ei)

                self.data[k] = entityInfos

                    
            

    def __getattr__(self, key):
        return self.data[key]

    def getTypeCode(self):
        """Builds a TypeCode on the fly for this row."""
        mbmrs = []
        for k, value in self.items():
            typeCodeFunc =typesDict.get(type(value))
            if typeCodeFunc is None:
                corbaType = _tc_EntityInfo
            else:
                corbaType = typeCodeFunc(value)
            mbmrs.append(StructMember(k, corbaType, None))
    

        return self.corbaBridge.orb.create_struct_tc(
            "IDL:codeit.com/Row:1.0", 
            "Row",  
            mbmrs)
    def encodeWithCoder(self, coder):
        return coder.serializeDict(self)
 
class Table:
    def __init__(self, rows=None, typeCode=None):
        self._rows = []
        self._pos = 0
        self._typeCodeForRow = None
        self._typeCodeForRows = None

        if typeCode is not None:
            self._set_rowStruct(typeCode)
        
        if rows:
            for row in rows:
                self.addRow(row)

    def hasMoreElements(self):
        return self._pos < len(self._rows)

    def addRow(self, row):
        if self._typeCodeForRow is None:
            self._set_rowStruct(row)
                
        self._rows.append(row)

    def fetchOne(self):
        return self._getNext(1, multiple=False)
      
    def fetchMany(self, num):
        return self._getNext(num, multiple=True)

    ####################################################################

    def _set_rowStruct(self, RowOrTypeCode):
        if type(RowOrTypeCode) == Row: 
            typeCode = RowOrTypeCode.getTypeCode()
        else:
            typeCode = RowOrTypeCode
            
        self._typeCodeForRow = typeCode
        self._typeCodeForRows = self.corbaBridge.orb.create_sequence_tc(0, typeCode)  
                    
    
    def _get_rowStruct(self):
        return self.typeCodeForRow
       
    def _getNext(self, num_to_fetch, multiple=True):
        if self.hasMoreElements():
            if num_to_fetch > 0:
                elements = self._rows[self._pos:self._pos+num_to_fetch]
                self._pos += num_to_fetch
            else: # Return everything
                elements = self._rows[self._pos:]
                self._pos = len(self._rows)
                
            # wrap the object
            # return elements

            
            if multiple:
                return CORBA.Any(self._typeCodeForRows, elements)
                #return [CORBA.Any(self._struct, x) for x in elements]
            
            elif not multiple and len(elements) == 1:
                return CORBA.Any(self._typeCodeForRow, elements[0])
            else:
                raise RuntimeError, "Requsted a single item, but there's more than one result."
        else:

            # Andy, we should probably have a different exception,
            # such as StopIteration. 
            raise StopIteration

    # define the standard iterator interface so that 
    # local Python code can use this if they want to, for
    # example PageTemplates...
    def __iter__(self):
        return self
    
    def next(self):
        if self.hasMoreElements():
            return self.fetchOne()
        else:
            # this must be a Stop error if using the
            # standard python iteration procedure
            raise StopIteration

    def encodeWithCoder(self, coder):
        return coder.serializeList(self._rows)



if __name__=='__main__':
    importIDL("./idl/epo.idl")
    orb = CORBA.ORB_init()
    poa = orb.resolve_initial_references("RootPOA")
    poa._get_the_POAManager().activate()

    r = Recordset()._this()
    w = Row(foo='bar')
    t = Table([w,])._this()
    r.addTable(t)

    for tbl in r.getTables():
        row = tbl.getNext().value()
        print row.foo
