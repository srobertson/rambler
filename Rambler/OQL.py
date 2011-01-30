"""
Example Usage:
Can't test everything without a working EDS

  >>> tokens = OQL.parseString("select * from book where foo=1")
  >>> tokens.rootEntity
  'book'

  >>> tokens.where[1][0].fullPath
  'book.foo'
  >>> tokens.where[1][2]
  '1'
  >>> tokens.direction
  ''

  
  >>> tokens = OQL.parseString("select title from books in publisher where publisher.primaryKey = '5'")
  >>> tokens.rootEntity
  'publisher'
  >>> tokens.collectionName
  'books'

  >>> tokens = OQL.parseString("select count(*) from publisher")
  >>> countExpr = tokens.columns[0]
  >>> type(countExpr) == OQL.CountExpr
  True
  >>> countExpr.getField().name == 'count'
  True
  >>> countExpr.type == int
  True

  >>> tokens = OQL.parseString("select * from book order by author asc")
  >>> tokens.orderBy[0][0].fullPath
  'book.author'
  >>> tokens.orderBy[0][1]
  'ASC'

  >>> tokens = OQL.parseString("select * from book order by author DeSc")
  >>> tokens.orderBy[0][0].fullPath
  'book.author'
  >>> tokens.orderBy[0][1]
  'DESC'

  >>> tokens = OQL.parseString("select * from book where author is null")
  >>> tokens.where[1][0].fullPath
  'book.author'

  >>> tokens.where[1][1]
  'is'

  >>> tokens.where[1][2]
  'null'
  
  An update statement is used to set the attributes of one or more objects at once
  >>> tokens = OQL.parseString("update lead set firstName='John'")
  >>> tokens.type
  'update'
  >>> tokens.rootEntity
  'lead'

  Update statements have a list of key value changes to be made
  >>> tokens.mutations
  ('firstName', 'John')
  
  >>> tokens = OQL.parseString("UPDATE lead set firstName = 'John', lastName='Hodges', assignedTo.firstName = 'Scott' where primaryKey = '300-1'")
  >>> tokens.type
  'update'
  >>> tokens.rootEntity
  'lead'
  >>> tokens.mutations
  (('firstName','John'), ('lastName', 'Hodges'), ('assignedTo.firstName', 'Scott'))
"""




from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException,ParseResults, Forward, oneOf, quotedString, \
    ZeroOrMore, OneOrMore, restOfLine




class Column(object):
    """
    """
    EDS=None

    def __init__(self, col):

        path = col.split(".")
        self.basename = path[-1]
        
        self.pathElements = path[:-1]

        self.relativePath = col
        self.relativeParentPath = ".".join(self.pathElements)

        # This is the relativePath plus the rootentity. This value is
        # only set when the columns have been expanded.
        
        self.fullPath = None
        self.fullParentPath = None

    # Needed for set comparisons.
    def __hash__(self):
        return hash(self.relativePath)

    def __eq__(self, other):
        return (type(self) == type(other) and
            self.relativePath == other.relativePath)

    def isEquivalent(self, other):
        
        """Return true if the other column is identical to this column
        or if this column would select that column because of the wild
        card character."""

        if self.basename == "*" or other.basename == "*":

            # This column uses a wild card, so if both columns have
            # the same parent then they're equivalent.
            
            return self.relativeParentPath == other.relativeParentPath
        else:
            return self.relativePath == other.relativePath


    def expandColumns(self, namespaces):

        """If the column is a wildcard expand it to a list of all the
        columns defined in the entity descriptor."""

        self._setFullPath(namespaces)
        
        if self.EDS is not None and self.basename == "*":
            columns = []
            eName = self._traverse()
            # Return all non relational fields.
            for field in self.EDS.getFields(eName):
                if not field.isRelation():
                    columnName = ".".join(self.pathElements + [field.name])
                    column = Column(columnName)
                    column._setFullPath(namespaces)
                    columns.append(column)
            return columns
        
        else:
            # Sigh, there's no client side EDS yet, so we can't expand the
            # columns here.

            return [self]


    def getField(self):
        """Returns a storage managed fields for this column starting
        at the base entity."""

        if self.basename == "*":
            # A wild card should never be exposed, since it should
            # have been expanded during the parsing process.
            raise ValueError, "Can't get the field for a wildcard"

        else:
            eName = self._traverse()
            field = self.EDS.getField(eName, self.basename)
            if field.isRelation():
                raise QuerryError("Relational fields can not be selected or compared.")
            else:
                return field

    def _traverse(self):
        # Traverse to the end of the realchain, for example
        # assignedTo.contact means we have to go from the lead
        # object to a user object to a contact

        eName = self.fullPathElements[0]
        for field in self.fullPathElements[1:-1]:
            if field == '*':
                # Can't traverse over the wildcard
                break
                
            field =self.EDS.getField(eName,field)
            assert field.isRelation(), "Can't traverse over %s " \
                   "because it's not a relation." % field.name
            eName = self.EDS.getName(field.type)
        return eName

    def _setFullPath(self, namespaces):

        defaultNamespace = namespaces['']
        if self.relativeParentPath != '' and self.relativeParentPath in namespaces:
            # Check to see if there's a namespace defined for the
            # first element in the path, if not then the fullpath uses
            # the default namespace.
            namespace = namespaces[self.relativeParentPath]
            
            self.fullParentPath = namespace
            self.fullPath = self.relativePath
            self.relativePath = self.relativePath[len(self.fullParentPath) + 1:]
        else:
            self.fullParentPath = ".".join([defaultNamespace] + self.pathElements)
            self.fullPath = self.fullParentPath + '.' + self.basename
            
        self.fullPathElements = self.fullPath.split('.')
        

    def do_column(klass, str, loc, toks):
        
        # Gosh it would be neet to verify that the column is an actual
        # column and to convert "*" to a list of the actual columns
        # here. But I can't figure out how to properly get the
        # rootEntitiy to this object.
        
        return klass(toks[0])
    do_column = classmethod(do_column)

class CountExpr(object):
    """ """
    def __init__(self, instr, loc, toks):
        self.type = int
        assert type(toks[0]) == Column
        self.column = toks[0]

    ## Coulmn interfaces ##

    # paths don't make sense for expresions but the OQL2SQL parse uses
    # it to match the results from the SQL back to the Rambler name.
    
    fullPath = 'count' 
    
    def expandColumns(self, namespaces):
        
        # this set's the full path of the underlying column, not sure
        # if it's needed or not
        
        self.column.expandColumns(namespaces)
        return [self]

    def getField(self):
        """Return an int field"""

        # CountExpr implements the Field interface, so returning it
        # should make everyone think it's an int
        
        return self



    ##  Field interface ##
        
    name = 'count'

 
    def isRelation(self):
        return False

    def getRelationName(self):
        raise ValueError("Field is not a relation")
    relationName = property(getRelationName)
    
    def getRole(self):
        raise ValueError("Field is not a relation")
    role = property(getRole)

    # delegate all methods and attributes we don't implement directly
    # to our column

    def __getattr__(self, attr):
        return getattr(self.column, attr)

            




def convertWildCards(str,loc,toks):

    # Expands wild cards to match all the columns while eliminating
    # dupes. 

    # TODO: I'm not doing it now, but it might be best to return
    # fields, or some how put the field object on the column object,
    # since we've already gone through the trouble of looking it
    # up. Might enhance performance.
    
    expandedCols = set()
    rootEntity = toks.rootEntity
    collectionName = toks.collectionName

    namespaces = {}
    if collectionName:
        namespaces[''] = rootEntity + '.' + collectionName
        namespaces[rootEntity] = rootEntity
    else:
        namespaces[''] = rootEntity
    
    for column in toks.columns:
        expandedCols.update(column.expandColumns(namespaces))

    toks['columns'] = list(expandedCols)

    if toks.orderBy:

        for sortDescriptor in toks.orderBy:
            col = sortDescriptor[0]
            col._setFullPath(namespaces)

    setWherePaths(namespaces, toks.where)


def setWherePaths(namespaces, where):

    for token in where:
        ttype = type(token)
        if issubclass(ttype, basestring):
            pass

        elif ttype == Column:
            token._setFullPath(namespaces)
        elif ttype == ParseResults:
            # Looks like a subclause, recurse into it
            setWherePaths(namespaces, token)
	else:
	    raise RuntimeError, "Unknown token type %s" % ttype





# define SQL tokens
selectStmt = Forward()
selectToken = CaselessLiteral( "select" ).setResultsName("type")
deleteToken = CaselessLiteral( "delete" ).setResultsName("type")
updateToken = CaselessLiteral('update').setResultsName('type')
fromToken   = CaselessLiteral( "from" )

# Update 
setToken    = CaselessLiteral("set")
setOpToken  = Literal('=').suppress()

ident          = Word( alphas, alphanums + "_$.*" ).setName("identifier")

columnName     = ('*' | delimitedList( ident, ".", combine=True )).\
                 setParseAction(Column.do_column)

expr = (CaselessLiteral("count(").suppress() + columnName + Literal(')').suppress()).setParseAction(CountExpr)

columnNameList = Group(delimitedList(
    expr |
    columnName ))
                        
#tableName      = Upcase( delimitedList( ident, ".", combine=True ) )
                        
rootEntity      = Word(alphas, alphanums).setResultsName('rootEntity') #delimitedList( ident, ".", combine=True )
in_ = CaselessLiteral("in")
collection = ident.setResultsName("collectionName") + in_ + rootEntity
#fromClause = (ident + in_ + ident) | ident
#tableNameList  = Group( delimitedList( tableName ) )

whereExpression = Forward()
and_ = CaselessLiteral("and")
or_ = CaselessLiteral("or")
between_ = CaselessLiteral("between")

E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge like ilike", caseless=True)
arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                         ( "." + Word(nums) ) ) + 
            Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) + 
            Optional( E + Optional("+") + Word(nums) ) )
bool =  CaselessLiteral("true") | CaselessLiteral("false")

columnRval = "None" | bool | realNum | intNum | quotedString | columnName # need to add support for alg expressions
assignment  = (columnName + setOpToken + columnRval)

whereCondition = Group(
    ( columnName + binop + columnRval ) |
    ( columnName + in_ + "(" + delimitedList( columnRval, ',', combine=True) + ")" ) |
    ( columnName + in_ + "(" + selectStmt + ")" ) |
    ( columnName + between_ + columnRval + and_ + columnRval ) |
    ( "(" + whereExpression + ")" ).setResultsName("whereExpr") |
    ( columnName + CaselessLiteral('is') + CaselessLiteral('null'))
    )
whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

orderByToken = CaselessLiteral("order by")
limitToken = CaselessLiteral("limit")
offsetToken = CaselessLiteral("offset")
asc_ = CaselessLiteral('ASC')
desc_ = CaselessLiteral('DESC')


# define the grammar


selectStmt      << (selectToken + 
                    (columnNameList.setResultsName( "columns" ) + 
                    fromToken +
                    (collection | rootEntity)) + 
                    Optional( Group( CaselessLiteral("where") + whereExpression ).setResultsName("where") , "" ) +
                    Optional(orderByToken.suppress()  +
			     delimitedList(Group(columnName + Optional(asc_ | desc_)))
			     .setResultsName('direction')).setResultsName("orderBy") +

                    Optional(limitToken.suppress() + Word(nums)).setResultsName("limit") +

                    Optional(offsetToken.suppress() + Word(nums)).setResultsName("offset") 
                    
                    ).setParseAction(convertWildCards)
                    

deleteStmt = (deleteToken +
              fromToken +
              rootEntity +
              Optional( Group( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("where")
              ).setParseAction(convertWildCards) # We only really need to run convertWildCards to make sure our paths are set up properly on our columns.



updateStmt = (updateToken + rootEntity + setToken +
              delimitedList( assignment, ',').setResultsName('mutations') +
                Optional( Group( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("where")
              ).setParseAction(convertWildCards) # We only really need to run convertWildCards to make sure our paths are set up properly on our columns.

              
OQL = selectStmt | deleteStmt | updateStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
OQL.ignore( oracleSqlComment )


# python hack so in other modules you only need to do
# import OQL
# OQL.parseString(...)

parseString = OQL.parseString



def makeFilterFunc(parsedQuery):
    statement = ['lambda %s:' % parsedQuery.rootEntity]
    
    try:
        res = __convertToken(parsedQuery.where[1:])
    except IndexError:
        res = []

    statement.extend(res)
    if len(statement) == 1:
        # no where clause so always return True
        statement.append("True")

    statement = " ".join(statement)
    
    return eval(statement)


def filterListWithOQL(l, query):
    #TODO: After converting the where clause to Predicates this method
    #should be replaced.

    """Given a list and either a string containing an OQL Query or a
    preparsedQuery, return a list of objects that match the supplied Query.

    For example, imagine that you have the following classs that
    represesnst an Employee at a company.

    
    >>> class Employee(object):
    ...  def __init__(self, name, salary, department):
    ...    self.name       = name
    ...    self.salary     = salary
    ...    self.department = department

    >>> employeeList = [
    ...  Employee('John',65000,'Tech'),
    ...  Employee('Frank',100000,'Tech'),
    ...  Employee('Sam',80000,'Tech'),
    ...  Employee('Mike',65000,'Marketing'),
    ...  Employee('Jan',85000,'Marketing'),
    ...  Employee('Sue',35000,'Accounting'),
    ...  Employee('Sally',120000,'Tech'),
    ...  Employee('Fred',35000,'Warehouse'),
    ...  ]

    You can retreive a list of all employees who make more than
    $100,000 a year like this.

    >>> oql = "SELECT * FROM employee WHERE salary > 100000"
    >>> len(filterListWithOQL(employeeList, oql))
    1

    Or all employees who make at least 100,000 or more, also note you
    can preparse the Query if you need to use it multiple times.
    
    >>> oql = "SELECT * FROM employee WHERE salary >= 100000"
    >>> parsedQuery = OQL.parseString(oql)
    >>> len(filterListWithOQL(employeeList, parsedQuery))
    2

    Employees that make over 100,000 or are in the Marketing
    department.
    
    
    >>> oql = "SELECT * FROM employee WHERE salary > 100000 or department = 'Marketing'"
    >>> len(filterListWithOQL(employeeList, oql))
    3


    Employees that make over 100,000 or in the Marketing department
    but only if they make less than 85,000
    
    >>> oql = "SELECT * FROM employee WHERE salary > 100000 or (department = 'Marketing' and salary < 85000)"
    >>> len(filterListWithOQL(employeeList, oql))
    2


    Note this test doesn't pass because OQL doesn't properly pares the
    in statement for some reason.
    
    oql = "SELECT * FROM employee WHERE department in ('Marketing', 'Warehouse')"
    parsedQuery = OQL.parseString(oql)
    # Sally,  Mike, Jan
    assert  len(filterListWithOQL(employeeList, parsedQuery)) == 3
    
    Select all the epmloyees
    >>> oql = "SELECT primaryKey from employee"
    >>> len(filterListWithOQL(employeeList, oql))
    8
    

    """

    # this whole routine basically generates a function on the fly
    # that can be passed to python's built-in filter() method. The
    # function takes a single object and simply applies the where
    # clause of an oql stament and returns true or false if the object
    # matches the criteria or not.

    # For examlp an oql statment of: 
    # select * from epmloyee where department = 'Marketing' and (salary < 85000 or name = 'Sally')
    # get translated into this python function. We actually use 
    
    # def filterFunc(employee):
    #   return employee.department == 'Marketing' and (employee.salary < 85000 or employee.name = 'Sally')
    #
    # We acutally use lambda but you get the idea.
 
    if isinstance(query, basestring):
        query = parseString(query)
    
    filterFunc = makeFilterFunc(query)
    return filter(filterFunc, l)

def __convertToken(tokens):
    """Converts a token to a python expression, and places it into the
    statement list."""

    for token in tokens:
        tokenType = type(token)
        if tokenType == ParseResults:
            for result in __convertToken(token):
                yield result

        elif tokenType == Column:
            yield token.fullPath
        else:
            if token == '=':
                token = '=='

            yield token


# TODO: This should be done by defining EDS as an outlet on the Column object
def setEDS(EDS):
    
    # kludge to get the EDS to the Column class without making it
    # aware of the component Registry
    
    Column.EDS = EDS


if __name__ == "__main__":
    import doctest
    import OQL

    doctest.testmod()


    t=OQL.parseString(u"SELECT primaryKey FROM ciuser WHERE username = 'srobertson' order by username, bob DESC")
    
    t = OQL.parseString("select * from foo where name = 'scott''s' and x = 10 ")
    pass



# ParsedQuery
#  type: str(create|select|update|delete)
#  rootEntity: str (select only)
#  collectionName: str
#  columns: select only, list of keypaths to return values for
#  where: Predicate object (either compound predicate or comparison predicate)
