from Rambler import OQL, outlet
from cgi import parse_qs

import inspect
import traceback
import sys
import pyparsing

class ReportQueryError(Exception):
    pass

class QueryError(Exception):
    pass

class QueryService(object):
    """The QueryService provides a method of querying/modifying
    different data sources for tabular data.

    It's what makes rambler scriptiable.
    
    A data source is any object that chooses to respond to a
    particular query and return 0 or more rows. For instance you could
    create an object that turns a csv formated file into a datasource
    so that othe objects could work with this file without having to
    know that it's stored as a csv file.


    Features:
    * Objects can participate in a generic create, 

    * Objects can rewrite queries on the fly/transparently to clients
      to do things like enforce security

    * Objects can provide computed views of data


    What's the difference between the persistance service and the query service?
    
    The PersitanceService primary purpose is to provide objects with a
    fine grain interface (Entities) with a mechanism to persist
    they're data whenever they're attributes are changed. Entities
    represent the core business data in the system.

    The QueryService on the other hand provides a course grained
    scriptable interface to local and remote objects that need to work
    with large ammounts of data in a generic fashion.

    """
    provides = 'Query'

    compReg = outlet('ComponentRegistry')
    relationRegistry = outlet("RelationRegistryService")
    configService    = outlet("ConfigService")
    log              = outlet("LogService")
    errorFactory     = outlet("ErrorFactory")

    createObservers = []
    dataSources = {}

    COULD_NOT_CREATE = 0
    COULD_NOT_SET_VALUES = 1

    # __init__ and __getattr__ allow the resultSet to appear as in
    # istance of QueryService, even though intstance delegate they're
    # methods to the resultset. It's dumb, but until we have away to
    # introspect interfaces the CORBABridge has to look at the type
    # returned from a method rather and guess what it should be.
    def __init__(self, resultSet):
        # delegate all instance method access to the resultSet
        self.resultSet = resultSet

    def __getattr__(self, name):
        return getattr(self.resultSet, name)



    def testableQS():
        """Class method that returns a QueryService assembled with
        it's dependancyes so it can be properly tested."""
        from Rambler.LoggingExtensions  import LogService
        from Rambler.CompBinder import CompBinder
        from Rambler.ciRelationService import RelationRegistry, Role
        from Rambler.ciConfigService import ciConfigService
        from Rambler.EntityDescriptionService import EntityDescriptionService
        from Rambler.EventChannel import EventChannel
        from Rambler.ErrorFactory import ErrorFactory
        import sys
        
        compReg = CompBinder()
        compReg.addComponent("LogService", LogService())
        compReg.addComponent("EventService", EventChannel())
        compReg.addComponent("QueryService", QueryService)
        compReg.addComponent("EntityDescriptionService", EntityDescriptionService())
        compReg.addComponent("RelationRegistryService", RelationRegistry())
        compReg.addComponent("ConfigService", ciConfigService())
        compReg.addComponent("ErrorFactory", ErrorFactory())
        compReg.bind()
        # handy method to deremine if we got all the dependancies right
        if len(compReg.needsBinding):
            raise RuntimeError, "Server could not start because\n" +\
                  compReg.analyzeFailures()

        # boy oh boy, setting up entities for testing purposes is a
        # pain

        class FakeHome(object):
            homeId = "Fake"
        sys.modules['__main__'].FakeHome = FakeHome

        eds = compReg.get("EntityDescriptionService")
        eds.addEntityInfo("book", None, None,'__main__.FakeHome', [], [])
        eds.addEntityInfo("author", None, None,'__main__.FakeHome', [], [])

        # now add a realationship for testing purposes
        role1 = Role('one','book')
        role1.field = 'books'
        role2 = Role('many','author')
        role2.field = 'author'
        

        rr = compReg.get('RelationRegistryService')        
        rr.addRelation('author_books', role1,role2)


        return compReg.get("QueryService")
    testableQS = staticmethod(testableQS)
    

    def notifyOnCreate(klass, observer):
        klass.createObservers.append(observer)
    notifyOnCreate = classmethod(notifyOnCreate)
    
    def create(klass, entityName, attributes):
        if type(attributes) != dict:
            attributes = dict(attributes)

        # information that needs to be passed back to an observers
        # postCreate() method
        
        contextInfo = {} 
        
        for observer in klass.createObservers:
            if hasattr(observer, 'preCreate'):
                contextInfo[observer] = observer.preCreate(entityName, attributes)

        # can't use the eds, for some silly reason we only store the
        # home class not the instance of the home object, oh well, the
        # whole entity class/home bit is due for a major overhaul.

        home  = klass.compReg.lookup(entityName + 'Home')

        # now inspect the create method on the home
        (args, varargs, varkw, defaults) = inspect.getargspec(home.create)
        
        # args is list of arguments the function is expecting, we want
        # to pop each of these out of the passed in attributes into a
        # new dictionary which we'll then apply to the create statement

        constructorargs = {}

        # defaults is a tuple of values to apply when the argument
        # wasn't specified. If the tuple has N elments, they corespend
        # to the last N elments listed in args.

        if defaults is None:
            # there are no defaults so defaultStartAtIndex will alway be bigger
            # than the index
            defaultStartAtIndex = len(args)
        else:
            defaultsStartAtIndex = len(args) - len(defaults)

        index = 1
        # first argument is self, so we skip it
        for arg in args[1:]:
            try:
                constructorargs[arg] = attributes.pop(arg)
            except KeyError:
                # if the index is greater than defaultStartAtIndex, there's a
                # default value for it so it's safe to ignore the
                # error
                if index < defaultsStartAtIndex:
                    raise QueryError('Missing required argument %s' % arg)

            index += 1


        entity = home.create(**constructorargs)

        # with our newly created entity in hand it's time to set it's
        # properties and the properties of any related objects
        
        try:
            entity.setValuesForKeysWithDictionary(attributes)
        except Exception, e:
            if klass.errorFactory.isError(e):
                # If it's one of our errors, and we hav observers,
                # pass it off to the oberservers error handling method
                # and allow it to do anything it needs to be done to
                # alter the object or the context.
                handled = False
                for observer in klass.createObservers:
                    context = contextInfo.get(observer, None)
                    if hasattr(observer, 'handleCreateError'):
                        handled = observer.handleCreateError(entityName, entity, context, e) or handled

                if not handled:
                    # We found no observer that resolved the error.  Raise it instead.
                    raise e
                
            else:
                # We got some other error that we weren't expecting
                # and no one is likely able to handle.  Wrap it in a
                # pretty error and pass it on.
                details = traceback.format_exception(*sys.exc_info())
                details = "".join(details)
                userInfo = {klass.errorFactory.REASON: "Unexpected error",
                            klass.errorFactory.DESCRIPTION: "Error encountered calling setValuesForKeysWithDictionary.\n%s" % details,
                            klass.errorFactory.SUGGESTION: ("We're just as stumped as you are on this one.  Help us out "
                                                           "by calling support and letting us know how you got this error!")}

                err = klass.errorFactory.newError(klass.COULD_NOT_SET_VALUES, userInfo)
                raise err
                
                
        for observer in klass.createObservers:
            context = contextInfo.pop(observer, None)
            if hasattr(observer, 'postCreate'):
                observer.postCreate(entityName, entity, context)

        # sigh, from corba, we sholud probably only return the
        # primaryKey not the entity, to discourage using the entities
        # fine grain interface.
        
        return entity
        
    create = classmethod(create)
    

    def registerDataSource(klass, root, dataSource):
        """Registers a datasource that will return records for the given name.

        Each datasource must implement a query method that takes a
        paresd oqlexpression as it's single argument.

        For example, here's a class that provides some tabular data in
        memory, Basically a tuple of tuples

        >>> class MyDataSource(object):
        ...   def __init__(self):
        ...     self._data = []
        ...     self._data.append(('a1','b1','c1')) # add row 1
        ...     self._data.append(('a2','b2','c2')) # add row 1
        ...   def query(self, tokens):
        ...     # this of course is an out right lie we need to return a recordset.
        ...     return self._data

        Now we can register it with the QueryService

        >>> qs = QueryService.testableQS()
        >>> qs.registerDataSource('foo', MyDataSource())

        Our datasoure will now be invoked whenever the rootEntity of
        the query is foo so for example.

        >>> rs = qs.query('select * from foo')

        Note, data sources should return a recordset not a list of
        tuples, but what the heck it's just a unit test.

        >>> len(rs)
        2
        

        """
        klass.dataSources[root.lower()] = dataSource
    registerDataSource = classmethod(registerDataSource)
    

    def getDataSourceForRoot(klass, root):
        return klass.dataSources[root.lower()]
    getDataSourceForRoot = classmethod(getDataSourceForRoot)


    def execute(klass, statement):
        """
        Performs a non-query operation (such as delete or update) and
        returns the number of records affected by it.
        
        statement - An OQL query string.
        """
        
        tokens = klass.parse(statement)

        assert tokens.type in ['delete','update'], "execute only supports delete statements."

        dataSource = klass.getDataSourceForRoot(tokens.rootEntity)
        numRecords = dataSource.execute(tokens)
        return numRecords

    execute = classmethod(execute)

    def query(klass, statement):
        """Returns a RecordSet of data matching the given query.
        statement - Is either an OQLQuery or an object that supports
        the ParsedQuery interface."""
        
        if isinstance(statement, basestring):
            
            try:
                tokens = klass.parse(statement)
            except ReportQueryError:
                # hack, until we move reports to be actual datasources
                # if we can't parse the query into tokens it might be a report query
                reportHome = klass.compReg.get('Report')
                if reportHome:
                    return reportHome.runReportString(statement)
                else:
                    raise
        else:
            # assume the statement is preparsed
            tokens = statement

        assert tokens.type == 'select', "query only supports select statements."
        
        dataSource = klass.getDataSourceForRoot(tokens.rootEntity)
        result = dataSource.query(tokens)
        return klass(result)
    
    query = classmethod(query)

    def getLimit(klass):
        try:
            limit = klass.configService.getint('default', 'query_limit')
        except klass.configService.NoOptionError:
            limit = 1000
        klass.log.info("Query limit is %s" % limit)
        return limit
                       
    getLimit = classmethod(getLimit)



    def parse(klass, statement):
        """Takes either an OQL query or our old fashioned urlencoded
        query and returns it as a token object.

        For example,  parse can be fed an oql query

        >>> qs = QueryService.testableQS()
        >>> query = 'select * from book where foo=1'
        >>> tokens = qs.parse(query)
        >>> tokens.rootEntity
        'book'
        >>> tokens.where[1][0].fullPath
        'book.foo'
        >>> tokens.where[1][2]
        '1'

        And it can be passed our old style, can't wait til it's 
        deprecated query like this.

        >>> query = 'type=book&query_select=title&foo=1'


        """
        try:
            return OQL.parseString(statement)
        except pyparsing.ParseException:
	    # Can't parse it via OQL, so let's see if it's urlencoded,
	    # but to be safe we'll log out the real exception
	    klass.log.debug('Problem parsing oql %s',statement, exc_info=True)
            return klass.parseURLQuery(statement)
        
        # let's determine it's type
        if statement.lower().startswith('select'):
            # it's an OQL query doesn't get much simpler than this
            return OQL.parseString(statement)
        else:
            return klass.parseURLQuery(statement)
    parse = classmethod(parse)




    def _parseURLQuery(klass, statement):
        """Takes a urlencoded query and returns a ParsedQuery

        >>> qs = QueryService.testableQS()
        >>> statement = "type=book&query_select=title&query_select=author"
        >>> pq = qs.parseURLQuery(statement)
        >>> pq.rootEntity == 'book'
        True
        >>> pq.collectionName == ''
        True
        >>> len(pq.columns) == 2
        True

        >>> statement ='type=relation&homeId=book&fromHome=author&fromKey=123&query_select=title&query_select=author&relation=author_books'
        >>> pq = qs.parseURLQuery(statement)
        >>> pq.rootEntity
        'author'
        >>> pq.collectionName
        'books'
        >>> len(pq.columns)
        2
        """

        # first we're going to turn the query into a dictionary, then
        # we'll remove all the key/value pairs that effect the query
        # such as it's offset, limits etc... everything left will be
        # turned into the where clause.

        args = dict(parse_qs(statement))
        try:
            rootEntity =  klass.fixHome(args.pop('type')[0]).lower()
        except KeyError:
            # if it's a report query, we raise this specific error so
            # we can pass the whole query unaltered to the report
            # service. Clunky, but with the introduction of this new
            # QueryService the report servic doesn't have much longer
            # to live
            raise ReportQueryError
            
        collectionName = ''
        limit = 0
        offset = 0
        orderBy = []

        if rootEntity == 'relation':
            # what a pain, relationship queries look like this

            # type=relation&sort_on=description&sort_order=ascending&fromKey=123&fromHome=Campaign& ...
            # homeId=expenseHome&relation=campaign_expenses&nocache=1

            # which is the equivalent to this query
            # select primaryKey from expenses in campaign where campaign.primaryKey = 12


            # so the first thing we need to do is determine the
            # rootEntity, which is either the fromHome or toHome
            # part of the urlquery (they're mutually exclusive)

            # In a url query you needed to know what side of the
            # relationship the entity was on, so either you had an
            # attribute that was pointing to something or you were
            # an object that was pointed by some attribute

            if args.has_key('fromHome'):
                rootEntity = klass.fixHome(args.pop('fromHome')[0]).lower()
                args['%s.primaryKey' % rootEntity] = [args.pop('fromKey')[0]]
                
            elif args.has_key('toHome'):
                rootEntity = klass.fixHome(args.pop('toHome')[0]).lower()
                args['%s.primaryKey' % rootEntity] = [args.pop('toKey')[0]]

            # Next we've got to determine the collectionName.  The
            # collectionName is the name of the attribute from the
            # rootEntity. In the above example it is expenses. The
            # collection name isn't provided in the urlquery. What
            # we do have is the relationship name i.e.
            # relation=campaign_expenses.  So what we have to do
            # in order to find the collectionName is lookup the
            # name of the relationship in the relation registry
            # and find the attribute/collection name.

            relationName = args.pop('relation')[0]
            # homeId, isn't used so we pop it out of the list
            homeId = args.pop('homeId')[0]

            for role in klass.relationRegistry.getRelation(relationName):
                # I could have this backwards
                if role.source.lower() == rootEntity.lower():
                    collectionName = role.field
                    break
                    
            # if we don't have a collection name by this point, we
            # were doing a query that isn't techinically allowed but
            # you could get away with it using the urlquery

            assert collectionName, "Can't query on a one way realtionship.\n" + statement

        # now that we have the rootEntity and collectionName let's
        # remove all the other key/values form the dict that effect
        # how the query behaves

        offset = int(args.pop('query_offset', [0])[0])

        # not sure why but in the old parser  we made sure offset was
        # never negative, we'll continue that trend
        
        offset = max(offset, 0)
        limit = int(args.pop('query_limit', [klass.getLimit()])[0])
        

        if args.has_key('nocache'):
            # Client-side only attribute, get rid of it!
            del args['nocache']

        columnNames = args.pop('query_select')
        columns = []
        namespaces = {}
        if collectionName:
            namespaces[''] = rootEntity + '.' + collectionName
            namespaces[rootEntity] = rootEntity
        else:
            namespaces[''] = rootEntity


	# Latest changes in OQL allow multiple sort descriptions after
	# order by, but I don't thing our url syntax allows that...

        orderByKeyPath = args.pop('sort_on', [None])[0]
        if orderByKeyPath is not None:
            orderByCol = OQL.Column(orderByKeyPath)
            orderByCol._setFullPath(namespaces)
	    orderBy.append(orderByCol)


        sort_order = args.pop('sort_order', [''])[0]
        if sort_order != '':
            if sort_order == 'ascending':
                orderBy.append('ASC')
            elif sort_order == 'descending':
                orderBy.append('DESC')
            else:
                raise RuntimeError('What do you want us to do sort sideways?')

	if orderBy:
	    orderBy = [orderBy]
            
        for name in columnNames:
            col = OQL.Column(name)
            # this converts wildecards '*' to every column based on the eds.
            col.expandColumns(namespaces)
            columns.append(col)
        

        # finally anything left belongs to the where clause. url
        # queries couldn't be nested so we just convert this to a list
        # of tuples. Each tuple has three parts, the attribute a
        # comparison operator (which in url queries is always '=' if it's  a single item or an in clause

        where = []
        for key in args.keys():
            val = args.pop(key)
            if key.startswith('_get_'):
                # Make the _get_ prefix an optional and deprecated syntax
                # of the old system.
                key = key[5:]

            col = OQL.Column(key)
            col.expandColumns(namespaces)
            where.append(col)
            
            if len(val) == 1:
                where.append('=')
                where.append("'%s'" % val[0])
            else:
                # it's a list of items
                where.append('in')
                inStmt = "('"

                inStmt += "','".join(val)
                inStmt += "')"
                where.append(inStmt)

            where.append('AND')


        if len(where):
            where.insert(0, 'WHERE')
            
            if where[-1] == 'AND':
                del where[-1]


                

        parsedQuery = ParsedQuery(rootEntity)
        parsedQuery.collectionName = collectionName
        parsedQuery.columns = columns
        parsedQuery.orderBy = orderBy

        parsedQuery.where = where
        parsedQuery.limit = limit
        parsedQuery.offset = offset
        parsedQuery.asString = statement
        parsedQuery.dialect = 'urlquery'
        parsedQuery.type = 'select'
        return parsedQuery
        

    # we use a different name because it seems class methonds aren't
    # recognized by doctest
    parseURLQuery = classmethod(_parseURLQuery)
    

    

    def fixHome(klass, home):

        if home.endswith('Home'):
            klass.log.warn('Stripping Home off of type=%s. FIX THE CLIENT!' % home)
            home = home[:-4]
        if home.lower() == 'user':
            klass.log.warn('Converting user to ciUser FIX THE CLIENT!')
            home = 'ciUser'

        return home

    fixHome = classmethod(fixHome)

        


class ParsedQuery(object):
    """Represents a Query that has been parsed into a structure that's
    easy to understand. ParsedQuerys have the following attributes.

    rootEntity:

      The of the type of the object that we're querying, most of the
      time it's the name of an entity, however it could be a
      computedView.

    collectionName

      This is the name of the attribute/relationship that represents a
      collecition. For instance say you were querying all the books by
      a specific author, in other words you have an Author object that
      has an attribute named 'books' which is a list of each book the
      author wrote. The rootEntity in this case would equal 'author'
      and collectinoName would be 'books'

    columns:

      A list of column objects the each represents an attribute you
      want returned.

    where:

      A list of clauses, clauses are either a series of 'attribute',
      'comparison', 'value' or another clause

    orderBy:
      The results will be sorted by this coulumn

    direction:

      If an order by clause was specified optionaly a direction can be
      specified as either ASC or DESC for ascending or descending. 

    offset:

      Starts returning results in the list from this offset. In other
      words if there are a 100 items and offset is 10, skip the first
      ten elments return the rest up to the limit.

     limit:

       No more than this many rows will be returned.

    
    The OQL module already parsese querys into an object that has
    these attributes. Althoguht it doesn't use this formal class .
    """

    def __init__(self, rootEntity):
        self.rootEntity = rootEntity
        self.columns    = []
        self.where      = []
        self.limit      = 0
        self.offset     = 0
        self.orderBy    = []
        self.direction  = ""
        self.asString = "" # unparsed query
        self.dialect = ""  # dialect of unparsed query

    def __str__(self):
        return self.asString

    def __repr__(self):
        return self.asString
        
