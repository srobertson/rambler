from Rambler import client
import epo


def getResults(query):

    """Generator that will fetch all the data from the server in a
    series of small transactions"""

    conn = client.connect("localhost", "juncture")
    qs = conn.lookup("QueryHome")

    # TODO: The first time this method is called, add the column headers


    offset = 0
    limit = 1024
    headers = []
    
    

    buffer = []

    while 1:
        
        if len(buffer) == 0:
            # We're out of results try to get some more
            batch = "%s  LIMIT %s OFFSET %s" % (query, limit, offset )
            conn.begin()
            try:
                
                recordSet = qs.query(batch)

                try:
                    results = recordSet.fetchMany("query", limit)
                except epo.StopIteration:
                    # We've exhausted the query
                    results = None
                conn.commit()
            except:
                conn.rollback()
                raise

            if results is None:
                # We didn't get any results, so this ends the generator
                break
            else:
                if headers == []:
                    tc=results.typecode().content_type()
                    for x in range(tc.member_count()):
                        headers.append(tc.member_name(x))
                    buffer.append(headers)
                    
                results = results.value()
                # Up the offset, so the next time we call query we get new leads
                offset += len(results)
                
                buffer.extend([flatten(r, headers) for r in results])

        else:
            yield buffer.pop(0)

def flatten(struct, headers):
    return ['"%s"' % getattr(struct, h) for h in headers]

def query(query):
    conn = client.connect("localhost", "juncture")
    qs = conn.lookup("QueryHome")

    data = []
    conn.begin()
    try:
        recordSet = qs.query(query)

        while 1:
            try:
                results = recordSet.fetchMany("query", 1024)
                data.extend(results.value())
            except epo.StopIteration:
                break
        conn.commit()
    except epo.QueryError, e:
        print "Error executing query:"
        print "-" * 20
        print e.details
        print "-" * 20
        
    except:
        conn.rollback()
        raise

    if not data:
        return
    
    for member in data[0]._members:
        print member, "\t|",

    print
    print "-" * 72

    for row  in data:
        for member in row._members:
            print str(getattr(row, member)) + "\t",

        print



def bulkDL(query,conn,qs):
    limit = 1024
    offset = 0
    data =[]

    while 1:
        batch = "%s  LIMIT %s OFFSET %s" % (query, limit, offset )
        conn.begin()
        try:
            recordSet = qs.query(batch)
            try:
                results = recordSet.fetchMany("query", limit)
            except epo.StopIteration:
                # We've exhausted the query
                results = None
            conn.commit()
        except:
            # rollback on any error
            conn.rollback()
            raise

        if results is None:
            break
        else:
            results = results.value()
            offset += len(results)
            data.extend(results)

    return data

if __name__ == "__main__":

    import sys

    if len(sys.argv) == 1:
        print 'Enter Query and press enter'
        while 1:
            queryStr = raw_input("=> ")
            if queryStr:
                query(queryStr)
    else:
        query= " ".join(sys.argv[1:])
        conn = client.connect("localhost", "juncture")
        qs = conn.lookup("QueryHome")

        data=bulkDL(query, conn,qs)

        #sys.exit()
        # Execute the command line query
##        data=[]
##        for result in getResults(query):
##            #print ",".join(result)
##            #print result

##            data.append(result[0])


        print len(data)
