"""Profiles each call made within a transaction."""

from Rambler import outlet

import sys
import hotshot, hotshot.stats
from StringIO import StringIO
from EventChannel import Handler






# Nasty hack to get around the fact that we obfuscate the
# Server module. If I like how I install this Profiler, I
# might make a cleaner way to do this.

# TODO: With the move to the new descriptor based component loading,
# this module wasn't readded.

#serverModule=sys.modules[Server.__class__.__module__]
#CORBAWrapper = serverModule.CORBAWrapper
#BoundMethod = serverModule.BoundMethod


import os

class ProfileMethod:
    
    def __init__(self, method, instance, epo_name):
        self.method = BoundMethod(method, instance, epo_name)
        self.name = method.__name__

    def __call__(self, *args, **kw):
        return ProfileService.runcall(self.name, self.method, *args, **kw)
        
                

STAT_DIR="/tmp/stats/"
class ProfileService:
    """Profiles each call made within a transaction"""

    log = outlet("LogService")

    interface = "ciIProfileService"
    __epo_name__ = "ProfileService"

    
    def __init__(self):
        self._callMap = {} # List of method calls per transaction
        self._handler = Handler(self.clear)
        self._trans = None

    def runcall(self, name, func, *args, **kw):
        
        tid = Server.txn.get_transaction_name()

        if tid == "":
            # Don't bother profiling methods invoked outside of a
            # transaction.
            return apply(func, args, kw)

        logdir = os.path.join(STAT_DIR, tid)
        if not self._callMap.has_key(tid):
            self._callMap[tid] = []
            if not os.path.exists(logdir):
                os.makedirs(logdir)

        self._callMap[tid].append(name)

        callnum = len(self._callMap[tid])
        logname = os.path.join(logdir, "%s.%s" % (name,callnum))

        prof = hotshot.Profile(logname)
        results = prof.runcall(func, *args, **kw)

        prof.close()


        return results

    def scanTransactions(self):
        if self._trans is not None:
            # Already scanned
            return
        
        trans = self._trans = []
        transactions = os.listdir(STAT_DIR)
        for txn in transactions:
            transdir = os.path.join(STAT_DIR, txn)
            stats = []
            trans.append(stats)

            # Sigh sort the list
            
            methods = os.listdir(transdir)
            
            offsets = [(int(methods[x].split(".")[1]), methods[x], x)
                       for x in range(len(methods)) ]
            offsets.sort()
            methods = [ method for pos, method, dontcare in offsets]

            for fname in methods:
                name = fname.split(".")[0]
                stats.append(
                    (name, hotshot.stats.load(os.path.join(transdir, fname))
                     ))


    def showTransactions(self):

        """Returns a list of each transaction profiled along with the
        time spent."""
        self.scanTransactions()
        txns = []

        # Summarize the stats
        for x in range(len(self._trans)):
            stats = self._trans[x]
            trans_time = 0
            remote_calls = 0
            for name, stat in stats:
                trans_time += stat.total_tt
                remote_calls += 1
            txns.append((x, trans_time, remote_calls))

        results = ["TX#\tTime\tCalls",
                   "=" * 22]

        for item in txns:
            results.append("%3d\t%4f\t%5d" % item)
            
        return "\n".join(results)

    def showRemoteCalls(self, txnNum):
        self.scanTransactions()
        remoteCalls = self._trans[txnNum]
        results = []
        for x in range(len(remoteCalls)):
            name, stats = remoteCalls[x]
            results.append((x, name, stats.total_tt, stats.total_calls))

        ret = ["Method #\tName\tTime\tCalls",
               "=" * 40]
        for result in results:
            ret.append("%8d\t%s\t%f\t%d" % result)

        return "\n".join(ret)

    def showStatsForMethod(self, txnNum, methodNum):
        self.scanTransactions()
        stats = self._trans[txnNum][methodNum][1]

        output=StringIO()
        stdout=sys.stdout
        sys.stdout=output
        stats.strip_dirs().print_stats()
        sys.stdout.flush()
        sys.stdout=stdout
        return output.getvalue()
        

    def clear(self, tid):
        # Transaction finished remove the calls if they exist
        if self._callMap.has_key(tid):
            calls = self._callMap[tid]
            del self._callMap[tid]
            
            print "Transaction %s:" % tid
            for call in calls:
                print "\t%s" % call

    def enableProfiler(self, state):

        ec = Server.getService("EventService")
        if state == True:
            self.log.info("Enabeling Profiler")
            CORBAWrapper.bindMethod = ProfileMethod
            TXNWatcher = Server.TXNWatcher_i

            # It would be usefull to profile the commit methods, but
            # using the current implimintation puts the TXNWatchers
            # method under transactino control.
            
            #TXNWatcher.commit_one_phase = ProfileMethod(TXNWatcher.commit_one_phase, TXNWatcher, "TXNWatcher")
            #TXNWatcher.rollback = ProfileMethod(TXNWatcher.rollback, TXNWatcher, "TXNWatcher")
            
            ec.subscribeToEvent("commit", self._handler, str)
            ec.subscribeToEvent("rollback", self._handler, str)
            return "Profiler Enabled"
        else:
            self.log.info("Disableing Profiler")
            CORBAWrapper.bindMethod = BoundMethod
            ec.ensubscribeFromEvent("commit", self._handler, str)
            ec.unsubscribeFromEvent("rollback", self._handler, str)
            return "Profiler Disabled"


#ProfileService = ProfileService()
