from Rambler import outlet
class TimerService(object):
    runLoop = outlet("RunLoop")
    
    # Implements our old TimerService API on top of our new
    # RunLoop. Currently we make sure that the timers are executed on
    # the MainThread, but we might want to consider executing them in
    # a different thread.

    threadName = "MainThread"
    
    def waitBeforeCalling(self, seconds, method, *args,  **kw):
       # Create a non repeating event
        return self.runLoop.runLoopForThread(self.threadName)\
               .waitBeforeCalling(seconds, method, *args, **kw)

    def intervalBetweenCalling(self, seconds, method, *args, **kw):
        # Create a repeating event
        return self.runLoop.runLoopForThread(self.threadName)\
               .intervalBetweenCalling(seconds, method, *args, **kw)
    
