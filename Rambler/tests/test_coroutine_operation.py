from nose.tools import eq_
from Rambler import coroutine, outlet
from Rambler.TestCase import TestCase

class TestCoroutineOperation(TestCase):
  scheduler = outlet('Scheduler')
  
  def test_context_switch(self):
    # Two toplevel coroutines should have two different contexts
    

    op1=self.routine(10)
    op2=self.routine(15)
    
    # Schedule both calls in the run_loop
    op1.start()
    op2.start()
    # advance the run_loop
   
    self.run_loop.runOnce()
    eq_(len(self.run_loop.timers),2)
    
    self.run_loop.runOnce()
    eq_(len(self.run_loop.timers),2)
    
    self.run_loop.runOnce()
    eq_(len(self.run_loop.timers),0)
    
    
    eq_(op1.result,10)
    eq_(op2.result,15)
    
    
  
  def routine(self,x):
    """Simulate
    
    TestCases's aren't real components so we can't use the decorator
    @coroutine
    def function(x):
      ....
    
    """
    def function(x):
      coroutine.context.x = x
      yield
      yield coroutine.context.x

    return self.CoroutineOperation.new(function(x), self.Scheduler.queue)
    
    
  