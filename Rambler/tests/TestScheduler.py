import unittest

from Rambler import outlet
from Rambler.defer import Deferred, failure, succeed
from Rambler.TestCase import TestCase

class TestScheduler(TestCase):
  # FIXME, this component relies on none of these components, nor any othe component in
  # the current extension. However rambler.TestCase loads every component which need
  # things from these other extensions, even though they are not used...
  
  def setUp(self):
    super(TestScheduler, self).setUp()
    self.runLoop = self.componentFor('RunLoop').currentRunLoop()

  
  def testSequntial(self):
    ticket = self.comp.call(self.sequentialCall)
    self.runLoop.run()
    
  def testParallel(self):
    ticket = self.comp.call(self.parallelCall)
    self.runLoop.run()
    
  def sequentialCall(self):
    # Invoke methods that each return defereds return one
    # defer at a time
    count = yield(self.returns1)
    count += yield(self.returns10)
    self.assertEqual(11, count)
    
  def parallelCall(self):
    counts = yield((self.returns1,), (self.returns10,))
    self.assertEqual(11, sum(counts))
    self.assertEqual(1, counts[0])
    self.assertEqual(10, counts[1])
    
  
    
  def returns1(self):
    """Method which when invoked returns the number 1"""
    return succeed(1)
    
  def returns10(self):
    """Method which when invoked returns the number 1"""
    return succeed(10)
    
    
if __name__ == "__main__":
  unittest.main()