import unittest
from Rambler.controllers.Operation import Operation
from Rambler.TestCase import TestCase


class TestOperationQueue(TestCase):
  
  def setUp(self):
    super(TestOperationQueue, self).setUp()
    self.run_loop = self.componentFor('RunLoop').currentRunLoop()
    self.queue = self.comp()
    
  def test_queue(self):
    operation1 = Operation()
    assert operation1.is_ready
    operation2 = Operation()
    operation2.add_dependency(operation1)
    assert operation2.is_ready == False
    
    self.queue.add_operation(operation1)
    self.queue.add_operation(operation2)
    
    self.run_loop.waitBeforeCalling(2, self.quit)
    self.run_loop.run()
    assert operation2.is_finished
    assert operation1.is_finished
    self.assertEqual(0, len(self.queue.operations))
    self.assertEqual(0, len(self.queue.executing))
    
    
  def quit(self):
    self.run_loop.stop()
  
  
if __name__ == '__main__':
  unittest.main()