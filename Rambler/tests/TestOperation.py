import unittest

from Rambler.controllers.Operation import Operation
from Rambler.RunLoop import RunLoop

class TestOperation(unittest.TestCase):
  
  def setUp(self):
    self.observed = False
    self.run_loop = RunLoop.currentRunLoop()
    
  def test_dependency(self):
    operation1 = Operation()
    assert operation1.is_ready
    operation2 = Operation()
    operation2.add_dependency(operation1)
    assert operation2.is_ready == False
    self.operation2 = operation2
    operation1.add_observer(self, 'is_finished')
    operation1.start()
    assert self.observed

  def test_sort(self):
    operation1 = Operation()
    #operation1.queue_priority = self.QueuePriorityLow
    operation2 = Operation()
    operation2.add_dependency(operation1)
    
    operation3 = Operation()
    operation3.queue_priority = Operation.QueuePriorityLow
    
    l = [operation2, operation1, operation3]
    l.sort()
    self.assertEqual(l, [operation1, operation3, operation2])
    
    operation4 = Operation()
    operation4.queue_priority = Operation.QueuePriorityVeryHigh
    
    operation5 = Operation()
    operation5.add_dependency(operation4)
    operation5.queue_priority = Operation.QueuePriorityVeryLow
    
    l = [operation2, operation1, operation5, operation4,  operation3]
    l.sort()
    self.assertEqual(l, [operation4, operation1, operation3, operation2, operation5])
    
    
  def observe_value_for(self, key_path, of_object, change):
    self.observed = True
    assert of_object.is_finished
    assert self.operation2.is_ready
  
  
if __name__ == '__main__':
  unittest.main()