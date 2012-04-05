from __future__ import with_statement
import sys
from Rambler.Entity import RObject
class Operation(RObject):

  QueuePriorityVeryLow = -8
  QueuePriorityLow = -4
  QueuePriorityNormal = 0
  QueuePriorityHigh = 4
  QueuePriorityVeryHigh = 8
  seq = sys.maxint
  
  def __init__(self, *args, **kw):
    super(Operation, self).__init__(*args, **kw)
    
    self.dependencies = set()
    self.queue_priority = self.QueuePriorityNormal
    
    self.finished   = False
    self.executing  = False
    self.cancelled   = False
    self.concurrent = False
    self.seq = Operation.seq
    if Operation.seq == 0:
      Operation.seq = sys.maxint
    else:
      Operation.seq -= 1
    
  def __cmp__(self, other):
    """Sort Operations by ready status then priority"""
    # note compare order deliberatly compares other first, because we want the opposite
    # of what normally be returned by the these tuples
    if isinstance(other, Operation):
      return cmp((other.is_ready, other.queue_priority, other.seq),(self.is_ready, self.queue_priority,self.seq))
    else:
      raise TypeError('Operations can only be compared to other Operation')
    
  def __repr__(self):
    return '<%s object at %s ready %s priority %s seq %s>'  % (self.__class__.__name__, id(self), self.is_ready, self.queue_priority, self.seq)

  # Executing the Operation
  def start(self):
    """Begins execution of the Operation."""
    if self.is_cancelled:
      with self.changing('is_finished'):
        self.finished = True
      return
    
    
    with self.changing('is_executing'):
      self.executing = True  
      
    with self.changing('is_finished', 'is_executing'):
      try:
        # if we're starting, we no longer have any dependencies
        self.dependencies.clear()
        
        if not self.is_cancelled:
          self.main()
      finally:
        self.executing = False
        self.finished = True          
    
  def main(self):
    """Preforms the Operations non concurent task."""
    pass
    
  # Canceling Operations
  def cancel(self):
    """Informs the Operation that it should cancel.
    """
    if not self.is_cancelled:
      self.will_change_value_for('is_cancelled')
      self.cancelled = True
      # remove our dependencies so that we're ready, properly behaved operations
      # will honor the cancel flag
      self.dependencies.clear()
      self.did_change_value_for('is_cancelled')
      
      if not self.is_executing and not self.is_finished:
        with self.changing('is_finished'):
          self.finished = True
        
    
  # Getting Operation Status
  @property
  def is_cancelled(self):
    return self.cancelled
    
  @property
  def is_executing(self):
    """Returns a bool indicating whether an operation is currently executing."""
    return self.executing
    
  @property
  def is_finished(self):
    return self.finished
  
  @property
  def is_concurrent(self):
    """Returns a bool indicating whether the operation runs asynchronusly.
    
    Discussion:
    The OperationQueue will start new threads for operations that are not concurent.
    
    """
    return self.concurrent
    
  @property
  def is_ready(self):
    """Returns a bool indicating whether the Operation can now be preformed.
    
    Discussion:
    Default implementation returns True if all Dependencies are finished.
    """
    for dependency in self.dependencies:
      if not dependency.is_finished:
        return False
    # If all dependencies are finished we're ready.
    return True
        
    
  # Managing Dependencies
  
  def add_dependency(self, dependency):
    if not isinstance(dependency,Operation):
      raise TypeError('dependency must be an Operation not %s' % type(dependency))
    self.dependencies.add(dependency)
  
  def remove_dependency(self, dependency):
    self.dependencies.remove(dependency)
    
  
    
  
    