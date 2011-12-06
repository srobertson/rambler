from __future__ import with_statement
from Rambler import component, outlet

class OperationCancelled(Exception):
  pass
  
class InvocationOperation(component('Operation')):
  runLoop = outlet('RunLoop')
  is_concurrent = True
  
  @classmethod
  def new(cls, method, *args, **kw):
    op = cls()
    op.invocation = method
    op.args = list(args)
    op.kw = kw
    return op
    
  def __repr__(self):
    return '<%s (%s) object at %s ready %s priority %s seq %s>'  % (self.__class__.__name__, self.invocation, id(self), self.is_ready, self.queue_priority, self.seq)
  
  def __init__(self):
    self._result = None
    self.invocation = None
    self.kw = None
    self.args = None
    super(InvocationOperation, self).__init__()
    
  def __repr__(self):
    if self.invocation:
      name = self.invocation.func_name
    else:
      name = ' (no invocation) '
    return "Invocation(%s)" % name
  
  def start(self):
    with self.changing('is_executing'):
      self.executing = True
    if self.is_concurrent:
      # run the method via the runloop, this may be an abuse of this flag
      self.runLoop.currentRunLoop().waitBeforeCalling(0,self.main)
    
    # else main() will be invoked by the thread
    
  def main(self):
    with self.changing('is_finished', 'is_executing'):
      if not self.is_cancelled:
        try:
          print '>>>', self.invocation.__name__
          self._result = self.invocation(*self.args, **self.kw)
        except Exception, e:
          self._result = e
      else:
        self._result = OperationCancelled()
      self.executing = False
      self.finished  = True
                
  @property
  def result(self):
    # todo raise a CancelledException if operation was canceled
    if isinstance(self._result, Exception):
      raise self._result
    else:
      return self._result
    
  