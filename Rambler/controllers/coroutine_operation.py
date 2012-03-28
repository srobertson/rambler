import sys
import types

from Rambler import component, outlet

  
class CoroutineOperation(component('Operation')):
  RunLoop = outlet('RunLoop')
  is_concurrent = True
  _result = None
  _exc_info = None
  
  @classmethod
  def new(cls, gen_op_or_result, queue):    
    if isinstance(gen_op_or_result, cls.Operation):
      # already an operation
      op = gen_op_or_result
    elif isinstance(gen_op_or_result, types.GeneratorType):
      op = CoroutineOperation(gen_op_or_result, queue)
    else: # it's a normal value
      return FinishedOperation(gen_op_or_result)
      
    return op
          
  def __init__(self, target, queue):
    super(CoroutineOperation,self).__init__()
    self.run_loop = self.RunLoop.currentRunLoop()
    self.name     = target.__name__
    self.target   = target
    self.queue    = queue
    
  def __repr__(self):
    return "<coroutine({0}) name:{1} coroutine:{2}>".format(id(self), self.name, self.target)

  def start(self):
    with self.changing('is_executing'):
      self.executing = True
      self.send(None)
      
  def run(self, value):
    """Recevies the results of the last operation we were waiting for.
    """
    if self.is_cancelled:
      self.target.close()
      return
      
    try:
      if  isinstance(value, Exception):
        # exception encountered from the last operation, give the 
        # coroutine a chance to rescue it. If it can't we'll catch
        # the error in the generic exception handler.
        result = self.target.throw(value)
      else:
        # The value the coroutine was waiting for is here, give it back to the coroutine
        result = self.target.send(value)
    except StopIteration:
      # coroutine finished gracefully
      self._result = value
      return self.finish()
    except:
      # coroutine finished not so gracefully, save the exception so we rethrow it if someone
      # access the results of this operation
      self._exc_info = sys.exc_info()
      return self.finish()
      
    if  isinstance(result, self.Operation) or isinstance(result, types.GeneratorType):
      # After processing the value, the coroutine want's us to wait on another operation
      self.wait_for_op(self.new(result,self.queue))
    else:
      # normal value, the coroutine was yielding controll to us, we'll return to it
      # in the next pass of the run loop.
      self.send(result)

  
  def send(self, value):
    self.run_loop.waitBeforeCalling(0, self.run, value)

  def wait_for_op(self, op):    
    op.add_observer(self, 'is_finished')
    op.add_observer(self, 'is_cancelled')
    # self.replace(op)
    # to avoid lock ups, we skip the queue and run the operation directly
    if  op.is_concurrent:
      op.start()
    else:
      self.queue.thread_pool_queue.put(op)
    
    
  def observe_value_for(self, key_path, op, change):
    # Note: observe_value is not guarnteed to come from the same thread, no?
    # wonder if we should protect this with a call to run_loop.waitFor
    op.remove_observer(self, 'is_finished')
    op.remove_observer(self, 'is_cancelled')
    
    if key_path == 'is_finished':

      self.send(op.result)
    elif key_path == 'is_cancelled':
      # is waiting here even important? generators will receive generator exit without this
      print "!!!!!!!!!! canceled !!!!!!!!!!"

  def finish(self):
    with self.changing('is_executing', 'is_finished'):
        self.executing = False
        self.finished = True
        # cleanup just because
        self.queue = self.target = None
  
  @property
  def result(self):
    """ Result of the operation
    Evaluates to the value of the last succesful operation or throws an exception.
    It is an error to call this 

    """
    if self._exc_info:
      raise self._exc_info[0],self._exc_info[1], self._exc_info[2]
    else:
      return self._result



    

