import types

from Rambler import component, outlet

  
class CoroutineOperation(component('Operation')):
  RunLoop = outlet('RunLoop')
  is_concurrent = True
  
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
    if self.is_cancelled:
      self.target.close()
      return
      
    try:
      # todo check value and call target.throw if it derives from an exception
      result = self.target.send(value)
    except StopIteration:
      self.result = value
      return self.finish()
    except Exception,e:
      self.result = e
      return self.finish()
      
    if  isinstance(result, self.Operation) or isinstance(result, types.GeneratorType):
      self.wait_for_op(self.new(result,self.queue))
    else:
      self.send(result)

  
  def send(self, value):
    self.run_loop.waitBeforeCalling(0, self.run, value)

  def wait_for_op(self, op):    
    op.add_observer(self, 'is_finished')
    op.add_observer(self, 'is_cancelled')
    self.queue.add_operation(op)
    
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

    

