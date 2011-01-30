import types
import traceback
import sys

from Rambler import outlet
from Rambler.defer import  Deferred, succeed


class Scheduler(object):
  """Provides threading like behavior to pyhon generators."""
  log = outlet('LogService')
  Operation = outlet('Operation')
  OperationQueue = outlet('OperationQueue')
  RunLoop = outlet('RunLoop')
  
  def __init__(self):
    self.ticket = 0
    self.results = {}

  def will_disassemble(self):
    self.queue.cancel_all_operatians()
    
  def assembled(self):
    self.queue = self.OperationQueue()
    self.queue.name = "Main Queue"

  def newticket(self):
   return Deferred()
  
  def call(self, method, *args, **kw):
    ticket = self.newticket()
    try:
      value = method(*args, **kw)
    except:
      self.log.exception('Error calling method %s', str(method))
      raise

    if type(value) == types.GeneratorType:
      try:
        invocations = value.next()
        self.invoke(ticket, value, invocations)
        return ticket
      except StopIteration,e:
        # method was a generator but didn't return anything to be invoked
        if len(e.args):
          value = e.args[0]
        else:
          value = None
          
    return succeed(value)

        
  def invoke(self, ticket, gen, invocations):
    """Give each thread a chance to run.

    invocations := (invocation | (invocation1, invocationX))
    invocation := (method_args_and_kw | method_args | method_kw | method)
    method := callable
    method_args_and_kw := method arg1,argx, kwdict
    method_args := method, arg1, argx

    method_keywords_only := method kwdict
    """
    
    self.results[ticket] = {}
    

    # In the future I'll be ripping out invocations which are just tuples in favor
    # of the cleaner Operation api, right now we support both
    if isinstance(invocations, self.Operation):
      d_args=[ticket, gen, 0, 1]
      run_loop = self.RunLoop.currentRunLoop()
      invocations.add_observer(self, 'is_finished', 0, run_loop, *d_args)
      invocations.add_observer(self, 'is_cancelled', run_loop)
      if not invocations.is_executing:
        self.queue.add_operation(invocations)
      return
      
    else:
      # convert to list in order to get a count prior to actually invoking
      invocations = list(self.normalize_invocations(invocations))

    seq = 0
    count = len(invocations)
    for method, args, kw in invocations:
      try:
        i_result = method(*args,**kw)
      except Exception, e:
        i_result = e

      d_args=[ticket, gen, seq, count]
        
      if isinstance(i_result, Deferred):
        i_result.addCallbacks(self.callback, self.errback, callbackArgs=d_args, errbackArgs=d_args)
      elif isinstance(i_result, self.Operation):
        run_loop = self.RunLoop.currentRunLoop()
        i_result.add_observer(self, 'is_finished', 0, run_loop, *d_args)
        i_result.add_observer(self, 'is_cancelled', run_loop)
        if not i_result.is_executing:
          self.queue.add_operation(i_result)
        
      elif isinstance(i_result, Exception):
        self.errback(i_result, ticket, gen, seq, count)
      else:
        # method returned a result rather than a defered
        self.callback(i_result, ticket, gen, seq, count)
        
      seq += 1

  
  def callback(self, results, ticket, thread, seq, count):
    # results can come back in any order, we store until they've all returned
    self.results[ticket][seq] = results
    if len(self.results[ticket]) == count:
      combined_results = self.results.pop(ticket)
      ordered_results = []
      for x in range(count):
        ordered_results.append(combined_results[x])
        
      if count == 1:
        ordered_results = ordered_results[0]
      
      try:
        self.invoke(ticket, thread, thread.send(ordered_results))
      except StopIteration,e:
        if len(e.args):
          ticket.callback(e.args[0])
        else:
          ticket.callback(None)
      
  def errback(self, failure, ticket, thread, seq, count):
    if isinstance(failure, Exception):
      ex_type, ex_value, ex_tb = sys.exc_info()
    else:
      ex_type = failure.type
      ex_value = failure.value
      ex_tb = failure.tb
      
    try:     
      self.invoke(ticket, thread, thread.throw(ex_type, ex_value, ex_tb))
      del ex_tb
    except StopIteration, e:
      if len(e.args):
        ticket.callback(e.args[0])
      else:
        ticket.callback(None)
    except Exception,e:
      # coroutine didn't handle the failure or their was a new one
      self.log.exception('Coroutine did not handle %s', e)
      ticket.errback(e)
  
  def observe_value_for(self, key_path, operation, change, run_loop, *args):
    #print "\033[0;31m %s %s \033[m" % (operation, key_path)
    run_loop.callFromThread(self.operation_finished, operation, *args)
  
  def operation_finished(self, operation, ticket, thread, seq, count):
    operation.remove_observer(self, 'is_finished')
    operation.remove_observer(self, 'is_cancelled')

    try:
      result = operation.result
    except StopIteration, e:
      if len(e.args):
        ticket.callback(e.args[0])
      else:
        ticket.callback(None)
      return
    except Exception, e:
      self.errback(e, ticket, thread, seq, count)
      return
    
    self.callback(result, ticket, thread, seq, count)
        
        
  def normalize_invocations(self, invocations):
    """Invocations can  be passed in various ways. Such as 
     invocations := (invocation | (invocation1, invocationX))
      invocation := (method_args_and_kw | method_args | method_kw | method)
      method := callable
      method_args_and_kw := method arg1,argx, kwdict
      method_args := method, arg1, argx

      method_keywords_only := method kwdict
      
      This method normalize them to always be 
      ((method1, args1, kw1),...,(methodX, argsX, kwX))
    
    """
    
    if callable(invocations):
      # single method no args
      yield (invocations, (), {})
    else:
      # We can now assume it's a list, the question is, is it a single invocation
      # or multiple?
      
      if callable(invocations[0]):
        # it's a single invocation, let's normalize the args
        yield self.normalize_invocation(invocations)
      else: # We can now assume that each item in invocations is a single invocation
        for invocation in invocations:
          yield self.normalize_invocation(invocation)
          
  def normalize_invocation(self, invocation):
    length = len(invocation)
    if length == 0:
      raise RuntimeError("Invocations must have at least one item and it must be callable")

    method = invocation[0]
    if length == 1:
      assert callable(method)
      return method, (), {}
    else:
      if isinstance(invocation[-1], dict):
        kw = invocation[-1]
        args = invocation[1:-1]
      else:
        args = invocation[1:]
        kw = {}
      return method, args, kw
      
    