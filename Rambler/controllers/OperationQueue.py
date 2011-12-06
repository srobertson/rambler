from __future__ import with_statement

import heapq
import os
import threading
import Queue

from Rambler import outlet


class OperationQueue(object):
  """Manages a set of Operation objects in a priority queue, regulating their execution
  based on priority and dependencies.
  """
  log = outlet('LogService')
  RunLoop = outlet('RunLoop')
  
  def __init__(self):
    # todo: Default this to the number of CPUs in the machine 
    self.is_suspended = False
    self.thread_pool_queue = Queue.Queue()
    self.threads = set()
    self._operations = []
    self.executing = set()
    
    self._max_concurrent_operation_count = 0
    
    # TODO: move this to a configuration servier or some other service that
    # returns platform information
    
    # Determine number of processors in the machine
    if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"): 
      #Linux and Unix 
      cpu_count = os.sysconf("SC_NPROCESSORS_ONLN") 
      if not isinstance(cpu_count, int) or not cpu_count > 0: 
        #MacOS X 
        cpu_count = int(os.popen2("sysctl -n hw.ncpu")[1].read())
  
    self.set_max_concurrent_operation_count(cpu_count)
        
    self.run_loop = self.RunLoop.currentRunLoop()
    self.name = ""
  
  def add_operation(self, operation):
    """Adds an operation to the queue"""
    if not operation.is_finished:
      heapq.heappush(self._operations, operation)
      self.__kick_queue()

  @property
  def operations(self):
    """Returns a copy of the operation queue."""
    ops = set(self._operations) 
    ops.update(self.executing)
    return ops
    
  def cancel_all_operatians(self):
    for operation in self._operations:
      operation.cancel()
      
    for operation in self.executing:
      operation.cancel()
      
  def wait_until_all_operations_are_finished(self):
    """Blocks the current thread until all operations are finished."""
    raise NotImplementedErro()
    
  def max_concurrent_operation_count(self):
    return self._max_concurrent_operation_count
    
  def set_max_concurrent_operation_count(self, value):
    self._max_concurrent_operation_count = value
    
    thread_count = len(self.threads)
    if thread_count < value:
      for x in range(value - thread_count):
        thread = OperationThread(self.thread_pool_queue)
        thread.log = self.log
        thread.start()
        self.threads.add(thread)
    elif thread_count > value:
      # kill a couple of threads
      for x in range(thread_count - value):
        thread = self.threads.pop()
        thread.stop = True

    self.__kick_queue()
    
  max_concurrent_operation_count = property(max_concurrent_operation_count, set_max_concurrent_operation_count)
  
  def suspended(self):
    """Returns bool indicating whether the OperationQueue is scheduling operations."""
    return self.is_suspended
  
  def set_suspended(self, value):
    """Modifies the execution of pending operations"""
    self.is_suspended = value
    if not value:
      self.__kick_queue()
      
  suspended = property(suspended, set_suspended)
        
  def observe_value_for(self, key_path, of_object, change):
    if self.name.startswith('Disco'):
      print self.name, key_path, of_object
    if key_path == 'is_finished' and of_object.is_finished:
      #self.log.info('<<< of_object %s finished', of_object)
      self.run_loop.callFromThread(self.__operation_finished, of_object)

      
  def __operation_finished(self, operation):
    if self.name.startswith('Disco'):
      print 'removing', operation
    
    operation.remove_observer(self, 'is_finished')
    self.executing.remove(operation)
    if self._operations and not self._operations[0].is_ready:
      heapq.heapify(self._operations)
    self.__kick_queue()

  def __kick_queue(self):
    """Called internally to execute the next operations"""
    #self.log.info('Q kicked %d %d' % (len(self.executing), len(self._operations)))
    if not self.suspended:
      num_to_launch = self.max_concurrent_operation_count - len(self.executing)
      for x in range(num_to_launch):          
        if len(self._operations) and self._operations[0].is_ready:
          operation = heapq.heappop(self._operations)
          self.log.info('Starting operation %s', operation)
          operation.add_observer(self, 'is_finished')
          self.executing.add(operation)
          self.log.info('Executing operation %s', operation)
          if  operation.is_concurrent:
            operation.start()
          else:
            self.thread_pool_queue.put(operation)
            
        else:
          break
  
  
#
class OperationThread(threading.Thread):
  
  log = outlet('LogService')
  
  def __init__(self, queue):
    self.stop = False
    self.queue = queue
    super(OperationThread, self).__init__()
    self.setDaemon(True)
    
  def run(self):
    """This method should only be invoked from a thread"""
    while not self.stop:
      operation = self.queue.get()
      try:
        operation.start()
      except:
        self.log.exception('Exception in %s', self.getName())