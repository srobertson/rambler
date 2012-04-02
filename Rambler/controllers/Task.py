import os
import sys
import traceback
import signal
import errno

from Rambler import outlet, component

#TODO: Move Stream to a component
from Rambler.RunLoop import Stream



class Task(component('Operation')):
  """Used to launch and interact with a subprocess asynchronously. 
  This is the only way you should execute a sub task.
  
  Usage: create a delegate with on_stdout, on_stdin, on_stderr methods to interact with the process,
  to determine if a process is done call termination_status
  
  In order to test this you need a working runLoop
  
  >>> from Rambler.RunLoop import RunLoop
  >>> from Rambler.LoggingExtensions import LogService
  >>> ls = LogService()
  >>> import sys
  >>> ls.useStreamHandler(sys.stderr)
  >>> RunLoop.log = ls.__binding__('RunLoop', RunLoop)
  >>> Task.runLoop = RunLoop
  >>> Task.assembled()
  >>> runLoop = RunLoop.currentRunLoop()
  
  Tasks have additional call back methods such as canReadError, willTerminate, didTerminate
  
  >>> class TaskDelegate(object):
  ...   def on_stdout(self, task, data):
  ...       print data
  ...   def on_stdin(self, task, bytes):
  ...     task.stdin.close()
  ...   def on_close(self, task, stream):
  ...      pass
  ...   def on_exit(self, task):
  ...     print task.termination_status
  ...     runLoop.currentRunLoop().stop()  
  

  >>> t = Task()
  >>> t.delegate = TaskDelegate()
  
  >>> t.args = ['-c', 'echo hello world']
  >>> t.launch_path = '/bin/sh'
  >>> t.launch()
  
  
  
  
  Just incase something gose wrong with the test stop the process
  >>> runLoop.waitBeforeCalling(10,  runLoop.stop) #doctest: +ELLIPSIS
  <Rambler.RunLoop.DelayedCall instance at ...>
  >>> runLoop.run()
  hello world
  <BLANKLINE>
  0
  
  In this example, this reads stdin until it is closed and the prints
  the results to standard output. 
  
  >>> t = Task()
  >>> t.delegate = TaskDelegate()
  >>> t.launch_path = 'python'
  
  >>> t.args = ['-c', 'import sys; print sys.stdin.read()']
  >>> t.launch()
  
  Now we write to the tasks stdin, our delegate's on_stdin will be notified when
  the task has received the data, at wich point it will close the stream.
  
  >>> t.write('Hi mom!')
    
  >>> runLoop.run()
  Hi mom!
  <BLANKLINE>
  0
  
  Example of a task exiting with a non-zero status
  
  >>> t = Task()
  >>> t.delegate = TaskDelegate()
  >>> t.launch_path = 'python'
  >>> t.args = ['-c', 'import sys; sys.exit(127)']
  >>> t.launch()
  >>> runLoop.run()
  127
  
  """
  
  runLoop = outlet('RunLoop')
  log = outlet('LogService')
  is_concurrent = True
  __tasks = {}
  
  try:
      MAXFD = os.sysconf("SC_OPEN_MAX")
  except:
      MAXFD = 256

  byte_size = 1024 * 8
  
  @classmethod
  def assembled(cls):
    # Store the currentRunLoop, we'll dispatch all SIG_CHILD to it
    cls.rebase()
    cls.mainRunLoop = cls.runLoop.currentRunLoop()
    signal.signal(signal.SIGCHLD, cls.on_sig_child)
    
  @classmethod
  def on_sig_child(cls, signum, frame):
    """A child has died, we'll figure out which on the next pass through the runLoop"""
    cls.mainRunLoop.callFromThread(cls.reap_child,signum,frame)
    del frame
    
  @classmethod
  def reap_child(cls, signum, frame):
    try:
      try:
        pid, sts = os.waitpid(0,os.WNOHANG)
      except OSError, e:
        if e.errno != errno.ECHILD:
          raise
        pid = 0
        
      if pid == 0:
        cls.log.warn('reap_child called with sig %s but there is no dead child', signum)
      else:
        task = cls.__tasks.pop(pid, None)
  
        if task:
          with task.changing('is_finished', 'is_executing'):
            task.finished = True
            task.executing = False
    
            task._handle_exitstatus(sts)
            if task.delegate:
              task.delegate.on_exit(task)
              task.delegate = None
    finally:
      del frame
      
    
  def __init__(self):
    super(Task,self).__init__()
    
    self.args = []
    self.process_id = None
    self.environment = os.environ
    self.launch_path = None
    self._current_directory = None
    self.delegate = None
    self._rc = None
    self.descriptors = set()

    # number of bytes waiting to be writtent/read by the child. 
    self.bytes_buffered = 0
    self.should_close = False
    
  def start(self):
    """Begins execution of the Operation."""
    if self.process_id is not None:
      raise RuntimeError('Task already started')
      
    if self.is_cancelled:
      with self.changing('is_finished'):
        self.finished = True
      return


    with self.changing('is_executing'):
      self.executing = True 
      self.launch()

  
  def launch(self):
    """Runs the process.
    
    Fork a process, os.dup the file handles to 0,1,2 (stdin, stdout and stderr), close
    any othe open file handles. Then execvp to replace the rambler process with the
    the appropriate command. Fun    
    
    """

    self.log.info(self.launch_path)    
    
    self.read_stdin, self.stdin = os.pipe()
    #self.log.info('\tstdin opened %s %s',self.read_stdin, self.stdin)
    self.stdin = Stream(self.stdin, self)
    
    self.stdout, self.write_stdout = os.pipe()
    #self.log.info('\tstdout opened %s %s',self.stdout, self.write_stdout)
    self.stdout = Stream(self.stdout, self)
    
    self.stderr, self.write_stderr = os.pipe()
    #self.log.info('\tstdout opened %s %s',self.stdout, self.write_stdout)
    self.stderr = Stream(self.stderr, self)
    

    self.process_id = os.fork()
 
    if self.process_id == 0:
      self._execute_child_cmd()
    else:
      Task.__tasks[self.process_id] = self
      # We're the parent we don't use this end
      os.close(self.read_stdin)
      os.close(self.write_stdout)
      os.close(self.write_stderr)
      self.stdout.read(self.byte_size)
      self.stderr.read(self.byte_size)
      
 
  @property
  def current_directory(self):
    return self._current_directory or os.getcwd()
    
  @current_directory.setter
  def current_directory(self, path):
    self._current_directory = path
    
  def terminate(self):
    """Kill the process prematuraly"""
    os.kill(self.process_id, signal.SIGKILL)


  # Process checking code copied from Python's subprocess module
  @property
  def termination_status(self, _deadstate = None):
    if self._rc is None:
      try:
        pid, sts = os.waitpid(self.process_id, os.WNOHANG)
        if pid == self.process_id:
          self._handle_exitstatus(sts)
      except os.error:
        if _deadstate is not None:
          self._rc = _deadstate
    return self._rc
  result = termination_status
  def _handle_exitstatus(self, sts):
    if os.WIFSIGNALED(sts):
      self._rc = -os.WTERMSIG(sts)
    elif os.WIFEXITED(sts):
      self._rc = os.WEXITSTATUS(sts)
    else:
      # Should never happen
      raise RuntimeError("Unknown child exit status!")
  
    
  def read(self, bytes):
    """Read up to XX number of bytes from the task's standard output. The read may be short 
    if the data is not available. Typically this method is invoked from a delegates canRead
    method.
    """

    self.stdout.read(bytes)
    self.stderr.read(bytes)
    
  def onRead(self, stream, data):

    if self.delegate:
      if stream == self.stdout:
        self.delegate.on_stdout(self, data)
      elif stream == self.stderr:
        if hasattr(self.delegate, 'on_stderr'):
          self.delegate.on_stderr(self, data)
    stream.read(self.byte_size)
      
  def onClose(self, stream):
    if self.delegate:
      self.delegate.on_close(self, stream)
      
  def onWrite(self, stream, bytes):
    self.bytes_buffered -= bytes
    
    if self.should_close and self.bytes_buffered == 0:
      self.stdin.close()
    
    if self.delegate:
      self.delegate.on_stdin(self, bytes)
      
  def onError(self, stream, error):
    self.log.error('Task %s error %s %s', self.launch_path, stream, error)

     
  def write(self, data):
    """Writes data to the Child's standard input."""
    if self.should_close or self._rc is not None:
      self.log.error('Write after close to script %s', self.launch_path)
    else:
      self.bytes_buffered += len(data)
      self.stdin.write(data)
    
  def close(self):
    """Flags stdin for closure when all outstanding bytes have been written"""
    self.should_close = True
    if self.bytes_buffered == 0:
       self.stdin.close()

  def add_fd(self, fd):
    """Add a file handle that will be kept open by the task after it forks"""
    self.descriptors.add(fd)
    
  def _execute_child_cmd(self):
    """Replace the currently forked process with a brand new one. 
    
    Note the execution never returns from this method. If some exception 
    happens w/o calling sys._exit you'll end up with two copies of the parent
    process running. Not good.    
    """
    try:
      os.chdir(self.current_directory)
      # Close handles not used by the child
      self.stdin.close()
      self.stdout.close()
    
      # Move the write end of the pipe to descriptor 0
      os.dup2(self.read_stdin, 0)
      os.close(self.read_stdin)  
      os.dup2(self.write_stdout, 1)
      os.close(self.write_stdout)
      os.dup2(self.write_stderr, 2)
      os.close(self.write_stderr)
      
    
      # Close any other open file descriptors
      self._close_fds()
    
      if self.environment is None:
        os.execvp(self.launch_path, [self.launch_path] + self.args)
      else:
        os.execvpe(self.launch_path, [self.launch_path] + self.args, self.environment)
    except:
      # We had an error after forking, but before replacing the process. 
      exc_type, exc_value, tb = sys.exc_info()
      # Save the traceback and attach it to the exception object
      exc_lines = traceback.format_exception(exc_type,
                                              exc_value,
                                              tb)
      os.write(2, 'Failed to execute %s\n' % self.launch_path)
      os.write(2, '\n'.join(exc_lines))
          
    # If we've reached this point we've failed to launch, most likely the cmd pointed to
    # an invalid path.
    os._exit(255)
    
  def _close_fds(self):
    """Closes all but the first 3 file handles, in the future we may expand 
    Task to allow you to specify keeping open certain handles. 
    """
    for fd in range(3, self.MAXFD):
      if fd not in self.descriptors:
        try:
          os.close(fd)
        except OSError:
          pass
        
        