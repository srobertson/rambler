# module grammar.py
# Copyright (c) 2009  Scott Robertson

from __future__ import with_statement
from collections import deque, defaultdict
import copy
import string
import sys
import types
import cStringIO


alphas = string.ascii_letters
nums = '0123456789'
alpha_nums = alphas+nums

class State(object):
  
  def __init__(self, name=None):
    self.name = name
    self.delegates = []
    self.parent = None
  
  def __call__(self, **kw):
    return self.copy(**kw) 
     
  def __repr__(self):
    if getattr(self, 'parent', None) is not None:
      chain = repr(self.parent) + '<-'
    else:
      chain = ''
    return "<%s%s{%s}>" % (chain, self.__class__.__name__, self.name or '')
    
  def __deepcopy__(self, memo):
    # deep copy everything except the delegates

    clone = self.__class__.__new__(self.__class__)
    memo[id(self)] = clone
    for attr,value in  self.__dict__.items():
      if attr != 'delegates':
        setattr(clone,attr, copy.deepcopy(value, memo))
      else:
        setattr(clone, attr, value)

    return clone
    

  def copy(self, **kw):
    clone = copy.deepcopy(self)
    for attr,value in kw.items():
      setattr(clone, attr,value)
      
    return clone

  def add_delegate(self, delegate):
    """Add a delegate which will receive on_enter and on_exit notifications"""
    self.delegates.append(delegate)
    return self
    
  def remove_delegate(self, delegate):
    """Removes a delegate from the list that receives notifications"""
    self.delegates.remove(delegate)
    
  def failed(self, rstack, parser):
    """Called by the parser when all possible sub paths shifted in by the State
    have been rejected. The state will be the current State in the Stack. The default
    behavior is to call reject signaling the current State has failed as well."""
    if self.parent:
      parser.reject(rstack)
    else:
      raise ParseError(rstack[0], "could not parse" )
      
    
  def reduced(self, child, accepted, parser):
    """Called when a child has reduced. Gives the parent a chance
    to alter the state of the parser.
    
    Default behavior is to assume parsing is done at this level as well."""
    
    parser.reduce(accepted)
    
  def feed(self, char, parser):
    """Called by the parser to give the state a chance to process the given input.
    
    Before exciting the function the State object must call one of parsers 4 functions.
    
    parser.accept() : The State accepted the input and wants more.
    parser.reduce() : The State has seen enough characters
    parser.reject() : The State could not parse the input
    parser.shift(state1, ..., stateN ): One or more States that should receive input in parallel
         
    """
    
  def has_ancestor(self, parent):
    if self.parent is None:
      return False
    elif self.parent == parent:
      return True
    else:
      return self.parent.has_ancestor(parent)
      
  def callbacks_for(self, method):
    for delegate in self.delegates:
      callback = getattr(delegate, method, None)
      if callback:
        yield callback

    
  def enter(self):
    for callback in self.callbacks_for('on_enter'):
      callback(self)
    
  def exit(self):
    for callback in self.callbacks_for('on_exit'):
      callback(self)
    

class Literal(State):
  """Accumulates characters as long as they match the literal in exact sequence
  
  
  >>> class Delegate(object):
  ...   def on_exit(self, state):
  ...     self.found = True
  
  >>> delegate = Delegate()
  >>> HTTP = Literal('HTTP').add_delegate(delegate)
  >>> with Parser(HTTP) as parser:
  ...  for c in "HTTP":
  ...    parser.feed(c)
  
  >>> delegate.found
  True
  
  """
  def __init__(self, literal, **kw):
    super(Literal, self).__init__(**kw)
    self.literal = literal
    self.length = len(literal)
  
  def __repr__(self):
    return "<%s{%s}:%s>" % (self.__class__.__name__, self.name or '', repr(self.literal))
    
  def enter(self):
    self.pos = 0
    self.expecting = self.literal[self.pos]
    super(Literal, self).enter()
  
  def feed(self, char, parser):
    if self.expecting == char:
      self.pos += 1
      if self.pos < self.length:
        self.expecting = self.literal[self.pos]
        parser.accept()
      else:
        parser.reduce(True)
    else:
      parser.reject()
      
  @property    
  def value(self):
    return self.literal
      
class Octet(State):
  """Reduces when any 8bit character that meets certain criteria is encountered.
  By default any 8 bit char is accepted.
  
   By defaul Octet will accept
    
    >>> with Parser(Octet()) as parser:
    ...   parser.feed('\\n')
    
    Octet's can be constrained to a specifc set of characters. For example 
    uppercase letters
    
    >>> UpperCase = Octet('A-Z')
    
    >>> with Parser(OneOrMore(UpperCase)) as parser:
    ...   for c in "ABCEDEFG":
    ...      parser.feed(c)
    
    
    Or you can pass in a numerical range
    >>> UpperCase = Octet((ord('A'), ord('Z')))
    
    >>> with Parser(OneOrMore(UpperCase)) as parser:
    ...   for c in "ABCEDEFG":
    ...      parser.feed(c)
    
    More than one range can be used. For example the range of 
    Upper and Lowercase letters.
    
    >>> Letters = Octet(UpperCase, 'a-z')
    >>> with Parser(OneOrMore(Letters)) as parser:
    ...   for c in "ABCeDeFG":
    ...      parser.feed(c)
    
   """
  
  def __init__(self, *include, **kw):
    """
    Arguments:
    include: A two dimentional sequence of range tests. If the octet return true
    for any test then it is accepted.

    
    """
    super(Octet,self).__init__(**kw)
    if len(include) == 0:
      # except any 8bit char by default
      include = ((0,255),)
    
    self.include = []
    for allowed in include:
      if isinstance(allowed, basestring):
        # if it's a string it should either be a single character 'a' or a two chars seperated
        # by a dash such as 'a-z'
        if len(allowed) == 1:
          self.include.append((ord(allowed),ord(allowed)))
        else:
          low, high = map(ord, allowed.split('-',1))
          assert low <= high
          self.include.append((low, high))
      elif isinstance(allowed, Octet):
        self.include.extend(allowed.include)
      elif len(allowed) == 1: # assume it's a number
        self.include.append((allowed, allowed))
      else: # assume it's a tuple of two numbers
        self.include.append(allowed)
          
  
  def enter(self):
    self.value = ""
    super(Octet,self).enter()
  
  def feed(self, char, parser):
    if char is END_OF_INPUT:
      parser.reject()
    else:
      num = ord(char)
      for low,high in self.include:
        if  low <= num <= high:
          self.value = char
          parser.reduce(True)
          return
      else:
        parser.reject()
      

class Word(State):
  """Accumulates one or more characters as long as they are in the allowed set. Delegates
  interested in the value can check it in the on_exit handler.
  
  >>> class Delegate(object):
  ...  def on_exit(self, state):
  ...    self.value = state.value
  
  >>> delegate = Delegate()

  >>> with Parser(Word().add_delegate(delegate)) as parser:
  ...   for c in 'blah':
  ...     parser.feed(c)
  
  >>> delegate.value
  'blah'
    
  """

  def __init__(self, start=alpha_nums, allowed=None, **kw):
    super(Word, self).__init__(**kw)
    self.start = start
    self.allowed = allowed or start
    self.buffer = deque()
    
  def __repr__(self):
    return "<%s{%s} %s>" % (self.__class__.__name__, self.name or '', self.value)

  def enter(self):
    self.buffer.clear()
    super(Word,self).enter()
    
  def exit(self):
    super(Word, self).exit()
  
  def feed(self, char, parser):
    if len(self.buffer):
      allowed = self.allowed
    else:
      allowed = self.start
      
    if char is not END_OF_INPUT and char in allowed:
      self.buffer.append(char)
      parser.accept()
    elif len(self.buffer):
      parser.reduce(False)#char is END_OF_INPUT)
    else:
      parser.reject()
    
  @property
  def value(self):
    return "".join(self.buffer)
    
    
class Compound(State):
  """Abstract Class for State's that are composed of multiple sub states.
  
  """
  
  def __init__(self, *states, **kw):
     super(Compound,self).__init__(**kw)
     self.states = []
     for s in states:
       if isinstance(s, basestring):
         self.states.append(Literal(s))
       else:
         self.states.append(s)
  
  
class And(Compound):
  """
  Ensures a series of states are parsed one after the other
  
  >>> class Delegate(object):
  ...   states = []
  ...
  ...   def on_exit(self, state):
  ...     self.states.append(state.value)
  
  >>> delegate = Delegate()
  
  >>> Method =  Word(name='method').add_delegate(delegate)
  >>> SP = Literal(' ')
  >>> Request_URI = Word(alpha_nums+'/', name='request_uri').add_delegate(delegate)
  >>> Request_Line = And(Method, SP, Request_URI)
  
  >>> with Parser(Request_Line) as parser:
  ...  for c in "GET /foo":
  ...    parser.feed(c)
  
  >>> delegate.states
  ['GET', '/foo']
  
  >>> delegate.states = []
  
  If short a ParseError will be raised
  >>> try:
  ...   with Parser(Request_Line) as parser:
  ...     for c in "GET ":
  ...       parser.feed(c)
  ...   assert True, 'should have raised ParseError'
  ... except ParseError:
  ...   pass
  
  """
  
  
  def enter(self):
    self.current = -1
    super(And, self).enter()
    
  def feed(self, char, parser):
    self.current += 1
    if self.current < len(self.states):
      next = self.states[self.current]
      parser.shift(next)
#    elif char == END_OF_INPUT:
#      # we've read enough and it's the END_OF_INPUT
#      parser.reduce(True)
    else:
      # we've read enough, we'll reduce, hopefully someone else will handle
      # this char
      parser.reduce(False)
      
  def reduced(self, state, accepted, parser):      
    if self.current + 1 == len(self.states):
      parser.reduce(accepted)
    elif accepted:
      parser.accept()
  
  @property
  def value(self):
    # not safe to call if sub states.value() does not return a char
    for state in self.states:
      if type(state.value) == types.GeneratorType:
        for part in state.value:
          yield part
      else:
        yield state.value
    #"".join([state.value for state in self.states])


class IgnoreWhiteSpace(And):
  """Ignore's white space in between parse expressions.
  
  Usage:
  
  >>> class Delegate(object):
  ...   def __init__(self):
  ...     self.field_name = None
  ...     self.field_value = None
  ...   def on_exit(self, state):
  ...     if state.name == 'field_name':
  ...       self.field_name = state.value
  ...     elif state.name == 'field_value':
  ...       self.field_value = state.value
  
  >>> delegate = Delegate()
  
  >>> field_name = Word(name="field_name").add_delegate(delegate)
  >>> Sep = Literal(':')
  >>> field_value = Word(name="field_value").add_delegate(delegate)

  
  >>> with Parser(IgnoreWhiteSpace(field_name, Sep, field_value)) as parser:
  ...   for c in "   this  :   test  ":
  ...     next = parser.feed(c)
  
  >>> delegate.field_name
  'this'
  >>> delegate.field_value
  'test'
  
  """
  def __init__(self, *states, **kw):
    self.whitespace = kw.pop('whitespace', string.whitespace)
    super(IgnoreWhiteSpace, self).__init__(*states, **kw)
    
  def feed(self, char, parser):
    if char != END_OF_INPUT and char in self.whitespace:
      # consume the white space
      parser.accept()
    else:
      # If it's not whitespace act like And
      super(IgnoreWhiteSpace, self).feed(char, parser)

  def reduced(self, state, accepted, parser):
    # keep reading... wonder if this should be an option
    if accepted:
      parser.accept()
      


class Octets(State):
  """Accumulates characters until content_length is reached
  
  Arguments:
    content_length: Number of characters to accumulate.
    
  Errors:
    Throws parse error if not enough characters were read by the
    time exit() is called.
    
  Example:
  
  >>> class Delegate(object):
  ...   def on_exit(self, state):
  ...     self.value = state.value

  >>> delegate=Delegate()
  >>> Data = Octets(content_length=5).add_delegate(delegate)
  
  >>> with Parser(Data) as parser:
  ...   for c in "12345":
  ...     parser.feed(c)
  
  >> delegate.value
  '12345'
    
  """
  def __init__(self, content_length=1,  **kw):
    super(Octets, self).__init__(**kw)
    self.content_length = content_length
    self.buffer = cStringIO.StringIO()#deque()
    
  def enter(self):
    #self.buffer.clear()
    self.buffer.reset()
    self.buffer.truncate()
    super(Octets,self).enter()
    
  def feed(self, char, parser):
    if not parser.end_of_input:
      self.buffer.write(char)
      if self.length() == self.content_length:
        parser.reduce(True)
      else:
        parser.accept()
        
    else:
      parser.reduce(True)
      
  def length(self):
    return self.buffer.tell()
    
  @property
  def value(self):
    return self.buffer.getvalue()
    #return "".join(self.buffer)

class Or(Compound):
  """Given a list of states feeds characters to one state at a time until
  it finds a state that can successfully accept the stream.
  
  Example:
  
  >>> class Delegate(object):
  ...   def on_exit(self, state):
  ...     self.seen = state
  ...     if state.name == 'OTHER':
  ...       self.value = state.value
  
  >>> delegate = Delegate()
  >>> GetMethod = Literal('GET', name="GET").add_delegate(delegate)
  >>> PostMethod = Literal('POST', name="POST").add_delegate(delegate)
  >>> PutMethod = Literal('PUT', name="PUT").add_delegate(delegate)
  >>> Other = Word(name="OTHER").add_delegate(delegate)
  >>> RequestMethod = Or(GetMethod, PostMethod, PutMethod, Other)
  
  >>> with Parser(RequestMethod) as parser:
  ...   for c in "GET":
  ...     parser.feed(c)
  
  >>> delegate.seen.name
  'GET'
  
  >>> parser.pos
  2
  
  >>> with Parser(RequestMethod) as parser:
  ...   for c in "POST":
  ...     parser.feed(c)
  
  >>> delegate.seen.name
  'POST'
  
  >>> parser.pos
  3
  
  >>> with Parser(RequestMethod) as parser:
  ...   for c in "PUT":
  ...     parser.feed(c)
  
  >>> delegate.seen.name
  'PUT'
  
  >>> parser.pos
  2
  
  >>> with Parser(RequestMethod) as parser:
  ...   for c in "blah":
  ...     parser.feed(c)
  
  >>> delegate.seen.name
  'OTHER'
  
  >>> delegate.value
  'blah'
  
  >>> parser.pos
  3
  """
  
  def enter(self):
    self.count = 0
    self.succeeded = None
    super(Or,self).enter()
    
  def exit(self):
    # Note, we use And's Superclass not Or, to avoid
    # the check that all tokens were consumed
    if self.count == 1:
      super(Or,self).exit()
    else:
      raise ParseError(self, "Expected to parse one of %s" % self.states)
      
    
  def feed(self, char, parser):
    if self.count == 0:
      parser.shift(*self.states)
    else:
      parser.reject()
    
#  def failed(self, rstack, parser):
#    pass
  
  def reduced(self, child, accepted, parser):
    self.count += 1
    self.succeeded = child
    parser.reduce(accepted)
    
  @property
  def value(self):
    if self.succeeded:
      return self.succeeded.value

class Optional(And):
  """Makes a given state Optional. It accomplishies this by determining the "next state"
   that it's parent will run in sequence and runs it in parallel with the Optional State.
   If the Optional state succeeds before the "next state", the "next state" will be run 
   as normal by it's parent. If the next state succeeds first, Optional will manipulate 
   the parent to think that it's already ran the "next state" (because it has).
   
                           And
                            ^
                          /   \
                        /       \
          Optional(SomeState)  State
  
  >>> class Delegate(object):
  ...   def on_enter(self,state):
  ...     if state.name == 'host':
  ...       self.host = None
  ...       self.port = None
  ...   def on_exit(self, state):
  ...     setattr(self, state.name, state.value)

  
  >>> delegate = Delegate()
  >>> port = Optional(':', Word(string.digits, name="port").add_delegate(delegate), name='port_option')        
  >>> hostport = And(Word(name="host").add_delegate(delegate), port , '/')
  >>> with Parser(hostport) as parser:
  ...   for c in "localhost:8085/":
  ...     parser.feed(c)
  
  >>> delegate.host
  'localhost'
  >>> delegate.port
  '8085'
  
  >>> with Parser(hostport) as parser:
  ...   for c in "localhost/":
  ...     parser.feed(c)
  
  >>> delegate.host
  'localhost'
  >>> delegate.port is None
  True
  
  >>> try:
  ...   with Parser(hostport) as parser:
  ...     for c in "localhost:a/":
  ...       parser.feed(c)
  ...   assert False, "should raise ParseError"
  ... except ParseError:
  ...   pass
  
  The next stat does not have to be a sibling of the Optional State, it could be a cousin
  like this
                         And
                          ^
                        /   \
                     And  CousinState
                      ^
                    /   \
                  /       \
              State    Optional(SomeState)
  
  
  >>> hostport = And(Word(name="host").add_delegate(delegate), port, name='hostport')
  >>> hostport_path = And(hostport, '/', name='hostport_path')


  >>> with Parser(hostport_path) as parser:
  ...   for c in "localhost/":
  ...     parser.feed(c)
  
  >>> delegate.host
  'localhost'
  >>> delegate.port
  
  >>> with Parser(hostport_path) as parser:
  ...   for c in "localhost:8080/":
  ...     parser.feed(c)
  
  >>> delegate.host
  'localhost'
  >>> delegate.port
  '8080'
  
  
  Optional with no siblings/cousins should not fail
  >>> hostport_path = And(hostport, Optional('/', name='/'), name='hostport_path')
  >>> with Parser(hostport_path) as parser:
  ...   for c in "google:8080":
  ...     parser.feed(c)
  
  >>> delegate.host
  'google'
  >>> delegate.port
  '8080'
  
  """
  def __init__(self, *states, **kw):
    super(Optional,self).__init__(*states,**kw)
    self.optional = self.states[0]
    self.found = False

    
  def enter(self):
    self.found = False
    super(Optional, self).enter()

     
  def feed(self, char, parser):
    # search for the next statement after this
    if self.current == -1:
      self.current += 1
      self.next_parent, self.next_state = self.find_next()
      if self.next_state:
        parser.shift(self.optional, self.next_state)
      else:
        parser.shift(self.optional)
    else:
      super(Optional,self).feed(char,parser)

      
  def reduced(self, state, accepted, parser):
    if state is self.next_state:
       parser.reduce(accepted)
       return
    elif state is self.optional:
      self.found = True
      if self.next_parent:
        # The Optional state reduced, we want the parent to rerun the cousin
        # state
        self.next_parent.current -= 1
    
    super(Optional, self).reduced(state, accepted, parser)

  def failed(self, rstack, parser):
    if self.next_state is None:
      # It's ok if our state rejected the input, if we're the last state in the parse tree
      parser.reduce(parser.end_of_input)
    else:
      super(Optional,self).failed(rstack, parser)
    #  raise ParseError(self, "optional state and cousin state rejected input")
      
    
  def find_next(self):
    
    node = self
    opt_count = 0
    while(node.parent is not None):
      node = node.parent
        
      if isinstance(node, And):
        parent = node
        pos = parent.current + 1 
        if pos < len(parent.states):
          cousin = parent.states[pos]
          if isinstance(cousin, Optional):
            cousin = And(*cousin.states)
          parent.current = pos
          return parent, cousin

    return None, None
    #raise ParseError(self, "Optional must have an And as an Ancestor ")
    
  @property
  def value(self):
    if self.found:
      return super(Optional,self).value
    else:
      return ''



class RepeatAtLeast(State):
  """Given a state repeat it at least X number of times.
  
  >>> char_a = Literal('A')
  
  RepeatAtLeast must be called with a state and the minimum number of
  occurances to expect, which can be Zero
   
  >>> with Parser(RepeatAtLeast(char_a, 0)) as parser:
  ...   for c in '':
  ...     parser.feed(c)
  
  
  Or some number greater than 0 in which case the state must exit 
  that many times
  >>> with Parser(RepeatAtLeast(char_a, 3)) as parser:
  ...   for c in 'AAA':
  ...     parser.feed(c)
  
  If it's not a parser error is thrown.
  >>> try:
  ...   with Parser(RepeatAtLeast(char_a, 3)) as parser:
  ...     for c in 'A':
  ...       parser.feed(c)
  ...   assert False, "parse error should have been thrown"
  ... except ParseError:
  ...   pass

  It's perfectaly acceptable to recive more of the input that a state expects
  as the minimum.
  >>> with Parser(RepeatAtLeast(char_a, 1)) as parser:
  ...   for c in 'AAA':
  ...     parser.feed(c)
  
  """
  def __init__(self, state, expected, **kw):
    super(RepeatAtLeast, self).__init__(**kw)
    if isinstance(state, basestring):
      state = Literal(state)
    self.state = state
    self.expected = expected
    self.collection = []
    
  def enter(self):
    self.collection = []
    super(RepeatAtLeast, self).enter()
    
  def exit(self):
    if len(self.collection) < self.expected:
      raise ParseError(self, "expected at leas %s of %s" % (self.expected, self.state))
    super(RepeatAtLeast, self).exit()
      
  def feed(self, char, parser):
    if char is END_OF_INPUT:
      if len(self.collection) < self.expected:
        parser.reject()
      else:
        parser.reduce(True)
    else:
       parser.shift(self.state())
    
  def reduced(self, child, accepted, parser):
    self.collection.append(child)
    if parser.end_of_input:
      parser.reduce(accepted)
    else:
      parser.shift(self.state())
      if accepted:
        parser.accept()
    
  def failed(self, rstack, parser):
    if len(self.collection) < self.expected:
      parser.reject(rstack)
    else:
      parser.reduce(False)
  #
  @property
  def value(self):      
    for state in self.collection:
      if type(state.value) == types.GeneratorType:
        for part in state.value:
          yield part
      else:
        yield state.value

class ZeroOrMore(RepeatAtLeast):
  """Shorthand for RepeatAtLeast(state,0)
  
  >>> char_a = Literal('A')
  
  RepeatAtLeast must be called with a state and the minimum number of
  occurances to expect, which can be Zero
   
  >>> zero_or_more_a = ZeroOrMore(char_a)
  >>> with Parser(zero_or_more_a) as parser:
  ...   for c in '':
  ...     parser.feed(c)
  
  >>> ''.join(zero_or_more_a.value)
  ''
  
  >>> with Parser(zero_or_more_a) as parser:
  ...   for c in 'AAA':
  ...     parser.feed(c)
  
  >>> ''.join(zero_or_more_a.value)
  'AAA'
  
  
  """
  
  def __init__(self, state, **kw):
    super(ZeroOrMore, self).__init__(state, 0, **kw )


class OneOrMore(RepeatAtLeast):
  """Shorthand for RepeatAtLeast(state,1)
  
  Example
  
  >>> class Delegate(object):
  ...   def __init__(self):
  ...     self.headers = {}
  ...     self.field_name = None
  ...
  ...   def on_exit(self, state):
  ...     if state.name == 'field_name':
  ...       self.field_name = state.value
  ...     elif state.name == 'field_value':
  ...       self.headers[self.field_name] = state.value
  
  >>> delegate = Delegate()
  
  >>> field_name = Word(alpha_nums+'-', name="field_name").add_delegate(delegate)
  >>> Sep = Literal(':')
  >>> field_value = Word(name="field_value").add_delegate(delegate)
  >>> CRLF = Literal('\\r\\n')
  >>> message_header = IgnoreWhiteSpace(field_name, Sep, field_value, CRLF,whitespace=' ')
  
  
  >>> Request=And(OneOrMore(message_header), CRLF, Octets(content_length=4))
  
  >>> with Parser(Request) as parser:
  ...   for c in "   this  :   test  \\r\\nContent-Length: 4\\r\\n\\r\\n1234":
  ...     next = parser.feed(c)
  
  >>> delegate.headers
  {'this': 'test', 'Content-Length': '4'}
  """
  def __init__(self,state, **kw):
    super(OneOrMore, self).__init__(state, 1, **kw )
    
        
class Enclosure(State):
  """Test that a certain state(start) is encountered then repeatedly attempts to
   parse the state until the the end state is encountered. end defaults to the
   same state object as start if not specefied. 
   
   >>> class Delegate(object):
   ...   def on_enter(self, state):
   ...      if state.name == 'string':
   ...        self.seen = ""
   ...   def on_exit(self, state):
   ...     if state.name == "octet":
   ...       self.seen += state.value
   
   >>> delegate = Delegate()
   >>> Quote = Literal('"')
   >>> octet = Octet(name='octet').add_delegate(delegate)
   >>> QuotedString = Enclosure(Quote, octet, name="string").add_delegate(delegate)
   >>> with Parser(QuotedString) as parser:
   ...   for c in '"blah"':
   ...     parser.feed(c)
   
   >>> delegate.seen
   'blah'
   
   In this example we create an Enclosure using the Literal '(' to start and ')' to end.
   >>> delegate = Delegate()
   >>> Left = Literal('(')
   >>> Right = Literal(')')
   >>> octet = Octet(name='octet').add_delegate(delegate)
   >>> Parantheses = Enclosure(Left, octet, end=Right, name="string").add_delegate(delegate)
   >>> with Parser(Parantheses) as parser:
   ...   for c in '(hi mom)':
   ...     parser.feed(c)

   >>> delegate.seen
   'hi mom'
    
   The enclosure will raise a ParseError if the start state rejects it's input.
   >>> try:
   ...   with Parser(Parantheses) as parser:
   ...     for c in ' (hi mom)':
   ...      parser.feed(c)
   ...   assert False, "should have raised ParseError"
   ... except ParseError:
   ...   pass

   >>> delegate.seen
   ''
   
   In the above example the leading space caused an error, wrapping Parantheses
   in an IgnoreWhiteSpace should allaw for the input to start with a space. Note
   how the spaces in the Enclosure are protecte (not ignored by the IgnoreWhiteSpace)
   
   >>> with Parser(IgnoreWhiteSpace(Parantheses)) as parser:
   ...  for c in ' (hi mom) ':
   ...    parser.feed(c)
   
   >>> delegate.seen
   'hi mom'
 
   
   """
  def __init__(self, start, repeat, end=None, **kw):
    self.start = start
    self.repeat = repeat
    self.end = end or start
    super(Enclosure, self).__init__(**kw)
    
  def enter(self):
    self.started = False
    super(Enclosure, self).enter()
    
  def feed(self, char, parser):
    parser.shift(self.start)
    
  def reduced(self, state, accepted, parser):
    if self.started and state is self.end:
      parser.reduce(accepted)
    else:
      # keep shipting state and end until end wins
      self.started = True
      parser.shift(self.end, self.repeat)
      if accepted:
        # make sure the two states we just inserted
        # don't see input until the next call to parser.feed()
        parser.accept()
        parser.accept()
    
    
class EndOfLine(Literal):
  def __init__(self, terminator='\n', **kw):
    super(EndOfLine, self).__init__(terminator,**kw)

    
class Until(State):
  """Accumulates characters until the given terminator is encountered.
  
  Arguments:
    terminator: the charcters we wish to match
    
  Optional:
    maxlength: raise an error if terminator not found by maxlength
    
  Notes: This is supper greedy, to avoid reading to much data you should
  set maxlength
  """
  
  def __init__(self, terminator, maxlength=None, **kw):
    super(Until,self).__init__(**kw)
    self.terminator = terminator
    self.maxlength = None

    
  def enter(self):
    # list supports slice, in exchange for poor memory performance compared to deque
    self.buffer = []
    # calculate len here incase an event handler modified terminator after
    # instantiation... could happen
    self.term_len = len(self.terminator)
    self.term_found = False
    super(Until, self).enter()
    
  def exit(self):
    if not self.term_found:
      raise ParseError(self, "%s not found" % self.terminator)
    super(Until, self).exit()
    
  def feed(self,ch):
    self.buffer.append(ch)
    if self.buffer[-self.term_len:] == self.terminator:
      self.term_found = True
      return None
    elif self.maxlength is not None and len(self.buffer) > self.maxlength:
      raise ParseError(self,'Read to many characters before finding terminator %s' % self.terminator)
    else:
      return self
      
  @property
  def value(self):
    return "".join(self.buffer)
  



LineEnd = EndOfLine()
    

MARKER = ()
END_OF_INPUT = MARKER
class Parser(object):

  def __init__(self, grammar, last=40, debug=False):
    self.grammar = grammar
    self.last_chars = deque()
    self.last = last
    self.callbacks = defaultdict(list)
    self.modified = False
    self.op_count = 0
    # number of operations shifts, reduces etc... that can happen before
    # throwing an error. Used to prevent infinite cycles
    self.max_op_count = 30
    self.debug = debug
    
  def __enter__(self):
    self.end_of_input = False
    self.grammar.enter()
    self.stack = deque((MARKER, self.grammar))
    self.pos = -1
    self.last_chars.clear()
    return self

  def __exit__(self,ex_type, value,traceback):
    if ex_type is None: # no error
      if len(self.stack) > 1:
        # Sholud we count the END_OF_INPUT as current pos or not?
        self.pos -= 1
        self.end_of_input = True
        try:
          self.feed(END_OF_INPUT)
        except ParseError, e:
          self.annotate_error(e)
          raise

        assert len(self.stack) == 1, "Only MARKER should remain not %s" % self.stack
    else:
      if ex_type == ParseError:
        self.annotate_error(value)
  
  def annotate_error(self, error):
    error.pos = self.pos
    error.last = "".join(self.last_chars)
  
        
  def add_callback(self, callback, name=None):
    """Add delegate to hear on_reduce.
    
    Arguments:
    delegate: object expected to have callback method.
    name: Restricts on_reduce calls to be made only when a state with the specified name reduces. 
    By default delegate will receive on_reduce for every State object regardless of name. 
    """
    self.callbacks[name].append(callback)
  
  def remove_delegate(self, callback, name=None):
    self.callbacks[name].remove(callback)
    
  def callbacks_for(self, method, name):
    for callback in self.callbacks[None]:
      yield callback
        
    if name is not None and self.callbacks.has_key(name):
      for callback in self.callbacks[name]:
        yield callback
      
    
  def on_reduce(self, state):
    """Called when any state reduces succesfully"""
    for callback in self.callbacks_for('on_reduce', state.name):
      callback(state)
      
    
  def feed(self, char):
    if len(self.last_chars) > self.last:
      self.last_chars.popleft()
      
    if char is not END_OF_INPUT:
      self.last_chars.append(char)
    self.op_count = 0
    self.pos += 1
    self.stack.rotate(-1)
    next = self.stack[0]
    if next is MARKER:
      raise ParseError(next, "trailing charcters after parser finished")
    if self.debug == True:
      print >> sys.stderr, repr(char)
    while next is not MARKER:
      if self.debug == True:
        print >> sys.stderr, '  %s' % next
      self.op_count += 1
      if self.op_count > self.max_op_count:
        raise ParseError(self, 'maximum number of operations exceeded parsing %s' % str(char))
      self.modified = False
      next.feed(char,self)
      assert self.modified, "state did not modify parser "
      next = self.stack[0]

        
  def accept(self):
    self.modified = True
    self.stack.rotate(-1)

  
  def shift(self, *states):
    self.modified = True
    parent = self.stack.popleft()
    
    for state in reversed(states):
      state.parent = parent
      state.enter()
      self.stack.appendleft(state)

      
  def reject(self, rstack=None):
    self.modified = True
    if rstack is None:
      rstack = deque()
      
    failed = self.stack.popleft()
    if self.debug == True:
      print >> sys.stderr, ' X%s' % failed
    
    parent = failed.parent
    if parent:
      # Notify parent that all paths failed if there are no more children in the stack
      for path in self.stack:
        if path != MARKER and path.has_ancestor(parent):
          # found a path that's still active
          return
      self.stack.appendleft(parent)
      rstack.append(failed)
      failed.parent = None
      parent.failed(rstack, self)
    else:
      # Top of the stack and no one handled the error
      raise ParseError(rstack[0], 'parse error')

      
  def reduce(self, accepted):
    self.modified = True
    state = self.stack.popleft()
    state.exit()
    self.on_reduce(state)
    parent = state.parent
    
    if self.debug == True:
      print >> sys.stderr, ' R%s' % state
    
    # remove any other branches that the parent may have inserted
    for x in range(len(self.stack)):
      victim = self.stack.popleft()
      if victim is MARKER:
        self.stack.append(MARKER)
      elif not victim.has_ancestor(parent):
        self.stack.append(victim)
      elif self.debug:
        # pruned
        print >> sys.stderr, ' P%s' % victim
      
    if parent:
      self.stack.appendleft(parent)
      parent.reduced(state, accepted, self)
    elif accepted == False and len(self.stack) == 1 and not self.end_of_input:
      # there's at least on character that hasn't been consumed, but no one
      # left to try
      raise ParseError(self, 'no one accepted')
      
      
      

class ParseError(Exception):
  def __init__(self, state, message=""):
    self.state = state
    self.message = message
    # this should be set by the parser if it's caught
    self.pos = ''
    self.last = ''
    
  def __str__(self):
    return "%s encountered '%s' at %s\n%s" % (self.state, self.message, self.pos, repr(self.last))


ALPHA         = Octet('A-Z','a-z')
DIGIT         = Octet('0-9')
ALPHANUM      = Octet(ALPHA, DIGIT)
HEXDIG        = Octet(DIGIT, 'A-F', 'a-f')


if __name__ == "__main__":
  import doctest
  doctest.testmod()