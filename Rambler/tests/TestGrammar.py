from __future__ import with_statement 

import unittest
import sys

from Rambler.grammar import *

class KeyValue(object):
  """Observer parsing for keys and values. 
  """
  def __init__(self):

    self.headers = {}
    self.body = None

    # Contstruct our grammar object    
    Key = Word(name='key', rest=alpha_nums + '-')
    Key.add_delegate(self)

    Value = Word(name='value')
    Value.add_delegate(self)

    Header = And(Key, Literal(':'), Value, LineEnd)
    Headers = OneOrMore(Header, name="headers")
    Headers.add_delegate(self)

    Payload = Chunk(name='body')
    Payload.add_delegate(self)
    self.payload=Payload

    Send = And(Literal('send'), LineEnd, Headers, LineEnd, Payload)
    
    self.parser = Parser(Send)
    
  def on_enter(self, state):
    #print >> sys.stderr, "enter", state
    pass
          
  def on_exit(self, state):
    #print >> sys.stderr, "exit", state
    if state.name == 'key':
      self.key = state.value
    elif state.name == 'value':
      self.headers[self.key] = state.value
    elif state.name == 'headers':
      content_length = self.headers.get('content-length')
      if content_length:
        self.payload.content_length = int(content_length)
    elif state.name == 'body':
      self.body = state.value
      
      
class TestGrammar(unittest.TestCase):
    
  def testParseExactContent(self):
    packet = KeyValue()

    with packet.parser as parser:
      for c in 'send\ndestination:foo\nblah:1\ncontent-length:4\n\nabcd':
        parser.feed(c)
        
    self.assertEquals(packet.headers, {'blah': '1', 'content-length': '4', 'destination': 'foo'})
    self.assertEquals(packet.body, 'abcd')

        
  def testParseTrailingContent(self):
    packet = KeyValue()

    try:
      with packet.parser as parser:
        for c in 'send\ndestination:foo\nblah:1\ncontent-length:4\n\nabcdef':
          parser.feed(c)
      self.fail("ParseError expected")
    except ParseError:
      pass
    
    # Q. Wonder if there should be away to reset/notify the delegate when a parse error is
    # encountered    
    self.assertEquals(packet.headers, {'blah': '1', 'content-length': '4', 'destination': 'foo'})
    self.assertEquals(packet.body, 'abcd')
    
  
  def _grammartestIngoreTrailingContent(self):
    packet = KeyValue()
    
    packet.parser.grammar.states.append(Ignore())

    with packet.parser as parser:
      for c in 'send\ndestination:foo\nblah:1\ncontent-length:4\n\nabcdef':
        parser.feed(c)
    
    self.assertEquals(packet.headers, {'blah': '1', 'content-length': '4', 'destination': 'foo'})
    self.assertEquals(packet.body, 'abcd')
    
  def testMultipleBlocks(self):
    packet = KeyValue()
    class EventH(object):
      def __init__(self):
        self.blocks = []
        
      def on_enter(self, state):
        #print >> sys.stderr, "enter", state
        packet.headers = {}
        packet.body = None
        
      def on_exit(self,state):
        #print >> sys.stderr, "exit", state
        self.blocks.append((packet.headers, packet.body))

    event = EventH()        
    packet.parser.grammar.add_delegate(event)
    send_block = OneOrMore(packet.parser.grammar)

    packet.parser.grammar = send_block
    with packet.parser as parser:
      bytes = 'send\ndestination:foo\nblah:1\ncontent-length:4\n\nabcdsend\ndestination:baz\ncontent-length:4\n\nabcd'
      for c in bytes:
        parser.feed(c)
        
    self.assertEquals(len(event.blocks), 2)
    block = event.blocks[0]
    self.assertEquals(block[0], {'blah': '1', 'content-length': '4', 'destination': 'foo'})
    self.assertEquals(block[1], 'abcd')
    
    block = event.blocks[1]
    self.assertEquals(block[0], {'content-length': '4', 'destination': 'baz'})
    self.assertEquals(block[1], 'abcd')
    
    
    
  

      
if __name__ == "__main__":
  unittest.main()
