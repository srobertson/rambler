import sys
import types
import re

interp= re.compile(r'#\{([^}]*)\}')


class fstr(object):
  """Provides string formating similar to rubies interpolation.

  Usage:
  The formated string will interret block of code sandwiched in beetween #{ }
  as python statements. 


  >>> count = 1

  >>> template = fstr("I've seen #{count}")
  
  Note that it dosen't interpret the string right away. Instead it waits until
  you typecast them template as a string such as
  >>> str(template)
  "I've seen 1"
  
  We can manipulate the variables the template relies on and have it generate a new string.
  >>> count += 1
  >>> str(template)
  "I've seen 2"

  In general you can use this object in places that accept  a string
  >>> 'aaa ' + template
  "aaa I've seen 2"

  But for methods like join you'll need to convert it to a string prior to use.
  
  >>> fstr('#{"%02d" % count}').render()
  '02'
  """


  def __new__(cls, format):
    parts = []
    for item in interp.findall(format):
      token = "#{%s}" % item
      head,format = format.split(token,1) 
      parts.append(head)
      parts.append(compile(item, ' ', 'eval'))
    parts.append(format)

    # short circuit, become a plain old string if we have no #{} tokens
    #if len(parts) == 1 and type(parts[0]) != types.CodeType:
    #  return format
    #else:
    o = object.__new__(cls)
    o.parts = parts
    return o

  def __str__(self):
    return self.render(level=2)

  def render(self, globals=None, locals=None, level=1):
    """Convert the format into a string. If level is passed crawl that 
    number of frames up the stack. The frame determines the scope of the eval call."""

    frame = sys._getframe(level)
    if not locals:
      locals = frame.f_locals
      
    if not globals:
      globals = frame.f_globals
      
    parts = []
    for part in self.parts:
      if type(part) == types.CodeType:
        key = '.'.join(part.co_names)
        try:
          parts.append(locals[key])
        except:
          parts.append(str(eval(part,globals, locals)))
      else:
        parts.append(part)
    return "".join(parts)

  def __add__(self, other):
    if isinstance(other, basestring):
      return self.render(level=2) + other
    else:
      raise TypeError("unsupported operand type(s) for +: '%s' and '%s'" % (type(self),type(other))) 
      
  def __radd__(self, other):
    if isinstance(other, basestring):
      return other + self.render(level=2) 
    else:
      raise TypeError("unsupported operand type(s) for +: '%s' and '%s'" % (type(self),type(other))) 
    
    

if __name__ == '__main__':
  import doctest
  doctest.testmod()
