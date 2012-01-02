from datetime import datetime, timedelta
import time

from collections import deque, defaultdict

class Series(defaultdict):
  def __init__(self, id):
    self.id = id
    super(Series, self).__init__(float)
    
  def __repr__(self):
    return super(defaultdict, self).__repr__()


class TimeSeries(object):
  series = {}
  
  @classmethod
  def will_disassemble(cls):
    cls.series.clear()
  
  
  def __init__(self, name, maxlen=15, now_method=time.time, **kw):
    if self.series.has_key(name):
      raise IndexError, "TimesSeries %s already exists" % name
    self.series[name] = self
    self.name = name
    
    if not kw:
      kw['minutes'] = 1
    elif len(kw) > 1:
      raise ValueError("Only one time unit of minutes, seconds, hours should be passed")
    self.precision = timedelta(**kw)
    
    self.create_bucket = Series
    self.now=now_method
    
    now = self.now()
    self.series = deque(maxlen=maxlen)
    for x in range(maxlen):
      self.series.append(self.create_bucket(x))
    self.current = now
    self.keys = set()
    

  def advance(self, by, now):
    last = self.series[-1].id
    for x in range(int(by)):
      last += 1
      self.series.append(self.create_bucket(last))
    self.current = now
    
  def count(self, name):
    now = self.now()
    behind, remaining = divmod(now - self.current, self.precision.seconds)
    if behind > 0:
      self.advance(behind, now)
    
    self.keys.add(name)
    self.series[-1][name] += 1
    #s[name] += 1
    
  def __iter__(self):
    def next():
      for s in self.series:
        for k in self.keys:
          # Ensure defaults exist
          s[k]
        yield s
    return next()