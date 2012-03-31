import datetime
import time

from Rambler.TestCase import TestCase


class Clock:
  def __init__(self):
    self.dt = datetime.datetime(2010,9,1,18)

  def now(self):
    return self.dt
      
  def cycle(self):
    self.dt = self.dt + datetime.timedelta(minutes=1)

class TestTimeSeries(TestCase):
  
  def test_normal(self):
    #return
    clock = Clock()
    ts = self.TimeSeries(15, now_method=clock.now)
  
    for minute in range(15):
      for x in range(105):
        ts.count('sqs:com_zeepco-Outbound')
      clock.cycle()
          
    self.assert_series(ts, 'sqs:com_zeepco-Outbound',
      ('18:00', 105),
      ('18:01', 105),
      ('18:02', 105),
      ('18:03', 105),
      ('18:04', 105),
      ('18:05', 105),
      ('18:06', 105),
      ('18:07', 105),
      ('18:08', 105),
      ('18:09', 105),
      ('18:10', 105),
      ('18:11', 105),
      ('18:12', 105),
      ('18:13', 105),
      ('18:14', 105),
    )
  
  def test_empty(self):
    clock = Clock()
    ts = self.TimeSeries(15, now_method=clock.now)
    
    self.assert_series(ts, 'sqs:com_zeepco-Outbound',
      ('17:46', 0),
      ('17:47', 0),
      ('17:48', 0),
      ('17:49', 0),
      ('17:50', 0),
      ('17:51', 0),
      ('17:52', 0),
      ('17:53', 0),
      ('17:54', 0),
      ('17:55', 0),
      ('17:56', 0),
      ('17:57', 0),
      ('17:58', 0),
      ('17:59', 0),
      ('18:00', 0),
    )
    
        
  def assert_series(self, ts, variable, *series):
    """Asserts that the given series equals the expected values"""
   
    for s, expected in zip(ts, series):
      self.assertEqual(s.id, expected[0], "Id %s does not match expected %s" % (s.id, expected[0]))
      self.assertEqual(s[variable], expected[1], "Id %s does not match expected %s" % (s.id, expected[0]))
    