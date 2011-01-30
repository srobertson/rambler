import math

class Stat(object):
  """Class used for sampling data over a time range. """
  # TODO: Cansider making a stats, math package for this component
  keys_to_serialize = ['description', 'min', 'max', 'count',
    'sum', 'sum_of_squares', 'mean', 'std_deviation']
  
  def __init__(self, description):
    self.description = description
    self.period = None
    self.min = 0.0
    self.max = 0.0
    self.count = 0.0
    self.sum   = 0.0
    self.sum_of_squares = 0.0
    self.mean = 0.0
    self.std_deviation = 0.0

  def __repr__(self):
    return "<%s min: %.5f mean: %.5f deviation: %.5f max: %.5f>" % (
            self.description, self.min, self.mean, self.std_deviation, self.max)

  def tally(self, value):
    # self.min can't be zero
    self.min = min(self.min, value) or value
    self.max = max(self.max, value)

    self.count += 1
    self.sum += value
    self.sum_of_squares += value ** 2
    self.mean = self.sum / self.count
    if self.count > 1:
      # running std deviation, broken into two steps for readability
      step = (self.sum_of_squares / self.count) - (self.mean ** 2)
      self.std_deviation = math.sqrt(step)      

