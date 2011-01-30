import sys

from collections import deque

class LRU(dict):

    """Class that behaves like a dictionary with a set size. If the
    dicitionary grows over the size the least recently used item in
    the dictionary is deleted. This object is useful for items like
    caches because it will maintain a fixed size.

    Note though that it's not thread safe, so care must be taken to
    ensure that it's access to it's methods are synchronized.

    

    For examlpe we can make an LRU that will only hold at most two items
    
    >>> lru = LRU(2)
    >>> lru.keys()
    []

    LRU's behave like normal dictionaries
    
    >>> lru['foo'] = 'bar'
    >>> lru.keys()
    ['foo']
    >>> lru['foo']
    'bar'

    >>> lru['spam'] = 'eggs'
    >>> lru['spam']
    'eggs'
    >>> lru.keys()
    ['foo', 'spam']

    But notice that when I add one more new item, 'boo' gets evicted
    because it's the least recently used item in the list.

    >>> lru['baz'] = 'blat'
    >>> lru == {'spam': 'eggs', 'baz':'blat'}
    True

    """
    
    def __init__(self, size):
        super(LRU, self).__init__()
        
        self.size = size
        self.lru = deque() # list of keys, the key's position in the list is 

    def __getitem__(self, key):
        """Return the item, and move it to the head of the list"""

        val = super(LRU, self).__getitem__(key)
        self.lru.remove(key)
        self.lru.appendleft(key)
        return val


    def __setitem__(self, key, value):
        if len(self.lru) + 1 > self.size:
            # make some space for us
            self.__delitem__(self.lru[-1])

        super(LRU, self).__setitem__(key, value)
        self.lru.appendleft(key)

    def __delitem__(self, key):
        try:
          super(LRU, self).__delitem__(key)
        except KeyError:
          pass
        
        try:
          self.lru.remove(key)
        except KeyError:
          pass  
