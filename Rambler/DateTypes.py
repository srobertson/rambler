from DateTime.DateTime import DateTime

class Date(DateTime):
    def __str__(self):
        return self.strftime('%m/%d/%Y')

class Timestamp(DateTime):
    def __str__(self):
        return self.strftime('%m/%d/%Y %I:%M:%S %p')
