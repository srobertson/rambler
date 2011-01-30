from Rambler.ciHomeBase import ciHomeBase
from Rambler import Server
from time import time
from omniORB import CORBA
from Money import Money, getCurrency

class MoneyWidget(object):

    interface = "MoneyWidget"

    def __init__(self, pk):
        self._set_primaryKey(pk)
        self._money = None

    def _get_primaryKey( self ):
        return self._primaryKey

    def _set_primaryKey(self, pk):
        self._primaryKey = pk


    def _get_home():
        return Server.getHome("moneyWidgetHome")

    _get_home = staticmethod(_get_home)

    def _get_money( self ):
        return self._money

    def _set_money( self, money ):
        try:
            CORBA.id(money)
            currency = getCurrency(money.currencyCode)
            m = Money(0, currency)
            m._setState(money.amount, currency)
            money = m
        except CORBA.BAD_PARAM:
            pass
        
        self._money = money

class MoneyWidgetHome(ciHomeBase):
    # If this object needs to be registered, the following 2 are also needed.

    homeId = "moneyWidgetHome"
    interface = "MoneyWidgetHome"
    entityClass = MoneyWidget

    def create( self, money):
        try:
            CORBA.id(money)
            currency = getCurrency(money.currencyCode)
            m = Money(money.amount, currency)
            money = m
        except CORBA.BAD_PARAM:
            pass
        
        pk = '%.5f' % time()
        moneyWidget = MoneyWidget(pk)
        moneyWidget._set_money(money)

        self.PersistenceService.create(moneyWidget)

        # Notify our observers that a new entity has been created
        #self._notifyCreate(moneyWidget)
        return moneyWidget
