class TransactionService(object):
    
    """Application needs some form of transaction Service. This is a
    non distributed version of a transaction service.

    This is mostly useful for tests, in the future we might flesh it
    out a bit further to keep track of one transaction per thread
    etc...
    
    """

    def begin(self):
        pass

    def commit(self, heuristics):
        pass

    def set_timeout(self, timeout):
        pass

    def get_transaction_name(self):
        return "Fake Txn Id"

    def rollback(self):
        pass
