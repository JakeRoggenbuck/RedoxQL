from .lstore import RDatabase, RTable, RQuery, RTransactionWorker


class TransactionWorker:
    """
    # Creates a transaction worker object.
    """

    def __init__(self, transactions=[]):
        self.worker = RTransactionWorker()

    """
    Appends t to transactions
    """

    def add_transaction(self, t):
        self.worker.add_transaction(t.transaction) # Transaction(python).RTransaction(rust)

    """
    Runs all transaction as a thread
    """

    def run(self):
        self.worker.run()
        # here you need to create a thread and call __run

    """
    Waits for the worker to finish
    """

    def join(self):
        self.worker.join()

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
