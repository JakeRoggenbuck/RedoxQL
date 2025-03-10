from .lstore import RTransactionWorker


class TransactionWorker:

    def __init__(self, transactions=()):
        """Creates a transaction worker object."""
        self.transaction_worker = RTransactionWorker()

        for t in transactions:
            self.transaction_worker.add_transaction(t)

        self.stats = []
        self.result = 0

    def add_transaction(self, t):
        """Appends t to transactions"""
        self.transaction_worker.add_transaction(t)

    def run(self):
        """Runs all transaction as a thread"""
        self.transaction_worker.run()

    def join(self):
        """Waits for the worker to finish"""
        self.transaction_worker.join()

    def __run(self):
        """
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
        """
        pass
