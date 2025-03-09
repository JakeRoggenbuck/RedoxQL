from .lstore import RTransactionWorker


class TransactionWorker:
    """
    # Creates a transaction worker object.
    """

    def __init__(self, transactions=[]):
        self.transaction_worker = RTransactionWorker()

        for t in transactions:
            self.transaction_worker.add_transaction(t)

        self.stats = []
        self.result = 0

    """
    Appends t to transactions
    """

    def add_transaction(self, t):
        self.transaction_worker.add_transaction(t)

    """
    Runs all transaction as a thread
    """

    def run(self):
        self.transaction_worker.run()

    """
    Waits for the worker to finish
    """

    def join(self):
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
