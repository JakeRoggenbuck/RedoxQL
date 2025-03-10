from .lstore import RDatabase, RTable, RQuery, RTransaction

class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.transaction = RTransaction()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        query_type = query.__name__  # Extracts function name (e.g., "insert", "select")
        print(query_type)
        self.transaction.add_query(query_type, table, list(args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # for query, args in self.queries:
        #     result = query(*args)
        #     # If the query has failed the transaction should abort
        #     if result == False:
        #         return self.abort()
        # return self.commit()
        return self.transaction.run()

    def abort(self):
        return self.transaction.abort()

    def commit(self):
        return self.transaction.commit()
