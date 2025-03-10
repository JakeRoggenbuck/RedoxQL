from .lstore import RTransaction


class Transaction:
    def __init__(self):
        """Creates a transaction object."""
        self.transaction = RTransaction()


    def add_query(self, query, table, *args):
        """Adds the given query to this transaction
        Example:
        q = Query(grades_table)
        t = Transaction()
        t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
        """
        function_name = query.__name__

        # TODO: Figure out of we need select and select_version in transaction
        if function_name != "select" and function_name != "select_version":
            self.transaction.add_query(function_name, table, args)

    # If you choose to implement this differently this method must still return
    # True if transaction commits or False on abort
    def run(self):
        # TODO: Python expects a bool
        return self.transaction.run()

    def abort(self):
        return self.transaction.abort()

    def commit(self):
        return self.transaction.commit()
