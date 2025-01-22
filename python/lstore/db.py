from lstore.table import Table


class Database:

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path: str):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name: str, num_columns: int, key_index: int):
        table = Table(name, num_columns, key_index)
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name: str):
        pass

    """
    # Returns table with the passed name
    """

    def get_table(self, name: str):
        pass
