from .lstore import RDatabase, RTable


class Database:

    def __init__(self):
        self.db = RDatabase()

    def open(self, path: str):
        self.db.open(path)

    def close(self):
        self.db.close()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(
        self,
        name: str,
        num_columns: int,
        key_index: int,
    ) -> RTable:
        self.db.create_table(name, num_columns, key_index)

        return self.db.get_table_from_index(0)

    """
    # Deletes the specified table
    """

    def drop_table(self, name: str):
        self.db.drop_table(name)

    """
    # Returns table with the passed name
    """

    def get_table(self, name: str) -> RTable:
        return self.db.get_table(name)
