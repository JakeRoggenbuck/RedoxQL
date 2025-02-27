from .lstore import RDatabase, RTable, RTableHandle


class Database:
    def __init__(self):
        self.db = RDatabase()

    def open(self, path: str):
        self.db.open(path)

    def close(self):
        self.db.close()

    def create_table(
        self,
        name: str,
        num_columns: int,
        key_index: int,
    ) -> RTable:
        """Creates a new table
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """
        return self.db.create_table(name, num_columns, key_index)

    def drop_table(self, name: str):
        """Deletes the specified table"""
        self.db.drop_table(name)

    def get_table(self, name: str) -> RTable:
        """Returns table with the passed name"""
        return self.db.get_table(name)
