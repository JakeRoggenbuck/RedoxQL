from typing import Any, List
from .lstore import RQuery, RTable


class ReturnRecord:
    def __init__(self, columns: List[int]):
        self.columns = columns[3:]


class Query:
    def __init__(self, table: RTable):
        """Creates a Query object that can perform different queries on the
        specified table

        Queries that fail must return False
        Queries that succeed should return the result or True
        Any query that crashes (due to exceptions) should return False
        """
        self.table = table
        self.rquery = RQuery(table)

    def delete(self, primary_key: int):
        """Delete record
        Read a record with specified RID
        Returns True upon succesful deletion
        Return False if record doesn't exist or is locked due to 2PL
        """
        self.rquery.delete(primary_key)

    def insert(self, *columns):
        """Insert a record with specified columns
        Return True upon succesful insertion
        Returns False if insert fails for whatever reason
        """
        res = self.rquery.insert(columns)
        return False if res == None else True

    def select(
        self,
        search_key: Any,
        search_key_index: int,
        projected_columns_index: List[int],
    ):
        """Read matching record with specified search key
        :param search_key: the value you want to search based on
        :param search_key_index: the column index you want to search based on
        :param projected_columns_index: what columns to return. array of 1 or 0 values.
        Returns a list of Record objects upon success
        Returns False if record locked by TPL
        Assume that select will never be called on a key that doesn't exist
        """
        return [
            ReturnRecord(
                list(
                    self.rquery.select(
                        search_key,
                        search_key_index,
                        projected_columns_index,
                    )
                )
            )
        ]

    def select_version(
        self,
        search_key: Any,
        search_key_index: int,
        projected_columns_index: int,
        relative_version,
    ):
        """Read matching record with specified search key
        :param search_key: the value you want to search based on
        :param search_key_index: the column index you want to search based on
        :param projected_columns_index: what columns to return. array of 1 or 0 values.
        :param relative_version: the relative version of the record you need to retreive.
        Returns a list of Record objects upon success
        Returns False if record locked by TPL
        Assume that select will never be called on a key that doesn't exist
        """
        return [
            ReturnRecord(
                list(
                    self.rquery.select_version(
                        search_key,
                        search_key_index,
                        projected_columns_index,
                        relative_version,
                    )
                )
            )
        ]

    def update(self, primary_key: int, *columns):
        """Update a record with specified key and columns
        Returns True if update is succesful
        Returns False if no records exist with given key or if the target
        record cannot be accessed due to 2PL locking
        """
        res = self.rquery.update(primary_key, columns)
        return False if res == None else True

    def sum(
        self,
        start_range: int,
        end_range: int,
        aggregate_column_index: int,
    ) -> int:
        """Sum
        :param start_range: int         # Start of the key range to aggregate
        :param end_range: int           # End of the key range to aggregate
        :param aggregate_columns: int  # Index of desired column to aggregate
        this function is only called on the primary key.
        Returns the summation of the given range upon success
        Returns False if no record exists in the given range
        """
        return self.rquery.sum(start_range, end_range, aggregate_column_index)

    def sum_version(
        self,
        start_range,
        end_range,
        aggregate_column_index,
        relative_version,
    ):
        """Sum version
        :param start_range: int         # Start of the key range to aggregate
        :param end_range: int           # End of the key range to aggregate
        :param aggregate_columns: int  # Index of desired column to aggregate
        :param relative_version: the relative version of the record you need
        to retreive.

        this function is only called on the primary key.
        Returns the summation of the given range upon success
        Returns False if no record exists in the given range
        """
        return self.rquery.sum_version(start_range, end_range, aggregate_column_index, relative_version)

    def increment(self, key: int, column: int) -> bool:
        """Incremenets one column of the record this implementation should work
        if your select and update queries already work
        :param key: the primary of key of the record to increment
        :param column: the column to increment
        Returns True is increment is successful
        Returns False if no record matches key or if target record is locked by 2PL.
        """
        return self.rquery.increment(key, column)
