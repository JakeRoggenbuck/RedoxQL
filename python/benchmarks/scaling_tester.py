from lstore.db import Database
from lstore.query import Query
import timeit

counts = [10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000]


def run_inserts(amount: int):
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)

    for i in range(0, amount):
        query.insert(10 + i, 93, 0, 0, 0)


for x in counts:
    execution_time = timeit.timeit(lambda: run_inserts(x), number=1)
    print(f"Time taken to insert {x} records: {execution_time:.4f} seconds")
