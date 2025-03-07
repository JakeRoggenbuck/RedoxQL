from lstore.db import Database
from lstore.query import Query
import time
import statistics

iters = 30
times = []

for x in range(iters):
    start = time.time()

    amount = 100_000
    db = Database()

    grades_table = db.create_table("Grades", 5, 0)
    query = Query(grades_table)

    for i in range(0, amount):
        query.insert(10 + i, 93, 0, 0, 0)

    for i in range(0, amount):
        query.update(10 + i, *[10 + i, 100, 10, 20, 30])

    for i in range(0, amount):
        v = query.select(10 + i, 0, [1, 1, 1, 1, 1])[0]

    delta = time.time() - start
    times.append(delta)
    print("Time taken: ", delta)


print("Mean: ", statistics.mean(times))
print("Stand. Dev: ", statistics.stdev(times))
