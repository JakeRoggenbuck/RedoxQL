from lstore.db import Database
from lstore.query import Query
from lstore import print_logo

print_logo()

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

recs = []

print("Starting insert...")
for i in range(0, 20):
    rec = query.insert(10 + i, 93, 0, 0, 0)
    print(f"Inserting key {10 + i}")
    recs.append(10 + i)

print("\nStarting select...")
for rec in recs:
    vals = query.select(rec, 0, [1, 1, 1, 1, 1])
    print(vals[0].columns)

print("\nStarting delete...")
for rec in recs:
    if rec % 2 == 0:
        print(f"Deleting {rec}")
        query.delete(rec)

print("\nStarting increment...")
for _ in range(5):
    rec = query.select(recs[1], 0, [1, 1, 1, 1, 1])[0]
    print(rec.columns)
    query.increment(recs[1], 2)
