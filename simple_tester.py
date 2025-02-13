from lstore.db import Database
from lstore.query import Query
from lstore import print_logo

print_logo()

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

recs = []

for i in range(0, 10):
    rec = query.insert(10 + i, 93, 0, 0, 0)
    recs.append(10 + i)

for rec in recs:
    vals = query.select(rec, 0, [0])
    print(vals)

for rec in recs:
    if rec % 2 == 0:
        print(f"Deleting {rec}")
        query.delete(rec)

for rec in recs:
    vals = query.select(rec, 0, [0])
    print(vals)

for x in range(0, 8):
    print(f"query.sum(13, 17, {x})", query.sum(13, 17, x))
