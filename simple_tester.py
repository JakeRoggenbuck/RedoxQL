from lstore.db import Database
from lstore.query import Query

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

recs = []

for i in range(0, 10):
    rec = query.insert(10 + i, 93, 0, 0, 0)
    recs.append(rec)

for rec in recs:
    vals = query.select(rec.rid, 0, 0)
    print(vals)
