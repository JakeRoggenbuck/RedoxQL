from lstore.db import Database
from lstore.query import Query

db = Database()
table = db.create_table("Count", 3, 0)
query = Query(table)

rec = query.insert(0, 10, 20)
print(rec)

vals = query.select(0, 0, [1, 1, 1])[0]
print(vals.columns)

query.delete(0)

rec = query.insert(1, 20, 30)
print(rec)

query.increment(1, 2)

vals = query.select(1, 0, [1, 1, 1])[0]
print(vals.columns)
