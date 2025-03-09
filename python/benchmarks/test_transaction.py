from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction

amount = 100
db = Database()
t = Transaction()

grades_table = db.create_table("Grades", 5, 0)
query = Query(grades_table)

for i in range(0, amount):
    t.add_query(query.update, grades_table, 10 + i, 93, 0, 0, 0)

t.run()

for i in range(0, amount):
    v = query.select(10 + i, 0, [1, 1, 1, 1, 1])
    print(v)
