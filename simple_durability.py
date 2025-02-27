from lstore.db import Database
from lstore.query import Query
from random import choice

keys = []

db = Database()
db.open("./D1")
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

for i in range(0, 10):
    a = query.insert(100_000_000 + i, 10, 20 * i, 30, 50)
    keys.append(100_000_000 + i)

for i in range(0, 10):
    a = query.select(choice(keys), 0, [1, 1, 1, 1, 1])[0]
    print(a.columns)

db.close()

print("Database closed. Opening it back up again.")

db2 = Database()
db2.open("./D1")
grades_table2 = db2.get_table("Grades")

# grades_table2.debug_page_dir()

query2 = Query(grades_table2)

for i in range(0, 10):
    a = query2.select(choice(keys), 0, [1, 1, 1, 1, 1])
    a = a[0]
    print(a.columns)
