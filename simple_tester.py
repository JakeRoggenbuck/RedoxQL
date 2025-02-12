from lstore.db import Database
from lstore.query import Query

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

for i in range(0, 10):
    query.insert(10 + i, 93, 0, 0, 0)
