from lstore.db import Database
from lstore.query import Query

db = Database()

grades_table = db.create_table('Grades', 3, 0)
q1 = Query(grades_table)

q1.insert(1, 2, 3)
q1.insert(2, 3, 4)
q1.insert(3, 4, 5)

rec = q1.select(1, 0, [1, 1, 1])[0]
assert rec.columns == [1, 2, 3]

rec = q1.select(2, 0, [1, 1, 1])[0]
assert rec.columns == [2, 3, 4]

# get the same table again
grades_table2 = db.get_table('Grades')
q2 = Query(grades_table2)

rec1 = q1.select(1, 0, [1, 1, 1])
rec2 = q2.select(1, 0, [1, 1, 1]) # this returns nothing
assert len(rec1) == len(rec2) == 1 # fails
rec2 = rec2[0]
assert rec2.columns == [1, 2, 3] # fails
