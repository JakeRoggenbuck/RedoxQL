from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    # print('inserted', records[key])
print("Insert finished")

# Check inserted records using select query
for key in records:
    # select function will return array of records 
    # here we are sure that there is only one record in t hat array
    # check for retreiving version -1. Should retreive version 0 since only one version exists.
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)

updated_records = {}
for key in records:
    updated_columns = [None, None, None, None, None]
    updated_records[key] = records[key].copy()
    for i in range(2, grades_table.num_columns):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # update our test directory
        updated_records[key][i] = value
    query.update(key, *updated_columns)

    #check version -1 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', records[key])
    else:
        pass
        # print('update on', original, 'and', updated_columns, ':', record)

    #check version -2 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', records[key])
    else:
        pass
        # print('update on', original, 'and', updated_columns, ':', record)
    
    #check version 0 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != updated_records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', updated_records[key])

keys = sorted(list(records.keys()))
# aggregate on every column 
for c in range(0, grades_table.num_columns):
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        # calculate the sum form test directory
        # version -1 sum
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], c, -1)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
            # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
        # version -2 sum
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], c, -2)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
        # version 0 sum
        updated_column_sum = sum(map(lambda key: updated_records[key][c], keys[r[0]: r[1] + 1]))
        updated_result = query.sum_version(keys[r[0]], keys[r[1]], c, 0)
        if updated_column_sum != updated_result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', updated_result, ', correct: ', updated_column_sum)
        else:
            pass
