from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_transactions = 100
number_of_operations_per_record = 1
num_threads = 8

keys = []
records = {}
seed(3562901)

# re-generate records for testing
for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    print(records[key])

transaction_workers = []
transactions = []

for i in range(number_of_transactions):
    transactions.append(Transaction())

for i in range(num_threads):
    transaction_workers.append(TransactionWorker())




updated_records = {}
# x update on every column
for j in range(number_of_operations_per_record):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        updated_records[key] = records[key].copy()
        for i in range(2, grades_table.num_columns):
            # updated value
            value = randint(0, 20)
            updated_columns[i] = value
            # update our test directory
            updated_records[key][i] = value
        transactions[key % number_of_transactions].add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
        transactions[key % number_of_transactions].add_query(query.update, grades_table, key, *updated_columns)
print("Update finished")


# add trasactions to transaction workers  
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(transactions[i])



# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()


score = len(keys)
for key in keys:
    correct = records[key]
    query = Query(grades_table)
    
    result = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0].columns
    if correct != result:
        print('select error on primary key', key, ':', result, ', correct:', correct)
        score -= 1
print('Version -1 Score:', score, '/', len(keys))

v2_score = len(keys)
for key in keys:
    correct = records[key]
    query = Query(grades_table)
    
    result = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0].columns
    if correct != result:
        print('select error on primary key', key, ':', result, ', correct:', correct)
        v2_score -= 1
print('Version -2 Score:', v2_score, '/', len(keys))
if score != v2_score:
    print('Failure: Version -1 and Version -2 scores must be same')

score = len(keys)
for key in keys:
    correct = updated_records[key]
    query = Query(grades_table)
    
    result = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0].columns
    if correct != result:
        print('select error on primary key', key, ':', result, ', correct:', correct)
        score -= 1
print('Version 0 Score:', score, '/', len(keys))

number_of_aggregates = 100
valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, -1)
    if column_sum == result:
        valid_sums += 1
print("Aggregate version -1 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)

v2_valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, -2)
    if column_sum == result:
        v2_valid_sums += 1
print("Aggregate version -2 finished. Valid Aggregations: ", v2_valid_sums, '/', number_of_aggregates)
if valid_sums != v2_valid_sums:
    print('Failure: Version -1 and Version -2 aggregation scores must be same.')

valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, 0)
    if column_sum == result:
        valid_sums += 1
print("Aggregate version 0 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)

db.close()
