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
number_of_operations_per_record = 10
num_threads = 8

keys = []
records = {}
seed(3562901)

# re-generate records for testing
for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    # print(records[key])

transaction_workers = []
transactions = []

for i in range(number_of_transactions):
    transactions.append(Transaction())

for i in range(num_threads):
    transaction_workers.append(TransactionWorker())





# x update on every column
for j in range(number_of_operations_per_record):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(2, grades_table.num_columns):
            # updated value
            value = randint(0, 20)
            updated_columns[i] = value
            # copy record to check
            original = records[key].copy()
            # update our test directory
            records[key][i] = value
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
    try:
        correct = records[key]
        query = Query(grades_table)
        
        result = query.select(key, 0, [1, 1, 1, 1, 1])[0].columns
        if correct != result:
            print('select error on primary key', key, ':', result, ', correct:', correct)
            score -= 1
    except:
        print('Record Not found', key)
        score -= 1
print('Score', score, '/', len(keys))

db.close()
