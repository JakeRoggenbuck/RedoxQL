from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

# Bank balance

db = Database()
db.open("./AbortTest")

balance_table = db.create_table('balances', 2, 0)

query = Query(balance_table)

# Insert balances
for i in range(0, 10):
    res = query.insert(i, 100)
    assert res == True # all inserts should succeed

res = query.insert(0, 100)
assert res == False # duplicate insert should fail

# test select
for i in range(0, 10):
    res = query.select(i, 0, [1, 1])

    print(res[0].columns[0], res[0].columns[1])

# test update in transaction
t = Transaction()

for i in range(0, 10):
    t.add_query(query.update, balance_table, i, *[None, 200])

res = t.run()
assert res == True # transaction should commit

for i in range(0, 10):
    res = query.select(i, 0, [1, 1])
    assert res[0].columns[1] == 200

# test update in transaction
t2 = Transaction()

for i in range(0, 10):
    t2.add_query(query.update, balance_table, i, *[None, 100])

t2.add_query(query.insert, balance_table, 0, 100)

res = t2.run()
assert res == False # transaction should abort

for i in range(0, 10):
    res = query.select(i, 0, [1, 1])
    assert res[0].columns[1] == 200 # all updates should have been rolled back since the insert is invalid

print("All tests passed")
db.close()
