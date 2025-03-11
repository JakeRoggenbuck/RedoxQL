from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction

# Create a database and table
db = Database()
db.open('./ECS165-minimal')

# Create a table
table = db.create_table('Test', 3, 0)
query = Query(table)

# Insert a record
print("Inserting record")
query.insert(1, 100, 200)

# Verify the record
print("Verifying record")
record = query.select(1, 0, [1, 1, 1])[0]
print(f"Original record: {record.columns}")

# Create and run a transaction
print("Creating transaction")
transaction = Transaction()
transaction.add_query(query.update, table, 1, None, 999, 888)
print("Running transaction")
transaction.run()

# Check the updated record
updated = query.select(1, 0, [1, 1, 1])[0]
print(f"Updated record: {updated.columns}")

# Abort the transaction
print("Aborting transaction")
transaction.abort()

# Check if the record is rolled back
final = query.select(1, 0, [1, 1, 1])[0]
print(f"Final record: {final.columns}")

# Compare with original
if final.columns == record.columns:
    print("SUCCESS: Transaction abort worked")
else:
    print("FAILURE: Transaction abort failed")

# Close the database
db.close() 