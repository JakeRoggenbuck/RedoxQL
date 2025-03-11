from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed
import time

def test_transaction_abortion_detailed():
    """
    A more detailed test that shows the step-by-step process of 
    transaction abort functionality.
    """
    print("\n=== DETAILED TRANSACTION ABORT TEST ===")
    
    # Create a fresh database and table for this test
    db = Database()
    db.open('./ECS165-detailed-abort-test')
    
    # Create a table with 5 columns (student id and 4 grades)
    grades_table = db.create_table('Grades', 5, 0)
    
    # Create a query class for the grades table
    query = Query(grades_table)
    
    # Step 1: Insert a single record with known values
    print("\nStep 1: Inserting a record with known values")
    key = 999
    original_values = [key, 10, 20, 30, 40]
    print(f"Original values: {original_values}")
    query.insert(*original_values)
    
    # Step 2: Verify the record was inserted correctly
    print("\nStep 2: Verifying the record was inserted correctly")
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    print(f"Initial record: {record.columns}")
    
    # Verify all values match
    all_match = True
    for i, val in enumerate(original_values):
        if record.columns[i] != val:
            all_match = False
            print(f"ERROR: Column {i} has value {record.columns[i]}, expected {val}")
    
    if all_match:
        print("SUCCESS: All values match the inserted record")
    
    # Step 3: Create a transaction for updating the record
    print("\nStep 3: Creating a transaction to update the record")
    transaction = Transaction()
    
    # Step 4: Update the record within the transaction
    print("\nStep 4: Adding update query to the transaction")
    updated_values = [None, 99, 88, 77, 66]  # Update only columns 1-4
    print(f"Updated values: {updated_values}")
    transaction.add_query(query.update, grades_table, key, *updated_values)
    
    # Step 5: Execute the transaction (without committing)
    print("\nStep 5: Executing the transaction (run without commit)")
    result = transaction.run()
    print(f"Transaction execution result: {result}")
    
    # Step 6: Verify the record was updated
    print("\nStep 6: Verifying the record was updated")
    updated_record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    print(f"Updated record: {updated_record.columns}")
    
    # Expected values after update
    expected_after_update = [key, 99, 88, 77, 66]
    
    # Verify updated values match
    all_match = True
    for i, val in enumerate(expected_after_update):
        if updated_record.columns[i] != val:
            all_match = False
            print(f"ERROR: Column {i} has value {updated_record.columns[i]}, expected {val}")
    
    if all_match:
        print("SUCCESS: All values match the expected updated record")
    
    # Step 7: Abort the transaction
    print("\nStep 7: Aborting the transaction")
    abort_result = transaction.abort()
    print(f"Transaction abort result: {abort_result}")
    
    # Step 8: Verify the record has been rolled back to original values
    print("\nStep 8: Verifying the record has been rolled back")
    final_record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    print(f"Record after abort: {final_record.columns}")
    
    # Verify rolled back values match the original
    all_match = True
    for i, val in enumerate(original_values):
        if final_record.columns[i] != val:
            all_match = False
            print(f"ERROR: Column {i} has value {final_record.columns[i]}, expected {val}")
    
    if all_match:
        print("SUCCESS: All values match the original record after abort")
    else:
        print("FAILURE: Record was not properly rolled back")
    
    # Close the database
    db.close()

def test_transaction_abortion():
    """
    Test to verify that transaction abort functionality works properly.
    When a transaction is aborted, all of its operations should be rolled back.
    """
    print("Testing transaction abort functionality...")
    
    # Create a fresh database and table for this test
    db = Database()
    db.open('./ECS165-abort-test')
    
    # Create a table with 5 columns (student id and 4 grades)
    grades_table = db.create_table('Grades', 5, 0)
    
    # Create a query class for the grades table
    query = Query(grades_table)
    
    # Dictionary to keep track of records
    records = {}
    number_of_records = 100
    seed(42)  # For reproducibility
    
    # Insert initial records
    print("Inserting initial records...")
    for i in range(number_of_records):
        key = 92106429 + i
        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
    
    # Verify all records were inserted correctly
    print("Verifying initial records...")
    for key in records:
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                print(f"Initial record error on key {key}: {record.columns}, expected: {records[key]}")
    
    # Create a transaction with updates
    print("Creating a transaction with updates...")
    transaction = Transaction()
    
    # Store original values to verify rollback
    original_values = {}
    updated_values = {}
    
    # Add update operations to the transaction
    update_keys = list(records.keys())[:50]  # Update the first 50 records
    for key in update_keys:
        original_values[key] = records[key].copy()
        
        # Create updated values
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            updated_columns[i] = randint(50, 70)  # Different range to distinguish updates
        
        # First select the record and then update it
        transaction.add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
        transaction.add_query(query.update, grades_table, key, *updated_columns)
        
        # Save the expected updated values for verification
        updated_values[key] = original_values[key].copy()
        for i in range(1, len(updated_columns)):
            if updated_columns[i] is not None:
                updated_values[key][i] = updated_columns[i]
    
    # Abort the transaction
    print("Aborting the transaction...")
    transaction.abort()
    
    # Wait a moment to ensure abort completes
    time.sleep(1)
    
    # Verify that no changes were applied (records should match original values)
    print("Verifying records after abort...")
    errors = 0
    for key in update_keys:
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        for i, column in enumerate(record.columns):
            if column != original_values[key][i]:
                errors += 1
                print(f"ERROR: Key {key}, column {i} has value {column}, expected {original_values[key][i]}")
    
    if errors == 0:
        print("SUCCESS: All records maintained original values after transaction abort")
    else:
        print(f"FAILURE: {errors} values were incorrectly modified despite transaction abort")
    
    # Create a transaction that we will run (not abort)
    print("Testing successful transaction execution...")
    transaction2 = Transaction()
    
    # Add update operations to the transaction
    for key in update_keys:
        # Create updated values
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            updated_columns[i] = randint(80, 100)  # Different range for this transaction
        
        transaction2.add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
        transaction2.add_query(query.update, grades_table, key, *updated_columns)
        
        # Update expected values
        for i in range(1, len(updated_columns)):
            if updated_columns[i] is not None:
                updated_values[key][i] = updated_columns[i]
    
    # Run the transaction
    print("Running and committing the second transaction...")
    result = transaction2.run()
    print(f"Transaction completed with result: {result}")
    
    # Verify that changes were applied
    print("Verifying records after successful transaction...")
    errors = 0
    for key in update_keys:
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        for i, column in enumerate(record.columns):
            if column != updated_values[key][i]:
                errors += 1
                print(f"ERROR: Key {key}, column {i} has value {column}, expected {updated_values[key][i]}")
    
    if errors == 0:
        print("SUCCESS: All records were correctly updated after successful transaction")
    else:
        print(f"FAILURE: {errors} values were not correctly updated after successful transaction")
    
    # Close the database
    db.close()

def test_concurrent_transaction_aborts():
    """
    Test to verify that concurrent transaction aborts work properly.
    """
    print("\nTesting concurrent transaction aborts...")
    
    # Create a fresh database and table for this test
    db = Database()
    db.open('./ECS165-concurrent-aborts')
    
    # Create a table with 5 columns (student id and 4 grades)
    grades_table = db.create_table('Grades', 5, 0)
    
    # Create a query class for the grades table
    query = Query(grades_table)
    
    # Dictionary to keep track of records
    records = {}
    number_of_records = 100
    seed(43)  # Different seed
    
    # Insert initial records
    print("Inserting initial records...")
    for i in range(number_of_records):
        key = 92106429 + i
        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
    
    # Create multiple transactions
    num_transactions = 10
    num_threads = 4
    transactions = []
    transaction_workers = []
    
    # Create transactions - some to commit, some to abort
    print(f"Creating {num_transactions} transactions...")
    for i in range(num_transactions):
        transaction = Transaction()
        
        # Each transaction will update 10 random records
        for _ in range(10):
            key = choice(list(records.keys()))
            updated_columns = [None, None, None, None, None]
            for j in range(1, grades_table.num_columns):
                updated_columns[j] = randint(0, 100)
            
            transaction.add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
            transaction.add_query(query.update, grades_table, key, *updated_columns)
        
        transactions.append(transaction)
    
    # Create transaction workers
    for i in range(num_threads):
        transaction_workers.append(TransactionWorker())
    
    # Abort half of the transactions
    for i in range(num_transactions // 2):
        print(f"Aborting transaction {i}...")
        transactions[i].abort()
    
    # Add remaining transactions to workers
    for i in range(num_transactions // 2, num_transactions):
        transaction_workers[i % num_threads].add_transaction(transactions[i])
    
    # Run the workers
    print("Running transaction workers...")
    for worker in transaction_workers:
        worker.run()
    
    # Wait for workers to finish
    for worker in transaction_workers:
        worker.join()
    
    print("All transaction workers completed.")
    
    # Close the database
    db.close()

if __name__ == "__main__":
    # Run all the tests
    test_transaction_abortion_detailed()
    test_transaction_abortion()
    test_concurrent_transaction_aborts()
    print("\nAll tests completed.")
