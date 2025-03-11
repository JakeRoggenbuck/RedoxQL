from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction

def simple_abort_test():
    """
    A simple test for transaction abort functionality
    """
    print("Simple Transaction Abort Test")
    
    # Create a fresh database
    db = Database()
    db.open('./ECS165-simple-abort-test')
    
    # Create a table
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    
    # Insert a record
    key = 999
    original_values = [key, 10, 20, 30, 40]
    print(f"Inserting record: {original_values}")
    query.insert(*original_values)
    
    # Verify the record
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    print(f"Initial record: {record.columns}")
    
    # Create a transaction
    transaction = Transaction()
    
    # Update the record within the transaction
    updated_values = [None, 99, 88, 77, 66]
    print(f"Updating to: {updated_values}")
    transaction.add_query(query.update, grades_table, key, *updated_values)
    
    # Run the transaction
    transaction.run()
    
    # Verify the update
    updated_record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    print(f"After update: {updated_record.columns}")
    
    # Abort the transaction
    print("Aborting transaction")
    transaction.abort()
    
    # Verify the rollback
    final_record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    print(f"After abort: {final_record.columns}")
    
    # Check if values match the original
    if final_record.columns == original_values:
        print("SUCCESS: Transaction abort worked correctly")
    else:
        print("FAILURE: Transaction abort did not restore original values")
    
    # Close the database
    db.close()

if __name__ == "__main__":
    simple_abort_test() 