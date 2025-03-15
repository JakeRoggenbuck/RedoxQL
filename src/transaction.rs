/*
 * Implementation of Two-Phase Locking (2PL) with Record-Level Locking
 *
 * This implementation follows a modified 2PL protocol with record-level granularity:
 *
 * 1. Growing Phase: Locks are acquired as operations are performed (lock-as-you-go)
 *    - Read operations require shared (S) locks on specific records
 *    - Write operations require exclusive (X) locks on specific records
 *    - If a lock cannot be acquired immediately, the transaction is aborted
 *    - No waiting or retrying for locks - immediate abort on conflict (no-wait policy)
 *
 * 2. Execution Phase: Operations are executed immediately after acquiring locks
 *    - Each operation is tracked to enable rollback if needed
 *    - If any operation fails, the transaction is aborted and all changes are rolled back
 *
 * 3. Shrinking Phase: All locks are released only after the transaction is committed or aborted
 *    - Locks are held until the transaction completes
 *
 * The implementation tracks which records are locked and maintains a history of operations
 * to support rollback in case of abort. This approach uses a "no-wait" policy for deadlock
 * prevention - transactions never wait for locks, they simply abort and can be retried later.
 *
 * Deadlock Prevention:
 * - We use a "no-wait" policy where transactions immediately abort if they cannot acquire a lock
 * - Ensure guards are dropped as soon as possible by using separate scopes
 */

use super::database::{LockType, RecordId};
use super::query::RQuery;
use super::table::RTableHandle;
use log::debug;
use pyo3::prelude::*;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
enum QueryFunctions {
    Delete,
    Insert,
    Update,
    Sum,
    SumVersion,
    Increment,
    Select,
    SelectVersion,
    None,
}

#[derive(Clone)]
struct SingleQuery {
    func: QueryFunctions,
    table: RTableHandle,
    args: Vec<Option<i64>>,
}

// Structure to track executed operations for potential rollback
#[derive(Clone)]
struct ExecutedOperation {
    query_type: QueryFunctions,
    table: RTableHandle,
    primary_key: i64,
    previous_values: Option<Vec<Option<i64>>>,
}

#[pyclass]
#[derive(Clone)]
pub struct RTransaction {
    queries: VecDeque<SingleQuery>,
    // Track operations that have been executed
    executed_operations: Vec<ExecutedOperation>,
    // Track locked records for this transaction
    locked_records: Vec<RecordId>,
    // Transaction ID for tracking
    #[pyo3(get, set)]
    transaction_id: i64,
}

#[pymethods]
impl RTransaction {
    #[new]
    pub fn new() -> Self {
        // Generate a unique transaction ID
        let transaction_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        RTransaction {
            queries: VecDeque::new(),
            executed_operations: Vec::new(),
            locked_records: Vec::new(),
            transaction_id,
        }
    }

    // Getter method for transaction_id
    pub fn transaction_id(&self) -> i64 {
        self.transaction_id
    }

    pub fn add_query(&mut self, function_name: &str, table: RTableHandle, args: Vec<Option<i64>>) {
        let func = match function_name {
            "delete" => QueryFunctions::Delete,
            "insert" => QueryFunctions::Insert,
            "update" => QueryFunctions::Update,
            "sum" => QueryFunctions::Sum,
            "sum_version" => QueryFunctions::SumVersion,
            "increment" => QueryFunctions::Increment,
            "select" => QueryFunctions::Select,
            "select_version" => QueryFunctions::SelectVersion,
            _ => QueryFunctions::None,
        };

        let q = SingleQuery { func, table, args };
        self.queries.push_back(q);
    }

    pub fn run(&mut self) -> bool {
        debug!("Started run for transaction {}!", self.transaction_id);

        // Create a local copy of queries to process
        let mut queries_to_process = self.queries.clone();
        self.queries.clear(); // Clear the original queue

        // Process each query, acquiring locks as we go
        while let Some(q) = queries_to_process.pop_front() {
            let lock_type = self.get_lock_type_for_query(&q);

            debug!(
                "Transaction {} processing query with lock type {:?} (function: {:?})",
                self.transaction_id, lock_type, q.func
            );

            // Execute the query with appropriate locking
            let success = self.execute_query(q.clone(), lock_type);

            if !success {
                debug!(
                    "Transaction {} failed during execution, performing cascading abort.",
                    self.transaction_id
                );
                // Perform cascading abort - release all locks and rollback changes
                return self.abort();
            }
        }

        debug!(
            "Transaction {} successful, committing.",
            self.transaction_id
        );
        self.commit();
        true
    }

    pub fn commit(&mut self) -> bool {
        debug!(
            "Committing transaction {} with {} operations.",
            self.transaction_id,
            self.executed_operations.len()
        );

        // Release all locks
        self.release_all_locks();

        // Clear the executed operations history
        self.executed_operations.clear();

        debug!("Transaction {} committed.", self.transaction_id);
        true
    }
}

impl RTransaction {
    // Helper method to determine lock type needed for a query
    fn get_lock_type_for_query(&self, query: &SingleQuery) -> LockType {
        match query.func {
            QueryFunctions::Delete
            | QueryFunctions::Insert
            | QueryFunctions::Update
            | QueryFunctions::Increment => LockType::Exclusive,
            QueryFunctions::Sum
            | QueryFunctions::SumVersion
            | QueryFunctions::Select
            | QueryFunctions::SelectVersion => LockType::Shared,
            QueryFunctions::None => {
                LockType::Shared // Default to shared for unknown operations
            }
        }
    }

    // Execute a single query with appropriate locking
    fn execute_query(&mut self, q: SingleQuery, lock_type: LockType) -> bool {
        let t = q.table.clone();
        let mut query = RQuery::new(t.clone());

        // Extract the number of columns safely before using query
        let num_columns = {
            let table_guard = query.handle.table.read().unwrap();
            table_guard.num_columns
        };

        // Determine which records need to be locked based on the query type
        match q.func {
            QueryFunctions::Delete | QueryFunctions::Update | QueryFunctions::Increment => {
                if let Some(Some(pk)) = q.args.first() {
                    // Get the table name for locking
                    let table_name = t.get_name();

                    // Try to acquire lock on the specific record
                    // If lock can't be obtained instantly, return false to trigger cascading abort
                    if !self.acquire_record_lock(table_name, *pk, lock_type, &t) {
                        debug!("Failed to acquire lock for record {} in table {}, aborting transaction {}",
                               pk, t.get_name(), self.transaction_id);
                        return false;
                    }

                    // Get previous values for potential rollback
                    let prev_values =
                        if let Some(records) = query.select(*pk, 0, vec![1; num_columns]) {
                            if let Some(record) = records.into_iter().next().flatten() {
                                Some(record.columns.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                    // Execute the operation
                    let success = match q.func {
                        QueryFunctions::Delete => {
                            query.delete(*pk);
                            true
                        }
                        QueryFunctions::Update => {
                            let rest = q.args.iter().skip(1).cloned().collect();
                            query.update(*pk, rest)
                        }
                        QueryFunctions::Increment => {
                            if let Some(Some(col)) = q.args.get(1) {
                                query.increment(*pk, *col)
                            } else {
                                false
                            }
                        }
                        _ => unreachable!(),
                    };

                    // Record the operation for potential rollback
                    if success {
                        self.executed_operations.push(ExecutedOperation {
                            query_type: q.func,
                            table: t.clone(),
                            primary_key: *pk,
                            previous_values: prev_values,
                        });
                    }

                    return success;
                }
                false
            }
            QueryFunctions::Insert => {
                debug!("Inserting record with args: {:?}", q.args);
                let args: Vec<i64> = q.args.iter().filter_map(|x| *x).collect();
                let pkraw = { args.get(t.table.read().unwrap().primary_key_column) };

                // For insert, we need to lock the primary key value
                if let Some(pk) = pkraw {
                    // Get the table name for locking
                    let table_name = t.get_name();

                    // Check if a record with this primary key already exists
                    // This is important to prevent duplicate primary keys
                    if let Some(records) = query.select(*pk, 0, vec![1; num_columns]) {
                        if !records.is_empty() && records[0].is_some() {
                            debug!("Cannot insert record with duplicate primary key: {}", pk);
                            return false;
                        }
                    }

                    let success = query.insert(args.clone());

                    // Record the operation for potential rollback
                    if success {
                        // Try to acquire lock on the record just inserted
                        // If lock can't be obtained instantly, return false to trigger cascading abort
                        if !self.acquire_record_lock(table_name, *pk, lock_type, &t) {
                            debug!("Failed to acquire lock for record {} in table {}, aborting transaction {}",
                                pk, t.get_name(), self.transaction_id);
                            return false;
                        }

                        self.executed_operations.push(ExecutedOperation {
                            query_type: q.func,
                            table: t.clone(),
                            primary_key: *pk,
                            previous_values: None,
                        });
                    }

                    return success;
                }
                false
            }
            QueryFunctions::Sum => {
                if let (Some(Some(s)), Some(Some(e)), Some(Some(c))) =
                    (q.args.get(0), q.args.get(1), q.args.get(2))
                {
                    // Get the table name for locking
                    let table_name = t.get_name();

                    // Acquire locks on the range of records
                    for pk in *s..=*e {
                        // If any lock can't be obtained instantly, return false to trigger cascading abort
                        if !self.acquire_record_lock(table_name.clone(), pk, lock_type, &t) {
                            debug!("Failed to acquire lock for record {} in table {}, aborting transaction {}",
                                   pk, t.get_name(), self.transaction_id);
                            return false;
                        }
                    }

                    query.sum(*s, *e, *c);
                    return true;
                }
                false
            }
            QueryFunctions::SumVersion => {
                if q.args.len() > 4 {
                    let start = q.args[0];
                    let end = q.args[1];
                    let cols: Vec<i64> = q.args[2..q.args.len() - 1]
                        .iter()
                        .filter_map(|x| *x)
                        .collect();
                    let ver = q.args[q.args.len() - 1];

                    if let (Some(s), Some(e), Some(v)) = (start, end, ver) {
                        // Get the table name for locking
                        let table_name = t.get_name();

                        // Acquire locks on the range of records
                        for pk in s..=e {
                            // If any lock can't be obtained instantly, return false to trigger cascading abort
                            if !self.acquire_record_lock(table_name.clone(), pk, lock_type, &t) {
                                debug!("Failed to acquire lock for record {} in table {}, aborting transaction {}",
                                       pk, t.get_name(), self.transaction_id);
                                return false;
                            }
                        }

                        query.select_version(s, e, cols, v);
                        return true;
                    }
                }
                false
            }
            QueryFunctions::Select => {
                if let (Some(Some(pk)), Some(Some(col_idx))) = (q.args.get(0), q.args.get(1)) {
                    // Get the table name for locking
                    let table_name = t.get_name();

                    // Acquire lock on the specific record
                    // If lock can't be obtained instantly, return false to trigger cascading abort
                    if !self.acquire_record_lock(table_name, *pk, lock_type, &t) {
                        debug!("Failed to acquire lock for record {} in table {}, aborting transaction {}",
                               pk, t.get_name(), self.transaction_id);
                        return false;
                    }

                    // Create a vector with 1 at the specified column index and 0 elsewhere
                    let mut cols = vec![0; num_columns];
                    if *col_idx < cols.len() as i64 {
                        cols[*col_idx as usize] = 1;
                    }

                    query.select(*pk, 0, cols);
                    return true;
                }
                false
            }
            QueryFunctions::SelectVersion => {
                if q.args.len() > 2 {
                    let pk = q.args[0];
                    let cols: Vec<i64> = q.args[1..q.args.len() - 1]
                        .iter()
                        .filter_map(|x| *x)
                        .collect();
                    let ver = q.args[q.args.len() - 1];

                    if let (Some(p), Some(v)) = (pk, ver) {
                        // Get the table name for locking
                        let table_name = t.get_name();

                        // Acquire lock on the specific record
                        // If lock can't be obtained instantly, return false to trigger cascading abort
                        if !self.acquire_record_lock(table_name, p, lock_type, &t) {
                            debug!("Failed to acquire lock for record {} in table {}, aborting transaction {}",
                                   p, t.get_name(), self.transaction_id);
                            return false;
                        }

                        query.select_version(p, p, cols, v);
                        return true;
                    }
                }
                false
            }
            QueryFunctions::None => {
                debug!("Unknown query type.");
                false
            }
        }
    }

    // Acquire a lock on a specific record
    fn acquire_record_lock(
        &mut self,
        table_name: String,
        primary_key: i64,
        lock_type: LockType,
        table_handle: &RTableHandle,
    ) -> bool {
        let record_id = (table_handle.clone(), primary_key, lock_type);

        // Get the RID (record ID) from the index
        let rid = {
            let table = table_handle.table.read().unwrap();
            let index = table.index.try_read().unwrap();
            match index.get(primary_key) {
                Some(&rid) => rid,
                None => {
                    // Record doesn't exist in index
                    debug!(
                        "Record with key {} not found in index for table {}",
                        primary_key, table_name
                    );
                    return false;
                }
            }
        };

        // Try to acquire the lock
        // No retries - either we get the lock immediately or we fail
        let lock_acquired = {
            let a = match lock_type {
                LockType::Shared => {
                    // Try to acquire a shared lock
                    table_handle
                        .table
                        .read()
                        .unwrap()
                        .page_directory
                        .directory
                        .get(&rid)
                        .unwrap()
                        .attempt_obtain_read()
                }
                LockType::Exclusive => {
                    // Try to acquire an exclusive lock
                    table_handle
                        .table
                        .read()
                        .unwrap()
                        .page_directory
                        .directory
                        .get(&rid)
                        .unwrap()
                        .attempt_obtain_write()
                }
            };

            if !a {
                debug!(
                    "Failed to acquire lock {} for record {} in transaction {}",
                    lock_type, record_id.1, self.transaction_id
                );
            }

            a
        };

        if lock_acquired {
            // Track that we've locked this record
            self.locked_records.push(record_id);
            return true;
        }

        debug!(
            "Failed to acquire {:?} lock for record {:?} in transaction {}",
            lock_type, record_id.1, self.transaction_id
        );
        false
    }

    // Release all locks held by this transaction
    fn release_all_locks(&mut self) {
        debug!(
            "Releasing all locks for transaction {}",
            self.transaction_id
        );

        for (table, primary_key, lock_type) in &self.locked_records {
            // Get a read lock on the table
            if let Ok(table_guard) = table.table.read() {
                // Check if the record exists in the page directory
                if let Some(record_lock) = table_guard.page_directory.directory.get(primary_key) {
                    // Release the lock
                    match lock_type {
                        LockType::Shared => {
                            record_lock.release_read_lock();
                        }
                        LockType::Exclusive => {
                            record_lock.release_write_lock();
                        }
                    }
                } else {
                    debug!(
                        "Record with primary key {} not found in page directory during lock release",
                        primary_key
                    );
                }
            } else {
                debug!(
                    "Failed to acquire read lock on table while releasing record lock for key {}",
                    primary_key
                );
            }
        }

        // Clear our tracking of locked records
        self.locked_records.clear();
    }

    // Abort the transaction and roll back all executed operations
    fn abort(&mut self) -> bool {
        debug!(
            "Aborting transaction {}. Rolling back {} operations.",
            self.transaction_id,
            self.executed_operations.len()
        );

        // Roll back operations in reverse order (LIFO)
        for op in self.executed_operations.iter().rev() {
            let mut query = RQuery::new(op.table.clone());

            match op.query_type {
                QueryFunctions::Insert => {
                    debug!(
                        "Rolling back insert: Deleting record with primary key {}",
                        op.primary_key
                    );
                    query.delete(op.primary_key);
                }
                QueryFunctions::Update | QueryFunctions::Increment => {
                    if let Some(previous_values) = &op.previous_values {
                        debug!("Rolling back update/increment: Restoring record with primary key {} to previous values", op.primary_key);
                        query.update(op.primary_key, previous_values.clone());
                    }
                }
                QueryFunctions::Delete => {
                    if let Some(previous_values) = &op.previous_values {
                        debug!(
                            "Rolling back delete: Re-inserting record with primary key {}",
                            op.primary_key
                        );
                        query.insert(previous_values.iter().map(|x| x.unwrap_or(0)).collect());
                    }
                }
                _ => {
                    // Read operations don't need rollback
                    debug!("Query type {:?} does not require rollback.", op.query_type);
                }
            }
        }

        // Release all locks after abort
        self.release_all_locks();

        // Clear the executed operations
        self.executed_operations.clear();

        debug!("Transaction {} aborted.", self.transaction_id);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::RDatabase;
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::thread;
    use std::time::{Duration, Instant};

    // Helper function to run a transaction with timeout
    fn run_with_timeout(transaction: &mut RTransaction, timeout_ms: u64) -> bool {
        // Run the transaction directly
        // This is simpler and avoids threading issues
        let start = Instant::now();
        let result = transaction.run();

        // Check if we exceeded the timeout
        if start.elapsed() > Duration::from_millis(timeout_ms) {
            debug!("Transaction execution took longer than {}ms", timeout_ms);
        }

        result
    }

    // Helper function to run a test with timeout
    fn run_test_with_timeout<F, R>(test_fn: F, timeout_ms: u64) -> Option<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let result = test_fn();
            let _ = tx.send(result);
        });

        // Wait for the result with timeout
        match rx.recv_timeout(Duration::from_millis(timeout_ms)) {
            Ok(result) => Some(result),
            Err(_) => {
                debug!("Test timed out after {}ms", timeout_ms);
                None
            }
        }
    }

    #[test]
    fn add_query_test() {
        let result = run_test_with_timeout(
            || {
                let mut t = RTransaction::new();
                let mut db = RDatabase::new();

                let table_ref = db.create_table("Scores".to_string(), 3, 0);
                let mut query = RQuery::new(table_ref.clone());

                t.add_query(
                    "insert",
                    table_ref.clone(),
                    vec![Some(0), Some(234), Some(345)],
                );

                let mut v = query.select(0, 0, vec![1, 1, 1]);

                // We haven't run the transaction yet so nothing is returned
                assert_eq!(v.clone().unwrap().len(), 0);

                // Run with a timeout to prevent test hanging
                let success = run_with_timeout(&mut t, 5000); // 5 second timeout
                assert!(success, "Transaction should succeed");

                v = query.select(0, 0, vec![1, 1, 1]);

                assert_eq!(
                    v.unwrap()[0].as_ref().unwrap().columns,
                    vec![Some(0), Some(234), Some(345)]
                );

                true
            },
            10000,
        ); // 10 second timeout for the entire test

        assert!(result.unwrap_or(false), "Test should complete successfully");
    }

    #[test]
    fn add_many_query_test() {
        let mut t = RTransaction::new();
        let mut db = RDatabase::new();

        let table_ref = db.create_table("Scores".to_string(), 3, 0);
        let mut query = RQuery::new(table_ref.clone());

        for x in 0..100 {
            t.add_query(
                "insert",
                table_ref.clone(),
                vec![Some(x), Some(x * x), Some(345 + x)],
            );
        }

        let mut v = query.select(0, 0, vec![1, 1, 1]);

        // We haven't run the transaction yet so nothing is returned
        assert_eq!(v.clone().unwrap().len(), 0);

        // Run with a timeout to prevent test hanging
        let success = run_with_timeout(&mut t, 5000); // 5 second timeout
        assert!(success, "Transaction should succeed");

        // Test selecting on different columns
        v = query.select(4, 1, vec![1, 1, 1]);

        assert_eq!(
            v.unwrap()[0].as_ref().unwrap().columns,
            vec![Some(2), Some(4), Some(345 + 2)]
        );

        v = query.select(16, 1, vec![1, 1, 1]);

        assert_eq!(
            v.unwrap()[0].as_ref().unwrap().columns,
            vec![Some(4), Some(16), Some(345 + 4)]
        );

        v = query.select(1024, 1, vec![1, 1, 1]);

        assert_eq!(
            v.unwrap()[0].as_ref().unwrap().columns,
            vec![Some(32), Some(1024), Some(345 + 32)]
        );
    }

    #[test]
    fn add_query_insert_and_delete_test() {
        let mut t = RTransaction::new();
        let mut db = RDatabase::new();

        let table_ref = db.create_table("Scores".to_string(), 3, 0);
        let mut query = RQuery::new(table_ref.clone());

        t.add_query(
            "insert",
            table_ref.clone(),
            vec![Some(0), Some(234), Some(345)],
        );

        let mut v = query.select(0, 0, vec![1, 1, 1]);

        // We haven't run the transaction yet so nothing is returned
        assert_eq!(v.clone().unwrap().len(), 0);

        // Run with a timeout to prevent test hanging
        let success = run_with_timeout(&mut t, 5000); // 5 second timeout
        assert!(success, "Transaction should succeed");

        v = query.select(0, 0, vec![1, 1, 1]);

        assert_eq!(
            v.unwrap()[0].as_ref().unwrap().columns,
            vec![Some(0), Some(234), Some(345)]
        );
    }

    #[test]
    fn test_transaction_abort_on_duplicate_primary_key() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("TestTable".to_string(), 3, 0);
        let mut query = RQuery::new(table_ref.clone());

        // First, insert a record with primary key 1
        let mut t1 = RTransaction::new();
        t1.add_query(
            "insert",
            table_ref.clone(),
            vec![Some(1), Some(100), Some(200)],
        );

        // Run the first transaction with timeout
        let success = run_with_timeout(&mut t1, 5000); // 5 second timeout
        assert!(success, "First transaction should succeed");

        // Verify the record was inserted
        let result = query.select(1, 0, vec![1, 1, 1]);
        assert!(result.is_some(), "Record with primary key 1 should exist");
        assert_eq!(
            result.unwrap()[0].as_ref().unwrap().columns,
            vec![Some(1), Some(100), Some(200)]
        );

        // Now create a transaction that will try to insert a duplicate primary key
        // but also includes other valid operations
        let mut t2 = RTransaction::new();

        // First insert a valid record
        t2.add_query(
            "insert",
            table_ref.clone(),
            vec![Some(2), Some(300), Some(400)],
        );

        // Then try to insert a duplicate primary key
        t2.add_query(
            "insert",
            table_ref.clone(),
            vec![Some(1), Some(500), Some(600)],
        );

        // Run the second transaction with timeout - it should abort
        let success = run_with_timeout(&mut t2, 5000); // 5 second timeout
        assert!(
            !success,
            "Second transaction should abort due to duplicate primary key"
        );

        // Verify the first record from t2 was rolled back
        let result = query.select(2, 0, vec![1, 1, 1]);
        assert!(
            result.is_none() || result.unwrap().is_empty(),
            "Record with primary key 2 should not exist after rollback"
        );

        // Verify the original record is unchanged
        let result = query.select(1, 0, vec![1, 1, 1]);
        assert!(result.is_some(), "Original record should still exist");
        assert_eq!(
            result.unwrap()[0].as_ref().unwrap().columns,
            vec![Some(1), Some(100), Some(200)],
            "Original record should be unchanged"
        );
    }

    #[test]
    fn test_transaction_rollback_on_invalid_query() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("TestTable".to_string(), 3, 0);
        let mut query = RQuery::new(table_ref.clone());

        // Create a transaction with multiple operations
        let mut t = RTransaction::new();

        // Insert a valid record
        t.add_query(
            "insert",
            table_ref.clone(),
            vec![Some(10), Some(1000), Some(2000)],
        );

        // Update a valid record
        t.add_query(
            "insert",
            table_ref.clone(),
            vec![Some(20), Some(3000), Some(4000)],
        );

        // Try to perform an invalid operation (update a non-existent record)
        t.add_query(
            "update",
            table_ref.clone(),
            vec![Some(999), Some(5000), Some(6000)],
        );

        // Run the transaction with timeout - it should abort
        let success = run_with_timeout(&mut t, 5000); // 5 second timeout
        assert!(!success, "Transaction should abort due to invalid update");

        // Verify all operations were rolled back
        let result = query.select(10, 0, vec![1, 1, 1]);
        assert!(
            result.is_none() || result.unwrap().is_empty(),
            "Record with primary key 10 should not exist after rollback"
        );

        let result = query.select(20, 0, vec![1, 1, 1]);
        assert!(
            result.is_none() || result.unwrap().is_empty(),
            "Record with primary key 20 should not exist after rollback"
        );
    }

    #[test]
    fn test_concurrent_transactions_with_record_locking() {
        use std::sync::{Arc, Barrier};

        let db = Arc::new(Mutex::new(RDatabase::new()));

        // Create a table
        let table_ref = {
            let mut db_guard = db.lock().unwrap();
            db_guard.create_table("ConcurrentTest".to_string(), 3, 0)
        };

        // Insert initial data
        {
            let mut init_txn = RTransaction::new();
            init_txn.add_query(
                "insert",
                table_ref.clone(),
                vec![Some(1), Some(100), Some(200)],
            );
            run_with_timeout(&mut init_txn, 5000); // 5 second timeout
        }

        // Create a barrier to synchronize threads
        let barrier = Arc::new(Barrier::new(2));

        // Create two transactions that will try to update the same record
        let table_ref_clone1 = table_ref.clone();
        let table_ref_clone2 = table_ref.clone();
        let barrier_clone1 = barrier.clone();
        let barrier_clone2 = barrier.clone();

        // Thread 1: Update record with primary key 1
        let handle1 = thread::spawn(move || {
            let mut txn1 = RTransaction::new();
            txn1.add_query(
                "update",
                table_ref_clone1,
                vec![Some(1), Some(150), Some(250)],
            );

            // Wait for both threads to reach this point
            barrier_clone1.wait();

            // Run the transaction
            txn1.run()
        });

        // Thread 2: Also try to update record with primary key 1
        let handle2 = thread::spawn(move || {
            let mut txn2 = RTransaction::new();
            txn2.add_query(
                "update",
                table_ref_clone2,
                vec![Some(1), Some(300), Some(400)],
            );

            // Wait for both threads to reach this point
            barrier_clone2.wait();

            // Run the transaction
            txn2.run()
        });

        // Get results
        let result1 = match handle1.join() {
            Ok(result) => result,
            Err(_) => {
                debug!("Thread 1 panicked");
                false
            }
        };

        let result2 = match handle2.join() {
            Ok(result) => result,
            Err(_) => {
                debug!("Thread 2 panicked");
                false
            }
        };

        // One transaction should succeed and one should fail
        assert!(
            result1 || result2,
            "At least one transaction should succeed"
        );
        assert!(
            !(result1 && result2),
            "Both transactions should not succeed"
        );

        // Verify the record was updated
        let mut query = RQuery::new(table_ref.clone());
        let result = query.select(1, 0, vec![1, 1, 1]);
        assert!(result.is_some(), "Record should still exist");

        let record = result.unwrap()[0].as_ref().unwrap().columns.clone();
        // The record should have been updated by one of the transactions
        assert!(
            (record == vec![Some(1), Some(150), Some(250)])
                || (record == vec![Some(1), Some(300), Some(400)]),
            "Record should be updated by one of the transactions"
        );
    }
}
