use super::query::RQuery;
use crate::table::{RTable, RTableHandle};
use pyo3::prelude::*;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Default)]
#[pyclass]
pub struct RTransaction {
    queries: Vec<(String, RTableHandle, Vec<i64>)>, // (query, args)
    // locks: LockManager, // Add a reference to a lock manager
}

#[pymethods]
impl RTransaction {
    #[new]
    pub fn new() -> Self {
        RTransaction {
            queries: Vec::new(),
        }
    }

    /// Add a query to the transaction
    pub fn add_query(&mut self, query_type: String, table_handle: RTableHandle, args: Vec<i64>) {
        self.queries.push((query_type, table_handle, args));
    }

    /// Runs the transaction
    pub fn run(&mut self) -> bool {
        println!("Starting transaction execution with {} queries.", self.queries.len());

        for (query_type, table_handle, args) in &self.queries {
            println!("Executing query: {} with args {:?}", query_type, args);
            
            let t: RTableHandle = table_handle.clone();
            let mut q = RQuery::new(t);
    
            let success = match query_type.as_str() {
                "insert" => {
                    let res = q.insert(args.clone());
                    println!("Insert result: {:?}", res);
                    res
                }
                "delete" => {
                    q.delete(args[0]);
                    println!("Deleted key: {}", args[0]);
                    true
                }
                "select" => {
                    let res = q.select(args[0], args[1], args[2..].to_vec());

                    res.is_some()
                }
                "update" => {
                    let res = q.update(args[0], args.iter().map(|&x| Some(x)).collect());
                    println!("Update result: {:?}", res);
                    res
                }
                "sum" => {
                    let sum_result = q.sum(args[0], args[1], args[2]);
                    println!("Sum result: {}", sum_result);
                    true
                }
                _ => {
                    println!("Unknown query type: {}", query_type);
                    return self.abort();
                }
            };
    
            if !success {
                return self.abort();
            }
        }
    
        println!("Transaction completed successfully.");
        self.commit()
    }
    

    /// Abort transaction: rollback and release all acquired locks
    pub fn abort(&mut self) -> bool{
        println!("Transaction aborted: rolling back changes");
        // for primary_key in &self.locks {
        //     self.locks.release_write(*primary_key);
        //     self.locks.release_read(*primary_key);
        // }
        // self.acquired_locks.clear();
        false
    }

    /// Commit transaction: finalize changes and release all locks
    pub fn commit(&mut self) -> bool{
        println!("Transaction committed successfully");
        // for primary_key in &self.acquired_locks {
        //     self.locks.release_write(*primary_key);
        //     self.locks.release_read(*primary_key);
        // }
        // self.acquired_locks.clear();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::RDatabase;
    use crate::table::{RTableHandle, RTable};
    use crate::query::RQuery;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_create_transaction() {
        let transaction = RTransaction::new();
        assert_eq!(transaction.queries.len(), 0, "Transaction should start with no queries");
    }

    #[test]
    fn test_add_query() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("TestTable".to_string(), 5, 0);
        let mut table = table_ref.table.write().unwrap();
        let mut transaction = RTransaction::new();
        transaction.add_query("insert".to_string(), table_ref.clone(), vec![1, 2, 3, 4, 5]);

        // Print the queries inside the transaction
        println!("Transaction contains {} queries:", transaction.queries.len());
        for (i, (query_type, _table, args)) in transaction.queries.iter().enumerate() {
            println!("  {}: Query Type: {}, Args: {:?}", i + 1, query_type, args);
        }
        assert_eq!(transaction.queries.len(), 1, "Query should be added to the transaction");
    
    }

    #[test]
    fn test_run_commit() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("TestTable".to_string(), 5, 0);
        let mut table = table_ref.table.write().unwrap();

        let mut transaction = RTransaction::new();
        transaction.add_query("insert".to_string(), table_ref.clone(), vec![1, 2, 3, 4, 5]);

        let success = transaction.run();
        assert!(success, "Transaction should commit successfully");
    }


    #[test]
    fn test_transaction_insert_and_select() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("Grades".to_string(), 5, 0);
        let mut table = table_ref.table.write().unwrap();

        let mut transaction = RTransaction::new();

        // Insert records
        transaction.add_query("insert".to_string(), table_ref.clone(), vec![1, 10, 20, 30, 40]);
        transaction.add_query("insert".to_string(), table_ref.clone(), vec![2, 15, 25, 35, 45]);
        transaction.add_query("insert".to_string(), table_ref.clone(), vec![3, 20, 30, 40, 50]);

        assert!(transaction.run(), "Transaction should successfully commit inserts");

        let mut q = RQuery::new(table_ref.clone());

        // Select first record
        let result = q.select(1, 0, vec![1, 1, 1, 1, 1]);
        assert!(result.is_some(), "Select should return a valid record");
        let records = result.unwrap();
        assert_eq!(
            records[0].as_ref().unwrap().columns,
            vec![Some(1), Some(10), Some(20), Some(30), Some(40)],
            "Inserted record should match"
        );

        // Select second record
        let result = q.select(2, 0, vec![1, 1, 1, 1, 1]);
        assert!(result.is_some(), "Select should return a valid record");
        let records = result.unwrap();
        assert_eq!(
            records[0].as_ref().unwrap().columns,
            vec![Some(2), Some(15), Some(25), Some(35), Some(45)],
            "Inserted record should match"
        );
    }
}
