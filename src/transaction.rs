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
    None,
}

#[derive(Clone)]
struct SingleQuery {
    func: QueryFunctions,
    table: RTableHandle,
    args: Vec<Option<i64>>,
}

#[pyclass]
#[derive(Clone)]
pub struct RTransaction {
    queries: VecDeque<SingleQuery>,
}

#[pymethods]
impl RTransaction {
    #[new]
    pub fn new() -> Self {
        RTransaction {
            queries: VecDeque::new(),
        }
    }

    pub fn add_query(&mut self, function_name: &str, table: RTableHandle, args: Vec<Option<i64>>) {
        let func = match function_name {
            "delete" => QueryFunctions::Delete,
            "insert" => QueryFunctions::Insert,
            "update" => QueryFunctions::Update,
            "sum" => QueryFunctions::Sum,
            "sum_version" => QueryFunctions::SumVersion,
            "increment" => QueryFunctions::Increment,
            _ => QueryFunctions::None,
        };

        let q = SingleQuery { func, table, args };

        self.queries.push_back(q.clone());
        // debug!("Pushed {:?}", q.func);
    }

    pub fn run(&mut self) -> bool {
        debug!("Started run for transaction!");
    
        let mut queries_executed: Vec<(SingleQuery, bool, Option<Vec<Option<i64>>>)> = Vec::new(); // Track executed queries and previous values
    
        for q in self.queries.iter_mut() {
            let t = q.table.clone();
            let mut query = RQuery::new(t);
            let mut prev_values: Option<Vec<Option<i64>>> = None;
    
            // Extract the number of columns safely before using query
            let num_columns = {
                let table_guard = query.handle.table.read().unwrap();
                table_guard.num_columns
            };
            // Return if transaction completed entirely or not
            let success: bool = match q.func {
                QueryFunctions::Delete => {
                    if let Some(Some(pk)) = q.args.first() {
                        // Capture previous values before deletion
                        if let Some(records) = query.select(*pk, 0, vec![1; num_columns]) {
                            if let Some(record) = records.into_iter().next().flatten() {
                                prev_values = Some(record.columns.clone()); // Save old row before deleting
                            }
                        }
                        query.delete(*pk);
                        true
                    } else {
                        false
                    }
                }
                QueryFunctions::Insert => {
                    let args: Vec<i64> = q.args.iter().filter_map(|x| *x).collect();
                    query.insert(args)
                }
                QueryFunctions::Update => {
                    if let Some(Some(pk)) = q.args.first() {
                        // Capture previous values before updating
                        if let Some(records) = query.select(*pk, 0, vec![1; num_columns]) {
                            if let Some(record) = records.into_iter().next().flatten() {
                                prev_values = Some(record.columns.clone());
                            }
                        }
                        let rest = q.args.iter().skip(1).cloned().collect();
                        query.update(*pk, rest)
                    } else {
                        false
                    }
                }
                QueryFunctions::Sum => {
                    if let (Some(Some(s)), Some(Some(e)), Some(Some(c))) = (q.args.get(0), q.args.get(1), q.args.get(2)) {
                        query.sum(*s, *e, *c);
                        true
                    } else {
                        false
                    }
                }
                QueryFunctions::SumVersion => {
                    if q.args.len() > 4 {
                        let start = q.args[0];
                        let end = q.args[1];
                        let cols: Vec<i64> = q.args[2..q.args.len() - 1].iter().filter_map(|x| *x).collect();
                        let ver = q.args[q.args.len() - 1];
    
                        if let (Some(s), Some(e), Some(v)) = (start, end, ver) {
                            query.select_version(s, e, cols, v);
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
                QueryFunctions::Increment => {
                    if let (Some(Some(pk)), Some(Some(col))) = (q.args.get(0), q.args.get(1)) {
                        // Get previous values before incrementing
                        if let Some(records) = query.select(*pk, 0, vec![1; num_columns]) {
                            if let Some(record) = records.into_iter().next().flatten() {
                                prev_values = Some(record.columns.clone());
                            }
                        }
                        query.increment(*pk, *col)
                    } else {
                        false
                    }
                }
                QueryFunctions::None => {
                    debug!("Something went wrong.");
                    false
                }
            };
    
            queries_executed.push((q.clone(), success, prev_values));
    
            if !success {
                debug!("Transaction failed, rolling back.");
                return self.abort(queries_executed);
            }
        }
    
        debug!("Transaction successful, committing.");
        self.commit();
        true
    }
    
    pub fn commit(&mut self) -> bool {
        debug!("Committing transaction with {} queries.", self.queries.len());

        // Release any necessary locks
        for q in &self.queries {
            let _guard = q.table.table.write().unwrap(); // Unlock tables after commit
        }

        debug!("Transaction committed.");
        true
    }
}

impl RTransaction {
    /// Rolls back successfully executed queries
    fn abort(&mut self, queries_executed: Vec<(SingleQuery, bool, Option<Vec<Option<i64>>>)>) -> bool {
        debug!(
            "Aborting transaction. Rolling back {} successful queries.",
            queries_executed.iter().filter(|(_, success, _)| *success).count()
        );
    
        for (q, success, prev_values) in queries_executed.into_iter().rev() {
            if !success {
                continue; // Skip queries that failed or weren't executed
            }
    
            let mut query = RQuery::new(q.table.clone());
    
            match q.func {
                QueryFunctions::Insert => {
                    if let Some(Some(pk)) = q.args.first() {
                        debug!("Rolling back insert: Deleting {}", pk);
                        query.delete(*pk);
                    }
                }
                QueryFunctions::Update => {
                    if let Some(Some(pk)) = q.args.first() {
                        if let Some(previous_values) = prev_values {
                            debug!("Rolling back update: Restoring {:?}", previous_values);
                            query.update(*pk, previous_values);
                        }
                    }
                }
                QueryFunctions::Delete => {
                    if let Some(previous_values) = prev_values {
                        debug!("Rolling back delete: Re-inserting {:?}", previous_values);
                        query.insert(previous_values.into_iter().map(|x| x.unwrap_or(0)).collect());
                    }
                }
                _ => {
                    debug!("Query type {:?} does not require rollback.", q.func);
                }
            }
        }
        debug!("Transaction aborted.");
        false
    }    
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::RDatabase;

    #[test]
    fn add_query_test() {
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

        // Usually this is done by the transaction_worker but we are doing it manually here
        t.run();

        v = query.select(0, 0, vec![1, 1, 1]);

        assert_eq!(
            v.unwrap()[0].as_ref().unwrap().columns,
            vec![Some(0), Some(234), Some(345)]
        );
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

        // Usually this is done by the transaction_worker but we are doing it manually here
        t.run();

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

        // Usually this is done by the transaction_worker but we are doing it manually here
        t.run();

        v = query.select(0, 0, vec![1, 1, 1]);

        assert_eq!(
            v.unwrap()[0].as_ref().unwrap().columns,
            vec![Some(0), Some(234), Some(345)]
        );

        t.add_query("delete", table_ref, vec![Some(0)]);

        // Usually this is done by the transaction_worker but we are doing it manually here
        t.run();

        v = query.select(0, 0, vec![1, 1, 1]);

        // We haven't run the transaction yet so nothing is returned
        assert_eq!(v.clone().unwrap().len(), 0);
    }
}
