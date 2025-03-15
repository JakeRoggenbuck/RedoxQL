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

    pub fn commit(&self) {}

    pub fn abort(&self) {}

    pub fn run(&mut self) {
        // TODO: Python expects a bool

        debug!("Started run for transaction!");

        for q in self.queries.iter_mut() {
            // TODO: Don't make a new RQuery for each call
            // We need to right now because there might be a test where
            // different tables are in the same query
            let t = q.table.clone();
            let mut query = RQuery::new(t);

            match q.func {
                QueryFunctions::Delete => {
                    let args_clone = q.args.clone();
                    let first = args_clone.first();

                    if let Some(f) = first {
                        if let Some(a) = f {
                            query.delete(*a);
                        }
                    }
                }
                QueryFunctions::Insert => {
                    let mut args = Vec::new();
                    let args_clone = q.args.clone();

                    // Get only Some type from args
                    // Should be everything!
                    for a in args_clone {
                        if let Some(b) = a {
                            args.push(b);
                        } else {
                            debug!("Optional passes to insert incorrectly!");
                        }
                    }

                    query.insert(args);
                }
                QueryFunctions::Update => {
                    let args_clone = q.args.clone();
                    let first = args_clone.first();
                    let rest = args_clone.iter().skip(1).cloned().collect();

                    if let Some(f) = first {
                        if let Some(a) = f {
                            query.update(*a, rest);
                        }
                    }
                }
                QueryFunctions::Sum => {
                    let args_clone = q.args.clone();
                    if args_clone.len() == 3 {
                        let start = args_clone[0];
                        let end = args_clone[1];
                        let col = args_clone[2];

                        match (start, end, col) {
                            (Some(s), Some(e), Some(c)) => {
                                query.sum(s, e, c);
                            }
                            _ => {
                                debug!("Wrong args for sum.");
                            }
                        }
                    }
                }
                QueryFunctions::SumVersion => {
                    let args_clone = q.args.clone();
                    if args_clone.len() > 4 {
                        // TODO: Figure out if indexing is actually slower than .get and then
                        // checking if it's optional
                        let start = args_clone[0];
                        let end = args_clone[1];
                        let cols = &args_clone[2..args_clone.len() - 1].to_vec();

                        let ver = args_clone[args_clone.len() - 1];

                        // Get just Some type
                        let mut new_cols = vec![];
                        for c in cols {
                            if let Some(a) = c {
                                new_cols.push(*a);
                            }
                        }

                        let newer_cols = Some(new_cols);

                        // Run select_version version safely only if all args are there
                        match (start, end, newer_cols, ver) {
                            (Some(s), Some(e), Some(c), Some(v)) => {
                                query.select_version(s, e, c, v);
                            }
                            _ => {
                                debug!("Wrong args for sum_version.");
                            }
                        }
                    }
                }
                QueryFunctions::Increment => {
                    let args_clone = q.args.clone();
                    if args_clone.len() == 2 {
                        let key = args_clone[0];
                        let col = args_clone[1];

                        match (key, col) {
                            (Some(k), Some(c)) => {
                                query.increment(k, c);
                            }
                            _ => {
                                debug!("Wrong args for increment.");
                            }
                        }
                    }
                }
                QueryFunctions::None => {
                    debug!("Something went wrong.")
                }
            }
        }

        debug!("Finished run for transaction!");
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
