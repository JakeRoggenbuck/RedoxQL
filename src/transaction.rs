use super::query::RQuery;
use super::table::{RTable, RTableHandle};
use log::debug;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
enum QueryFunctions {
    Insert,
}

#[derive(Clone)]
struct SingleQuery {
    func: QueryFunctions,
    table: RTableHandle,
    args: Vec<i64>,
}

#[pyclass]
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

    pub fn add_query(&mut self, function_name: &str, table: RTableHandle, args: Vec<i64>) {
        let q = SingleQuery {
            func: QueryFunctions::Insert,
            table,
            args,
        };

        self.queries.push_back(q.clone());
        debug!("Pushed {:?}", q.func);
    }

    pub fn run(&mut self) {
        debug!("Started run!");

        for q in self.queries.iter_mut() {
            // TODO: Don't make a new RQuery for each call
            // We need to right now because there might be a test where
            // different tables are in the same query
            let t = q.table.clone();
            let mut query = RQuery::new(t);

            match q.func {
                QueryFunctions::Insert => {
                    if q.args.len() > 0 {
                        query.insert(q.args.clone());
                    }
                }
            }
        }
    }
}
