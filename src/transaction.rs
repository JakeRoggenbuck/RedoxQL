use super::query::RQuery;
use super::table::{RTable, RTableHandle};
use log::debug;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
enum QueryFunctions {
    Update,
}

#[derive(Clone)]
struct SingleQuery {
    func: QueryFunctions,
    table: RTableHandle,
    args: Vec<Option<i64>>,
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

    pub fn add_query(&mut self, function_name: &str, table: RTableHandle, args: Vec<Option<i64>>) {
        let q = SingleQuery {
            func: QueryFunctions::Update,
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
            let mut query = RQuery::new(q.table.clone());

            match q.func {
                QueryFunctions::Update => {
                    if q.args.len() > 0 {
                        let first = q.args[0];
                        q.args.drain(0..1);

                        if let Some(f) = first {
                            query.update(f, q.args.clone());

                            debug!("Ran update!");
                        }
                    }
                }
            }
        }
    }
}
