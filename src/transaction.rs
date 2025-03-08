use super::query::RQuery;
use crate::table::RTable;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Default)]

#[pyclass]
pub struct RTransaction {
    queries: Vec<(RQuery, Vec<i64>)>,
}

#[pymethods]
impl RTransaction {
    #[new]
    pub fn new() -> Self {
        RTransaction {
            queries: Vec::new(),
        }
    }

    pub fn add_query(&mut self, query: RQuery, args: Vec<i64>) {
        self.queries.push((query, args));
    }

    fn run(&mut self) {
        
    }

    fn abort(&mut self) {
        
    }

    fn commit(&mut self) {
        
    }

}