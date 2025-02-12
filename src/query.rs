use super::database::{RTable, Record};
use pyo3::prelude::*;

#[pyclass]
pub struct RQuery {
    pub table: RTable,
}

#[pymethods]
impl RQuery {
    #[new]
    fn new(table: RTable) -> Self {
        RQuery { table }
    }

    fn delete(&mut self, primary_key: u64) {
        self.table.delete(primary_key)
    }

    fn insert(&mut self, values: Vec<u64>) -> Record {
        self.table.write(values)
    }

    fn select(
        &mut self,
        search_key: i64,
        _search_key_index: i64,
        _projected_columns_index: Vec<i64>,
    ) -> Option<Vec<u64>> {
        self.table.read(search_key as u64)
    }

    fn select_version(&mut self) {}

    fn update(&mut self) {}

    fn sum(&self, start: u64, end: u64, col_index: u64) -> i64 {
        self.table.sum(start, end, col_index)
    }

    fn sum_version(&mut self) {}

    fn increment(&mut self) {}
}
