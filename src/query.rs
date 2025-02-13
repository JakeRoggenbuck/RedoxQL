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
        primary_key: i64,
        _search_key_index: i64,
        _projected_columns_index: Vec<i64>,
    ) -> Option<Vec<u64>> {
        self.table.read(primary_key as u64)
    }

    fn select_version(&mut self) {}

    fn update(&mut self) {}

    fn sum(&mut self, start_primary_key: u64, end_primary_key: u64, col_index: u64) -> i64 {
        self.table
            .sum(start_primary_key, end_primary_key, col_index)
    }

    fn sum_version(&mut self) {}

    fn increment(&mut self) {}
}
