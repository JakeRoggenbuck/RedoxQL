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

    fn delete(&mut self, primary_key: i64) {
        // Delete the value in each column where the id == primary_key
        // i.e. delete the whole record
    }

    fn insert(&mut self, values: Vec<u64>) -> Record {
        self.table.write(values)
    }

    fn select(
        &mut self,
        search_key: i64,
        _search_key_index: i64,
        projected_columns_index: Vec<i64>,
    ) -> Vec<u64> {
        self.table.read(search_key as u64).unwrap()
    }

    fn select_version(&mut self) {}

    fn update(&mut self) {}

    fn sum_version(&mut self) {}

    fn increment(&mut self) {}
}
