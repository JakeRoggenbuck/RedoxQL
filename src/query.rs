use super::database::RTable;
use pyo3::prelude::*;
use std::iter::zip;

#[pyclass]
pub struct Query {
    pub table: RTable,
}

impl Query {
    fn new(table: RTable) -> Self {
        Query { table }
    }

    fn delete(&mut self, primary_key: i64) {
        // Delete the value in each column where the id == primary_key
        // i.e. delete the whole record
    }

    fn insert(&mut self, values: Vec<u64>) {
        self.table.write(values);
    }

    fn select(
        &mut self,
        search_key: i64,
        _search_key_index: i64,
        projected_columns_index: Vec<i64>,
    ) -> Vec<i64> {
        vec![]
    }

    fn select_version(&mut self) {}

    fn update(&mut self) {}

    fn sum_version(&mut self) {}

    fn increment(&mut self) {}
}
