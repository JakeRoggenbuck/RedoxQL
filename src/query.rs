use super::database::Table;
use pyo3::prelude::*;
use std::iter::zip;

#[pyclass]
pub struct Query {
    pub table: Table,
}

impl Query {
    fn new(table: Table) -> Self {
        Query { table }
    }

    fn delete(&mut self, primary_key: i64) {
        // Delete the value in each column where the id == primary_key
        // i.e. delete the whole record
    }

    fn insert(&mut self, values: Vec<i64>) {
        self.table.insert_row(values);
    }

    fn select(
        &mut self,
        search_key: i64,
        _search_key_index: i64,
        projected_columns_index: Vec<i64>,
    ) -> Vec<i64> {
        let row = self.table.fetch_row(search_key);

        let mut output = Vec::<i64>::new();

        // TODO: Maybe change this to a filter
        for (a, should_return_col) in zip(row, projected_columns_index) {
            if should_return_col == 1 {
                output.push(a);
            }
        }

        output
    }

    fn select_version(&mut self) {}

    fn update(&mut self) {}

    fn sum_version(&mut self) {}

    fn increment(&mut self) {}
}
