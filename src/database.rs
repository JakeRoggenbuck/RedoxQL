use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
enum DatabaseError {
    OutOfBounds,
}

/// This is the place that actually stores the values
///
/// TODO: Keep track of the Base and Tail Pages
#[derive(Clone)]
#[pyclass]
pub struct Column {
    // TODO: This should be pages later
    values: Vec<i64>,
}

impl Column {
    fn insert(&mut self, value: i64) {
        // TODO: Use pages to do this
        self.values.push(value);
    }

    fn fetch(&self, index: usize) -> i64 {
        // TODO: Out of bounds check
        return self.values[index];
    }

    fn new() -> Self {
        Column { values: Vec::new() }
    }
}

/// Hold a shared reference to every column
/// Have multiple Arc<Mutex> instead of an Arc<Mutex<Vec>> so that each can be locked and unlocked
/// separately by different threads
///
/// Columns and Column are just attractions on Base and Tail Pages
pub struct Columns {
    len: usize,
    columns: Vec<Arc<Mutex<Column>>>,
}

impl Columns {
    fn insert(&mut self, col_index: usize, value: i64) -> Result<i64, DatabaseError> {
        if col_index >= self.len {
            return Err(DatabaseError::OutOfBounds);
        }

        // Access the index'th column
        let m: &Arc<Mutex<Column>> = &self.columns[col_index];
        let mut col = m.lock().unwrap();

        // Add another value to the column
        col.insert(value);

        return Ok(value);
    }

    fn fetch(&mut self, col_index: usize, val_index: usize) -> Result<Option<i64>, DatabaseError> {
        if col_index >= self.len {
            return Err(DatabaseError::OutOfBounds);
        }

        // Access the index'th column
        let m: &Arc<Mutex<Column>> = &self.columns[col_index];
        let col = m.lock().unwrap();

        let v = col.fetch(val_index);
        Ok(Some(v))
    }

    fn create_column(&mut self) -> usize {
        let c = Arc::new(Mutex::new(Column::new()));
        self.columns.push(c);

        let index = self.len;
        self.len += 1;

        index
    }

    fn new() -> Self {
        Columns {
            len: 0,
            columns: vec![],
        }
    }
}

pub struct Table {
    pub name: String,
    pub columns: Columns,
}

impl Table {
    fn insert_row(&mut self, values: Vec<i64>) {
        let mut i = 0usize;

        for value in values {
            // TODO: Handle bounds check for cols
            let m = &self.columns.columns[i];

            let mut col = m.lock().unwrap();

            col.insert(value);
            i += 1;
        }
    }

    fn fetch_row(&mut self, index: usize) -> Vec<i64> {
        let mut row = Vec::<i64>::new();

        for m in &self.columns.columns {
            let col = m.lock().unwrap();
            let val = col.fetch(index);

            row.push(val);
        }

        row
    }
}

#[pyclass]
pub struct Database {
    tables: Vec<Table>,
}

#[pymethods]
impl Database {
    #[staticmethod]
    fn ping() -> String {
        return String::from("pong!");
    }

    #[staticmethod]
    fn new() -> Self {
        Database { tables: vec![] }
    }

    fn create_table(&mut self, name: String) {
        let t = Table {
            name,
            columns: Columns::new(),
        };

        self.tables.push(t);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_test() {
        let mut db = Database::new();

        // Create a table "users"
        db.create_table(String::from("users"));

        // Create a column
        let c: usize = db.tables[0].columns.create_column();

        // This is an internal API
        match db.tables[0].columns.insert(c, 1) {
            Ok(a) => assert_eq!(a, 1),
            Err(e) => panic!("{:?}", e),
        }
    }

    #[test]
    fn fetch_test() {
        let mut db = Database::new();

        // Create a table "users"
        db.create_table(String::from("users"));

        // Create a column
        let c: usize = db.tables[0].columns.create_column();

        match db.tables[0].columns.insert(c, 1) {
            Ok(a) => assert_eq!(a, 1),
            Err(e) => panic!("{:?}", e),
        }

        // Try to fetch the 0th id of the c'th column
        match db.tables[0].columns.fetch(c, 0) {
            Ok(a) => assert_eq!(a, Some(1)),
            Err(e) => panic!("{:?}", e),
        }
    }

    #[test]
    fn insert_row_test() {
        let mut db = Database::new();

        // Create a table "users"
        db.create_table(String::from("users"));

        // Create a column
        db.tables[0].columns.create_column();
        db.tables[0].columns.create_column();
        db.tables[0].columns.create_column();

        let users: &mut Table = &mut db.tables[0];
        users.insert_row(vec![0, 11, 12]);
    }

    #[test]
    fn fetch_row_test() {
        let mut db = Database::new();

        // Create a table "users"
        db.create_table(String::from("users"));

        // Create a column
        db.tables[0].columns.create_column();
        db.tables[0].columns.create_column();
        db.tables[0].columns.create_column();

        let users: &mut Table = &mut db.tables[0];
        users.insert_row(vec![0, 11, 12]);

        // Fetch the 0th row
        assert_eq!(users.fetch_row(0), vec![0, 11, 12]);
    }
}
