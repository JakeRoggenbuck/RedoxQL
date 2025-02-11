use super::index::Index;
use super::page::Page;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct PageRange {
    // Max amount of base pages should be set to 16
    base_pages: Vec<Arc<Mutex<Page>>>,
    tail_pages: Vec<Arc<Mutex<Page>>>,

    // The index of the first non-full base page
    first_non_full_page: usize,
}

impl PageRange {
    fn write(&mut self, value: i64) {
        // Get the current page
        let cur_page = self.base_pages[self.first_non_full_page].clone();

        // Make a closure to prevent multiple mutex lock deadlock
        {
            let page = cur_page.lock().unwrap();

            // Check the current page's capacity
            if !page.has_capacity() {
                self.first_non_full_page += 1;
                self.base_pages.push(Arc::new(Mutex::new(Page::new())));
            }
        }

        let _ = self.base_pages[self.first_non_full_page]
            .lock()
            .unwrap()
            .write(value);
    }

    fn read(&self, index: usize) -> Option<i64> {
        // Get the current page
        let cur_page = self.base_pages[self.first_non_full_page].clone();
        let page = cur_page.lock().unwrap();

        return page.read(index);
    }

    fn new() -> Self {
        PageRange {
            base_pages: vec![Arc::new(Mutex::new(Page::new()))],
            tail_pages: vec![],
            first_non_full_page: 0,
        }
    }
}

struct RecordAddress {
    page: Page,
    offset: i64,
}

struct Record<'a> {
    rid: i64,
    addresses: Vec<&'a RecordAddress>,
}

#[derive(Debug)]
enum DatabaseError {
    OutOfBounds,
}

/// This is the place that actually stores the values
///
/// TODO: Keep track of the Base and Tail Pages
#[pyclass]
pub struct Column {
    page_range: PageRange,
}

impl Column {
    fn insert(&mut self, value: i64) {
        let _ = self.page_range.write(value);
    }

    fn fetch(&self, index: i64) -> Option<i64> {
        // TODO: Out of bounds check
        return self.page_range.read(index as usize);
    }

    fn new() -> Self {
        Column {
            page_range: PageRange::new(),
        }
    }
}

#[derive(Clone)]
#[pyclass]
pub struct Table {
    pub name: String,
    pub primary_key_column: i64,
    pub columns: Vec<Arc<Mutex<Column>>>,

    // TODO: Fix this to be the correct usage
    pub page_directory: HashMap<i64, i64>,
    // TODO: Add index
}

impl Table {
    pub fn insert_row(&mut self, values: Vec<i64>) {
        let mut i = 0usize;

        for value in values {
            // TODO: Handle bounds check for cols
            let m = &self.columns[i];

            let mut col = m.lock().unwrap();

            col.insert(value);
            i += 1;
        }
    }

    pub fn fetch_row(&mut self, index: i64) -> Vec<i64> {
        let mut row = Vec::<i64>::new();

        for m in &self.columns {
            let col = m.lock().unwrap();
            let val = col.fetch(index);

            row.push(val.expect("Value should be fetched"));
        }

        row
    }

    fn insert(&mut self, col_index: usize, value: i64) -> Result<i64, DatabaseError> {
        if col_index >= self.columns.len() {
            return Err(DatabaseError::OutOfBounds);
        }

        // Access the index'th column
        let m: &Arc<Mutex<Column>> = &self.columns[col_index];
        let mut col = m.lock().unwrap();

        // Add another value to the column
        col.insert(value);

        return Ok(value);
    }

    fn fetch(&mut self, col_index: usize, val_index: i64) -> Result<Option<i64>, DatabaseError> {
        if col_index >= self.columns.len() {
            return Err(DatabaseError::OutOfBounds);
        }

        // Access the index'th column
        let m: &Arc<Mutex<Column>> = &self.columns[col_index];
        let col = m.lock().unwrap();

        let v = col.fetch(val_index);
        Ok(Some(v.expect("Value should be fetched.")))
    }

    fn create_column(&mut self) -> usize {
        let c = Arc::new(Mutex::new(Column::new()));
        self.columns.push(c);

        self.columns.len() - 1
    }

    fn _merge() {
        unreachable!("Not used in milestone 1")
    }
}

#[pyclass]
pub struct RDatabase {
    tables: Vec<Table>,
    // Map table names to index on the tables: Vec<Table>
    tables_hashmap: HashMap<String, usize>,
}

#[pymethods]
impl RDatabase {
    #[staticmethod]
    fn ping() -> String {
        return String::from("pong!");
    }

    fn open(&self, _path: String) {
        unreachable!("Not used in milestone 1");
    }

    fn close(&self) {
        unreachable!("Not used in milestone 1");
    }

    fn create_table(&mut self, name: String, num_columns: i64, primary_key_column: i64) -> Table {
        let mut t = Table {
            name: name.clone(),
            columns: vec![],
            primary_key_column,
            page_directory: HashMap::new(),
        };

        // Create num_columns amount of columns
        for _ in 0..num_columns {
            t.create_column();
        }

        let i = self.tables.len();

        // Map a name of a table to it's index on the self.tables field
        self.tables_hashmap.insert(name, i);

        self.tables.push(t);

        // Should it really be cloning here?
        // I guess since it has just an Arc Mutex, the underlying data should persist
        return self.tables[i].clone();
    }

    fn get_table(&self, name: String) -> Table {
        let i = self.tables_hashmap.get(&name).expect("Should exist");
        // Should it really be cloning here?
        return self.tables[*i].clone();
    }

    fn drop_table(&mut self, name: String) {
        let i_ref = self.tables_hashmap.get(&name).expect("Should exist");
        let i = *i_ref;

        // Remove from tables vec
        self.tables.remove(i);

        // c0, c1, c2, c3, c4
        // .remove(2)
        // c0, c1, c3, c4

        // Decrement id
        // c0, c1, c2, c3
        for (_, id) in self.tables_hashmap.iter_mut() {
            if *id > i {
                println!("{} {}", *id, i);
                *id -= 1;
            }
        }

        // Remove from tables hashmap
        self.tables_hashmap.remove(&name);
    }

    #[staticmethod]
    fn new() -> Self {
        RDatabase {
            tables: vec![],
            tables_hashmap: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_table_test() {
        let mut db = RDatabase::new();

        // Create a table "users"
        db.create_table(String::from("users"), 1, 0);

        assert_eq!(db.tables.len(), 1);

        db.drop_table("users".to_string());

        assert_eq!(db.tables.len(), 0);
    }

    #[test]
    fn drop_on_of_many_tables_test() {
        let mut db = RDatabase::new();

        db.create_table(String::from("users"), 1, 0);
        db.create_table(String::from("accounts"), 2, 0);
        db.create_table(String::from("bikes"), 4, 0);

        assert_eq!(db.tables.len(), 3);

        db.drop_table("users".to_string());

        assert_eq!(db.tables.len(), 2);
    }

    #[test]
    fn insert_test() {
        let mut db = RDatabase::new();

        // Create a table "users"
        db.create_table(String::from("users"), 1, 0);

        // This is an internal API
        match db.tables[0].insert(0, 1) {
            Ok(a) => assert_eq!(a, 1),
            Err(e) => panic!("{:?}", e),
        }
    }

    #[test]
    fn fetch_test() {
        let mut db = RDatabase::new();

        // Create a table "users"
        db.create_table(String::from("users"), 1, 0);

        // Create a column
        let c: usize = db.tables[0].create_column();

        match db.tables[0].insert(c, 1) {
            Ok(a) => assert_eq!(a, 1),
            Err(e) => panic!("{:?}", e),
        }

        // Try to fetch the 0th id of the c'th column
        match db.tables[0].fetch(c, 0) {
            Ok(a) => assert_eq!(a, Some(1)),
            Err(e) => panic!("{:?}", e),
        }
    }

    #[test]
    fn insert_row_test() {
        let mut db = RDatabase::new();

        // Create a table "users"
        db.create_table(String::from("users"), 3, 0);

        let users: &mut Table = &mut db.tables[0];
        users.insert_row(vec![0, 11, 12]);
    }

    #[test]
    fn fetch_row_test() {
        let mut db = RDatabase::new();

        // Create a table "users"
        db.create_table(String::from("users"), 3, 0);

        let users: &mut Table = &mut db.tables[0];
        users.insert_row(vec![0, 11, 12]);

        // Fetch the 0th row
        assert_eq!(users.fetch_row(0), vec![0, 11, 12]);
    }
}
