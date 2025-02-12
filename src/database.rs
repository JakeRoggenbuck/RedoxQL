use super::index::RIndex;
use super::container::{BaseContainer, TailContainer}
use super::page::PhysicalPage;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const PAGES_PER_PAGE_RANGE: usize = 16;

#[derive(Clone)]
pub struct PageRange {
    base_container: BaseContainer,
    tail_container: TailContainer,

    // The index of the first non-full base page
    first_non_full_page: usize,
}

impl PageRange {
    fn new(num_cols: u64) -> Self {
        let mut base = BaseContainer::new(num_cols);
        base.initialize();

        let mut tail = TailContainer::new(num_cols);
        tail.initialize();

        PageRange {
            base_container: base,
            tail_container: tail,
            first_non_full_page: 0,
        }
    }

    fn write(&mut self, value: i64) {
        // Get the current page
        let cur_page = self.base_pages[self.first_non_full_page].clone();

        // Make a closure to prevent multiple mutex lock deadlock
        {
            let page = cur_page.lock().unwrap();

            // Check the current page's capacity
            if !page.has_capacity() {
                self.first_non_full_page += 1;
                self.base_pages
                    .push(Arc::new(Mutex::new(PhysicalPage::new())));
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

    fn has_capacity(&self) -> bool {
        self.first_non_full_page < PAGES_PER_PAGE_RANGE
    }
}

struct RecordAddress {
    page: PhysicalPage,
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

#[derive(Clone)]
#[pyclass]
pub struct RTable {
    pub name: String,
    pub primary_key_column: i64,
    pub page_range: PageRange,

    // TODO: Fix this to be the correct usage
    pub page_directory: HashMap<i64, i64>,

    #[pyo3(get)]
    pub num_columns: i64,
}

impl RTable {
    fn create_column(&mut self) -> usize {
        let i = self.page_range.base_pages.len();

        self.page_range
            .base_pages
            .push(Arc::new(Mutex::new(PhysicalPage::new())));

        self.num_columns += 1;

        return i;
    }

    fn _merge() {
        unreachable!("Not used in milestone 1")
    }
}

#[pyclass]
pub struct RDatabase {
    tables: Vec<RTable>,
    // Map table names to index on the tables: Vec<RTable>
    tables_hashmap: HashMap<String, usize>,
}

#[pymethods]
impl RDatabase {
    #[new]
    fn new() -> Self {
        RDatabase {
            tables: vec![],
            tables_hashmap: HashMap::new(),
        }
    }

    fn open(&self, _path: String) {
        unreachable!("Not used in milestone 1");
    }

    fn close(&self) {
        unreachable!("Not used in milestone 1");
    }

    fn create_table(&mut self, name: String, num_columns: i64, primary_key_column: i64) -> RTable {
        let mut t = RTable {
            name: name.clone(),
            page_range: PageRange::new(num_columns as u64),
            primary_key_column,
            page_directory: HashMap::new(),
            num_columns: 1,
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

    fn get_table(&self, name: String) -> RTable {
        let i = self.tables_hashmap.get(&name).expect("Should exist");
        // Should it really be cloning here?
        return self.tables[*i].clone();
    }

    fn get_table_from_index(&self, i: i64) -> RTable {
        return self.tables[i as usize].clone();
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
                *id -= 1;
            }
        }

        // Remove from tables hashmap
        self.tables_hashmap.remove(&name);
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
}
