use pyo3::prelude::*;
use std::collections::HashMap;
use super::table::RTable;

#[pyclass]
pub struct RDatabase {
    tables: Vec<RTable>,
    // Map table names to index on the tables: Vec<RTable>
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

    fn create_table(&mut self, name: String, num_columns: u64, primary_key_column: u64) -> RTable {
        let mut t = RTable {
            name: name.clone(),
            page_ranges: vec![],
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
                println!("{} {}", *id, i);
                *id -= 1;
            }
        }

        // Remove from tables hashmap
        self.tables_hashmap.remove(&name);
    }

    #[new]
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
}