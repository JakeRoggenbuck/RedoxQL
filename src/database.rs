use super::index::RIndex;
use super::pagerange::PageRange;
use super::table::RTable;
use pyo3::prelude::*;
use std::collections::HashMap;


#[pyclass]
pub struct RDatabase {
    /// This is where we keep all of the tables
    tables: Vec<RTable>,
    // Map table names to index on the tables: Vec<RTable>
    tables_hashmap: HashMap<String, usize>,
}

#[pymethods]
impl RDatabase {
    #[new]
    pub fn new() -> Self {
        RDatabase {
            tables: vec![],
            tables_hashmap: HashMap::new(),
        }
    }

    fn open(&self, _path: String) {
        // unreachable!("Not used in milestone 1");
    }

    fn close(&self) {
        // unreachable!("Not used in milestone 1");
    }

    pub fn create_table(&mut self, name: String, num_columns: i64, primary_key_column: i64) -> RTable {
        let t = RTable {
            name: name.clone(),
            page_range: PageRange::new(num_columns as i64),
            primary_key_column: primary_key_column as usize,
            page_directory: HashMap::new(),
            num_columns: num_columns as usize,
            num_records: 0,
            index: RIndex::new(),
        };

        // PREVIOUS IMPLEMENTATION
        // let i = self.tables.len();

        // Map a name of a table to it's index on the self.tables field
        // self.tables_hashmap.insert(name, i);

        // self.tables.push(t);


        // Push t into the tables vector so its address becomes stable.
        self.tables.push(t);
        let i = self.tables.len() - 1;
        // Get a raw pointer to the table in the vector.
        let table_ptr = &self.tables[i] as *const RTable;
        // Set the owner pointer in the index.
        self.tables[i].index.set_owner(table_ptr);
        // Map a name of a table to it's index
        self.tables_hashmap.insert(name, i);

        // Should it really be cloning here?
        // I guess since it has just an Arc Mutex, the underlying data should persi
        return self.tables[i].clone()
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
