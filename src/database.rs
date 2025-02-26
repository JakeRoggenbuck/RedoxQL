use super::index::RIndex;
use super::pagerange::PageRange;
use super::table::{RTable, RTableMetadata, StatePersistence};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RDatabaseMetadata {
    tables: Vec<RTableMetadata>,
    tables_hashmap: HashMap<String, usize>,
}

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

    fn open(&mut self, _path: String) {
        // TODO: Read the metadata of the database
        let db_meta = RDatabaseMetadata::default();

        // Load each table metadata into this current databases' tables
        for table in &db_meta.tables {
            self.tables.push(table.load_state());
        }
    }

    fn close(&self) {
        // TODO: Save the metadata of the database

        let hardcoded_filename = "./database.data";

        let database_meta = RDatabaseMetadata {
            tables: Vec::<RTableMetadata>::new(),
            tables_hashmap: self.tables_hashmap.clone(),
        };

        for table in &self.tables {
            // TODO: Get the metadata for each table
            // TODO: Push it to database_meta.tables
            table.save_state()
        }

        let table_bytes: Vec<u8> = bincode::serialize(&database_meta).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&table_bytes).expect("Should serialize.");
    }

    pub fn create_table(
        &mut self,
        name: String,
        num_columns: i64,
        primary_key_column: i64,
    ) -> RTable {
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
