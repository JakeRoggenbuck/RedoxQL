use crate::table::RTableHandle;

use super::index::RIndex;
use super::pagerange::PageRange;
use super::table::{PageDirectory, RTable, RTableMetadata, StatePersistence};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RDatabaseMetadata {
    tables: Vec<RTableMetadata>,
    tables_hashmap: HashMap<String, usize>,
    db_filepath: Option<String>,
}

#[pyclass]
pub struct RDatabase {
    /// This is where we keep all of the tables
    tables: Vec<Arc<RwLock<RTable>>>,
    // Map table names to index on the tables: Vec<RTable>
    tables_hashmap: HashMap<String, usize>,

    db_filepath: Option<String>,
}

#[pymethods]
impl RDatabase {
    #[new]
    pub fn new() -> Self {
        RDatabase {
            tables: vec![],
            tables_hashmap: HashMap::new(),
            db_filepath: None,
        }
    }

    fn open(&mut self, path: String) {
        if self.db_filepath.is_none() {
            self.db_filepath = Some(path.clone());
        }

        if !Path::new("./redoxdata").exists() {
            create_dir_all("./redoxdata").expect("Should be able to make dir");
        }

        if let Some(p) = &self.db_filepath {
            if !Path::new(&p).exists() {
                // The database has not been closed yet
                // Assuming the users makes sure to close the database before they want to open it
                // again, we don't have to do anything else. We can just exit. The rest of the function
                // only needs to load up the database if something exists to load up
                return;
            }
        }

        // Read the metadata of the database
        let file = BufReader::new(File::open(path).expect("Should open file."));
        let db_meta: RDatabaseMetadata =
            bincode::deserialize_from(file).expect("Should deserialize.");

        // Load each table metadata into this current databases' tables
        let mut index = 0;
        for table in &db_meta.tables {
            let l = table.load_state();
            // l.page_directory.display();

            self.tables.push(Arc::new(RwLock::new(l)));
            self.tables_hashmap.insert(table.name.clone(), index);
            index += 1;
        }
    }

    fn close(&self) {
        let mut database_meta = RDatabaseMetadata {
            tables: Vec::<RTableMetadata>::new(),
            tables_hashmap: self.tables_hashmap.clone(),
            db_filepath: self.db_filepath.clone(),
        };

        for table in &self.tables {
            {
                // Get the metadata for each table
                let tm: RTableMetadata = table.read().unwrap().get_metadata();
                // Push it to database_meta.tables
                database_meta.tables.push(tm);
            }

            // Save the table to disk
            table.read().unwrap().save_state();
        }

        let table_bytes: Vec<u8> = bincode::serialize(&database_meta).expect("Should serialize.");

        match &self.db_filepath {
            Some(p) => {
                let mut file = BufWriter::new(File::create(p).expect("Should open file."));
                file.write_all(&table_bytes).expect("Should serialize.");
            }
            None => {
                // This actually happens in testM1.py when .close() gets called even though there
                // never was a .open to begin with. In this case, we can just create a random
                // filename and save the database, or do nothing. In this case, we can just do
                // nothing.
            }
        }
    }

    pub fn create_table(
        &mut self,
        name: String,
        num_columns: i64,
        primary_key_column: i64,
    ) -> RTableHandle {
        let table = RTable {
            name: name.clone(),
            page_range: PageRange::new(num_columns as i64),
            primary_key_column: primary_key_column as usize,
            page_directory: PageDirectory::new(),
            num_columns: num_columns as usize,
            num_records: 0,
            index: Arc::new(RwLock::new(RIndex::new())),
        };

        let arc_table = Arc::new(RwLock::new(table));

        // Set the owner on the index inside the table
        {
            let table_guard = arc_table.read().unwrap();
            let mut index_guard = table_guard.index.write().unwrap();
            index_guard.set_owner(Arc::downgrade(&arc_table));
        }

        // PREVIOUS IMPLEMENTATION
        // let i = self.tables.len();

        // Map a name of a table to it's index on the self.tables field
        // self.tables_hashmap.insert(name, i);

        // self.tables.push(t);

        // Push t into the tables vector so its address becomes stable.
        self.tables.push(arc_table.clone());
        let i = self.tables.len() - 1;
        // Map a name of a table to its index
        self.tables_hashmap.insert(name, i);

        RTableHandle { table: arc_table }
    }

    fn get_table(&self, name: String) -> RTableHandle {
        let i = self.tables_hashmap.get(&name).expect("Should exist");

        let t = self.tables[*i].clone();

        RTableHandle { table: t }
    }

    fn get_table_from_index(&self, i: i64) -> RTableHandle {
        RTableHandle {
            table: self.tables[i as usize].clone(),
        }
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
    use std::sync::Weak;

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
    fn test_create_table_with_set_owner() {
        let mut db = RDatabase::new();

        // Create a table
        let table = db.create_table(String::from("users"), 3, 0);

        // Verify table was added to the database
        assert_eq!(db.tables.len(), 1);
        assert!(db.tables_hashmap.contains_key("users"));
        assert_eq!(*db.tables_hashmap.get("users").unwrap(), 0);

        // Verify owner is set correctly for the index
        let table_ = table.table.read().unwrap();
        let index = table_.index.read().unwrap();
        assert!(index.owner.is_some());
        if let Some(owner_weak) = &index.owner {
            // Verify owner can be upgraded (reference is valid)
            assert!(owner_weak.upgrade().is_some());

            // Verify the table referenced by the index matches what we expect
            if let Some(owner_arc) = owner_weak.upgrade() {
                let referenced_table = owner_arc.read().unwrap();
                assert_eq!(referenced_table.name, "users");
                assert_eq!(referenced_table.num_columns, 3);
                assert_eq!(referenced_table.primary_key_column, 0);
            }
        }
    }
    #[test]
    fn test_drop_table_memory_management() {
        let mut db = RDatabase::new();

        // Create a table
        let table = db.create_table(String::from("users_to_drop"), 3, 0);

        let weak_ref: Option<Weak<RwLock<RTable>>>;
        {
            // Save a weak reference to the table's owner
            let table_ = table.table.read().unwrap();
            let index = table_.index.read().unwrap();
            weak_ref = if let Some(owner) = &index.owner {
                Some(owner.clone())
            } else {
                panic!("Owner not set properly");
            };
        }
        // drop the table (so we aren't holding onto it manually when checking later)
        drop(table);

        let weakrefb = weak_ref.as_ref().unwrap().clone();
        // Verify the weak reference can be upgraded before dropping
        assert!(weakrefb.upgrade().is_some());

        // Verify the table's properties before dropping
        if let Some(owner_arc) = weakrefb.upgrade() {
            let referenced_table = owner_arc.read().unwrap();
            assert_eq!(referenced_table.name, "users_to_drop");
            assert_eq!(referenced_table.num_columns, 3);
            assert_eq!(referenced_table.primary_key_column, 0);
        }

        // Drop the table
        db.drop_table("users_to_drop".to_string());

        // Verify the table is removed from the database
        assert_eq!(db.tables.len(), 0);
        assert!(!db.tables_hashmap.contains_key("users_to_drop"));

        // Verify the reference is no longer valid (table is fully dropped)
        assert!(
            weakrefb.upgrade().is_none(),
            "Table wasn't properly dropped - Arc reference still exists"
        );
    }

    #[test]
    fn test_create_table_index_owner_is_same_table() {
        let mut db = RDatabase::new();

        // Create a table
        let table1 = db.create_table(String::from("users"), 3, 0);

        {
            table1.table.write().unwrap().write(vec![1, 2, 3]);
        }

        // Verify table was added to the database
        assert_eq!(db.tables.len(), 1);
        assert!(db.tables_hashmap.contains_key("users"));
        assert_eq!(*db.tables_hashmap.get("users").unwrap(), 0);

        // insert data into the table
        let table2 = db.get_table("users".to_string());
        assert_eq!(
            table1.table.read().unwrap().num_records.clone(),
            table2.table.read().unwrap().num_records.clone()
        );

        {
            table2.table.write().unwrap().write(vec![1, 2, 3]);
        }

        // check that the number of records is the same
        assert_eq!(
            table1.table.read().unwrap().num_records.clone(),
            table2.table.read().unwrap().num_records.clone()
        );

        // Verify owner is set correctly for the index
        let table3: std::sync::RwLockReadGuard<'_, RTable> = table1.table.read().unwrap();
        let index = table3.index.read().unwrap();
        assert!(index.owner.is_some());
        if let Some(owner_weak) = &index.owner {
            // Verify owner can be upgraded (reference is valid)
            assert!(owner_weak.upgrade().is_some());

            // Verify the table referenced by the index matches the original table (by value)
            if let Some(owner_arc) = owner_weak.upgrade() {
                // get the table through the weak reference
                let referenced_table = owner_arc.read().unwrap();

                // check the the table's contain the same values
                assert_eq!(referenced_table.name, table3.name);
                assert_eq!(referenced_table.num_columns, table3.num_columns);
                assert_eq!(
                    referenced_table.primary_key_column,
                    table3.primary_key_column
                );
                assert_eq!(referenced_table.num_records, table3.num_records);
            }
        }
    }
}
