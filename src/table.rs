use crate::container::default_mask;
use crate::index::RIndexHandle;
use crate::pagerange;

use super::index::RIndex;
use super::pagerange::{PageRange, PageRangeMetadata};
use super::record::{Record, RecordMetadata};
use bincode;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::sync::{Arc, RwLock};

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct PageDirectoryMetadata {
    pub directory: HashMap<i64, RecordMetadata>,
}

#[derive(Default, Clone)]
pub struct PageDirectory {
    pub directory: HashMap<i64, Record>,
}

impl PageDirectory {
    pub fn new() -> Self {
        PageDirectory {
            directory: HashMap::new(),
        }
    }

    fn load_state() -> PageDirectory {
        let hardcoded_filename = "./redoxdata/page_directory.data";

        let file = BufReader::new(File::open(hardcoded_filename).expect("Should open file."));
        let page_meta: PageDirectoryMetadata =
            bincode::deserialize_from(file).expect("Should deserialize.");

        let mut pd: PageDirectory = PageDirectory {
            directory: HashMap::new(),
        };

        // TODO: We need to somehow load all of the physical pages, wrap them
        // in an Arc Mutex, and assign those references to the record addresses
        // that need them
        //
        // We could store the id for where the the physical page it stored on disk
        // in the physical, and then we can load all of them up here, assuming we
        // have something storing the max page index

        for (rid, record_meta) in page_meta.directory {
            pd.directory.insert(rid, record_meta.load_state());
        }

        return pd;
    }

    fn save_state(&self) {
        let hardcoded_filename = "./redoxdata/page_directory.data";

        let mut pd_meta = PageDirectoryMetadata {
            directory: HashMap::new(),
        };

        for (rid, record) in &self.directory {
            let r: RecordMetadata = record.get_metadata();
            pd_meta.directory.insert(*rid, r);
        }

        let table_bytes: Vec<u8> = bincode::serialize(&pd_meta).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&table_bytes).expect("Should serialize.");
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RTableMetadata {
    pub name: String,
    pub primary_key_column: usize,
    pub num_records: i64,
    pub num_columns: usize,
    pub page_range: PageRangeMetadata,
}

pub trait StatePersistence {
    fn load_state(&self) -> RTable {
        let hardcoded_filename = "./redoxdata/table.data";

        let file = BufReader::new(File::open(hardcoded_filename).expect("Should open file."));
        let table_meta: RTableMetadata =
            bincode::deserialize_from(file).expect("Should deserialize.");

        RTable {
            name: table_meta.name.clone(),
            primary_key_column: table_meta.primary_key_column,
            num_columns: table_meta.num_columns,
            num_records: table_meta.num_records,

            page_range: PageRange::load_state(),
            page_directory: PageDirectory::load_state(),
            index: Arc::new(RwLock::new(RIndex::new())),
        }
    }
}

impl StatePersistence for RTableMetadata {}
impl StatePersistence for RTable {}

#[derive(Clone, Default)]
#[pyclass]
pub struct RTable {
    pub name: String,

    pub primary_key_column: usize,

    pub page_range: PageRange,

    // Map RIDs to Records
    pub page_directory: PageDirectory,

    pub num_records: i64,

    #[pyo3(get)]
    pub num_columns: usize,

    pub index: Arc<RwLock<RIndex>>,
}

impl RTable {
    pub fn write(&mut self, values: Vec<i64>) -> Record {
        // Use the primary_key_column'th value as the given key
        let primary_key = values[self.primary_key_column];

        let rid = self.num_records;
        {
            let mut index = self.index.write().unwrap();
            index.add(primary_key, rid);
        }
        let rec = self.page_range.write(rid, values);

        // Save the RID -> Record so it can later be read
        self.page_directory.directory.insert(rid, rec.clone());

        self.num_records += 1;
        return rec;
    }

    pub fn read_base(&self, primary_key: i64) -> Option<Vec<i64>> {
        // Lookup RID from primary_key
        let index = self.index.try_read().unwrap();
        let rid = index.get(primary_key);

        if let Some(r) = rid {
            let rec = self.page_directory.directory.get(&r);

            // If the rec exists in the page_directory, return the read values
            match rec {
                Some(r) => {
                    // Create a column mask that selects all columns (all 1s)
                    // Convert Option<Option<i64>> to Option<i64>
                    return self
                        .page_range
                        .read(r.clone(), default_mask(&r, false))
                        .map(|values| values.into_iter().map(|v| v.unwrap_or(0)).collect());
                }
                None => return None,
            }
        }

        None
    }

    pub fn read(&self, primary_key: i64) -> Option<Vec<i64>> {
        let Some(result) = self.read_base(primary_key as i64) else {
            return None;
        };
        let base_rid = result[self.page_range.base_container.rid_col as usize];
        let base_indirection_column =
            result[self.page_range.base_container.indirection_col as usize];

        if base_rid == base_indirection_column {
            return Some(result);
        }

        let Some(tail_record) = self.page_directory.directory.get(&base_indirection_column) else {
            return None;
        };

        // Create a column mask that selects all columns (all 1s)
        // Read with column mask and unwrap Option<i64> values
        let a = self.page_range.read(tail_record.clone(), default_mask(&tail_record, false));

        if let Some(values) = a {
            return Some(values.into_iter().filter_map(|v| v).collect());
        }

        None
    }

    // Given a RID, get the record's values
    pub fn read_by_rid(&self, rid: i64) -> Option<Vec<i64>> {
        if let Some(record) = self.page_directory.directory.get(&rid) {
            return self.page_range.read(record.clone(), default_mask(&record, false))
                .map(|values| values.into_iter().map(|v| v.unwrap_or(0)).collect());
        }
        None
    }

    pub fn read_relative(&self, primary_key: i64, relative_version: i64) -> Option<Vec<i64>> {
        let Some(base) = self.read_base(primary_key as i64) else {
            return None;
        };
        let base_rid = base[self.page_range.base_container.rid_col as usize];
        let base_indirection_column =
            base[self.page_range.base_container.indirection_col as usize];
        if base_rid == base_indirection_column {
            return Some(base);
        }

        let mut current_rid = base_indirection_column;
        let mut versions_back = 0;
        let target_version = relative_version.abs() as i64;

        while versions_back < target_version {
            let Some(current_record) = self.page_directory.directory.get(&current_rid) else {
                return None;
            };

            // read the current record
            let Some(record_data) = self.page_range.read(current_record.clone(), default_mask(&current_record, false)) else {
                return None;
            };

            // get the indirection of the previous version
            let prev_indirection: i64 =
                record_data[self.page_range.tail_container.indirection_col as usize].unwrap();

            // if we've reached the base record, stop here
            if prev_indirection == base_rid {
                current_rid = base_rid;
                break;
            }

            current_rid = prev_indirection;
            versions_back += 1;
        }

        // read the final record we want
        let Some(final_record) = self.page_directory.directory.get(&current_rid) else {
            return None;
        };

        return self.page_range.read(final_record.clone(), default_mask(&final_record, false))
            .map(|values| values.into_iter().map(|v| v.unwrap_or(0)).collect());
    }

    pub fn delete(&mut self, primary_key: i64) {
        // Lookup RID from primary_key
        let index = self.index.read().unwrap();
        let rid = index.get(primary_key);

        if let Some(r) = rid {
            self.page_directory.directory.remove(&r);
        }
    }

    pub fn sum(&mut self, start_primary_key: i64, end_primary_key: i64, col_index: i64) -> i64 {
        let mut agg = 0i64;

        for primary_key in start_primary_key..end_primary_key + 1 {
            if let Some(v) = self.read(primary_key) {
                agg += v[(v.len() - self.num_columns + col_index as usize)] as i64;
            }
        }

        return agg;
    }

    pub fn sum_version(
        &mut self,
        start_primary_key: i64,
        end_primary_key: i64,
        col_index: i64,
        relative_version: i64,
    ) -> i64 {
        let mut agg = 0i64;

        for primary_key in start_primary_key..end_primary_key + 1 {
            if let Some(v) = self.read_relative(primary_key, relative_version) {
                agg += v[(col_index + 3) as usize] as i64;
            }
        }

        return agg;
    }

    /// Save the state of RTable in a file
    pub fn save_state(&self) {
        let hardcoded_filename = "./redoxdata/table.data";

        // Save the state of the page range
        self.page_range.save_state();

        self.page_directory.save_state();

        let table_meta = self.get_metadata();

        let table_bytes: Vec<u8> = bincode::serialize(&table_meta).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&table_bytes).expect("Should serialize.");
    }

    pub fn get_metadata(&self) -> RTableMetadata {
        RTableMetadata {
            name: self.name.clone(),
            primary_key_column: self.primary_key_column,
            num_columns: self.num_columns,
            num_records: self.num_records,
            page_range: self.page_range.get_metadata(),
        }
    }

    fn _merge() {
        unreachable!("Not used in milestone 1")
    }
}

#[derive(Default, Clone)]
#[pyclass]
pub struct RTableHandle {
    pub table: Arc<RwLock<RTable>>,
}

#[pymethods]
impl RTableHandle {
    pub fn write(&self, values: Vec<i64>) {
        let mut table = self.table.write().expect("Failed to acquire write lock");
        table.write(values);
    }

    pub fn read(&self, primary_key: i64) -> Option<Vec<i64>> {
        let table = self.table.read().expect("Failed to acquire read lock");
        table.read(primary_key)
    }

    pub fn delete(&self, primary_key: i64) {
        let mut table = self.table.write().expect("Failed to acquire write lock");
        table.delete(primary_key);
    }

    // Allow access to properties
    #[getter]
    pub fn get_num_records(&self) -> i64 {
        let table = self.table.read().expect("Failed to acquire read lock");
        table.num_records
    }

    #[getter]
    pub fn index(&self) -> RIndexHandle {
        let table = self.table.read().expect("Failed to acquire read lock");
        RIndexHandle {
            index: table.index.clone(),
        }
    }

    #[getter]
    pub fn get_name(&self) -> String {
        let table = self.table.read().expect("Failed to acquire read lock");
        table.name.clone()
    }

    #[getter]
    pub fn get_num_columns(&self) -> usize {
        let table = self.table.read().expect("Failed to acquire read lock");
        table.num_columns
    }

    #[getter]
    pub fn get_primary_key_column(&self) -> usize {
        let table = self.table.read().expect("Failed to acquire read lock");
        table.primary_key_column
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::RDatabase;

    #[test]
    fn load_and_save_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("Scores".to_string(), 3, 0);
        let mut table = table_ref.table.write().unwrap();

        table.write(vec![0, 10, 12]);
        table.write(vec![0, 10, 12]);
        table.write(vec![0, 10, 12]);
        table.write(vec![0, 10, 12]);

        table.save_state();

        let new_table: RTable = table.load_state();

        assert_eq!(table.name, new_table.name);
        assert_eq!(table.primary_key_column, new_table.primary_key_column);
        assert_eq!(table.num_records, new_table.num_records);
        assert_eq!(table.num_columns, new_table.num_columns);

        assert_eq!(new_table.num_records, 4);
    }

    #[test]
    fn read_and_write_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("Scores".to_string(), 3, 0);
        let mut table = table_ref.table.write().unwrap();

        // Write
        table.write(vec![0, 10, 12]);

        // Read and check
        assert_eq!(table.read(0).unwrap(), vec![0, 0, 0, 0, 10, 12]);

        // Write
        table.write(vec![1, 20, 30]);

        // Read and check
        assert_eq!(table.read(1).unwrap(), vec![1, 0, 1, 1, 20, 30]);
    }

    #[test]
    fn read_base_and_write_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("Scores".to_string(), 3, 0);
        let mut table = table_ref.table.write().unwrap();

        // Write
        table.write(vec![0, 10, 12]);

        // Read and check
        assert_eq!(table.read_base(0).unwrap(), vec![0, 0, 0, 0, 10, 12]);

        // Write
        table.write(vec![1, 20, 30]);

        // Read and check
        assert_eq!(table.read_base(1).unwrap(), vec![1, 0, 1, 1, 20, 30]);
    }

    #[test]
    fn sum_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("Scores".to_string(), 2, 0);
        let mut table = table_ref.table.write().unwrap();

        table.write(vec![0, 10]);
        table.write(vec![1, 20]);
        table.write(vec![2, 5]);
        table.write(vec![3, 100]);

        // Sum the values in col 1
        assert_eq!(table.sum(0, 3, 1), 135);

        // Sum the primary keys in col 0
        assert_eq!(table.sum(0, 3, 0), 6);

        // Sum the values in col 1 from 1-2
        assert_eq!(table.sum(1, 2, 1), 25);
    }

    #[test]
    fn delete_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("Scores".to_string(), 3, 0);
        let mut table = table_ref.table.write().unwrap();

        // Write
        table.write(vec![0, 10, 12]);
        // Read and check
        assert_eq!(table.read_base(0).unwrap(), vec![0, 0, 0, 0, 10, 12]);

        // Delete
        table.delete(0);
        // Read and find None
        assert_eq!(table.read_base(0), None);
    }
}
