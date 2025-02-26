use super::index::RIndex;
use super::pagerange::PageRange;
use super::record::Record;
use bincode;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

#[derive(Serialize, Deserialize, Debug)]
struct RTableMetadata {
    name: String,
    primary_key_column: usize,
    num_records: i64,
    num_columns: usize,
}

#[derive(Clone, Default)]
#[pyclass]
pub struct RTable {
    pub name: String,
    pub primary_key_column: usize,
    pub page_range: PageRange,
    pub page_directory: HashMap<i64, Record>, // Map RIDs to Records
    pub num_records: i64,

    #[pyo3(get)]
    pub num_columns: usize,

    #[pyo3(get)]
    pub index: RIndex,
}

impl RTable {

    pub fn write(&mut self, values: Vec<i64>) -> Record {
        // Use the primary_key_column'th value as the given key
        let primary_key = values[self.primary_key_column];

        let rid = self.num_records;
        self.index.add(primary_key, rid);

        let rec = self.page_range.write(rid, values);

        // Save the RID -> Record so it can later be read
        self.page_directory.insert(rid, rec.clone());

        self.num_records += 1;
        return rec;
    }

    pub fn read_base(&self, primary_key: i64) -> Option<Vec<i64>> {
        // Lookup RID from primary_key
        let rid = self.index.get(primary_key);

        if let Some(r) = rid {
            let rec = self.page_directory.get(&r);

            // If the rec exists in the page_directory, return the read values
            match rec {
                Some(r) => return self.page_range.read(r.clone()),
                None => return None,
            }
        }

        None
    }

    pub fn read(&self, primary_key: i64) -> Option<Vec<i64>> {
        let Some(result) = self.read_base(primary_key as i64) else {
            return None;
        };
        let base_rid = result[self.page_range.base_container.rid_column as usize];
        let base_indirection_column =
            result[self.page_range.base_container.indirection_column as usize];

        if base_rid == base_indirection_column {
            return Some(result);
        }

        let Some(tail_record) = self.page_directory.get(&base_indirection_column) else {
            return None;
        };

        return self.page_range.read(tail_record.clone());
    }

    // Given a RID, get the record's values
    pub fn read_by_rid(&self, rid: i64) -> Option<Vec<i64>> {
        if let Some(record) = self.page_directory.get(&rid) {
            return self.page_range.read(record.clone());
        }
        None
    }

    pub fn read_relative(&self, primary_key: i64, relative_version: i64) -> Option<Vec<i64>> {
        let Some(base) = self.read_base(primary_key as i64) else {
            return None;
        };
        let base_rid = base[self.page_range.base_container.rid_column as usize];
        let base_indirection_column =
            base[self.page_range.base_container.indirection_column as usize];
        if base_rid == base_indirection_column {
            return Some(base);
        }

        let mut current_rid = base_indirection_column;
        let mut versions_back = 0;
        let target_version = relative_version.abs() as i64;

        while versions_back < target_version {
            let Some(current_record) = self.page_directory.get(&current_rid) else {
                return None;
            };

            // read the current record
            let Some(record_data) = self.page_range.read(current_record.clone()) else {
                return None;
            };

            // get the indirection of the previous version
            let prev_indirection: i64 =
                record_data[self.page_range.tail_container.indirection_column as usize];

            // if we've reached the base record, stop here
            if prev_indirection == base_rid {
                current_rid = base_rid;
                break;
            }

            current_rid = prev_indirection;
            versions_back += 1;
        }

        // read the final record we want
        let Some(final_record) = self.page_directory.get(&current_rid) else {
            return None;
        };

        return self.page_range.read(final_record.clone());
    }

    pub fn delete(&mut self, primary_key: i64) {
        // Lookup RID from primary_key
        let rid = self.index.get(primary_key);

        if let Some(r) = rid {
            self.page_directory.remove(&r);
        }
    }

    pub fn sum(&mut self, start_primary_key: i64, end_primary_key: i64, col_index: i64) -> i64 {
        let mut agg = 0i64;

        for primary_key in start_primary_key..end_primary_key + 1 {
            if let Some(v) = self.read(primary_key) {
                agg += v[(col_index + 3) as usize] as i64;
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
        let hardcoded_filename = "./table.data";

        let table_meta = RTableMetadata {
            name: self.name.clone(),
            num_columns: self.num_columns,
            num_records: self.num_records,
            primary_key_column: self.primary_key_column,
        };

        let table_bytes: Vec<u8> = bincode::serialize(&table_meta).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&table_bytes).expect("Should serialize.");
    }

    pub fn load_state(&self) -> RTable {
        let hardcoded_filename = "./table.data";

        let file = BufReader::new(File::open(hardcoded_filename).expect("Should open file."));
        let table_meta: RTableMetadata =
            bincode::deserialize_from(file).expect("Should deserialize.");

        RTable {
            name: table_meta.name.clone(),
            primary_key_column: table_meta.primary_key_column,
            num_columns: table_meta.num_columns,
            num_records: table_meta.num_records,

            // TODO: Should we load these up too or create new ones?
            // I think load them up to, so we need to do that as well
            page_range: PageRange::new(table_meta.num_columns as i64),
            page_directory: HashMap::new(),
            index: RIndex::new(),
        }
    }

    fn _merge() {
        unreachable!("Not used in milestone 1")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{database::RDatabase, query::RQuery};

    #[test]
    fn load_and_save_test() {
        let mut db = RDatabase::new();
        let mut table: RTable = db.create_table("Scores".to_string(), 3, 0);

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
        let mut table: RTable = db.create_table("Scores".to_string(), 3, 0);

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
        let mut table: RTable = db.create_table("Scores".to_string(), 3, 0);

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
        let mut table: RTable = db.create_table("Scores".to_string(), 2, 0);

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
        let mut table: RTable = db.create_table("Scores".to_string(), 3, 0);

        // Write
        table.write(vec![0, 10, 12]);
        // Read and check
        assert_eq!(table.read_base(0).unwrap(), vec![0, 0, 0, 0, 10, 12]);

        // Delete
        table.delete(0);
        // Read and find None
        assert_eq!(table.read_base(0), None);
    }

    #[test]
    fn merge_into_base_page_test() {
        let mut db: RDatabase = RDatabase::new();
        let mut table: RTable = db.create_table("Scores".to_string(), 3, 0);
        let mut q = RQuery::new(table);

        table.write(vec![0, 10, 10]);
        table.write(vec![1, 20, 20]);
        table.write(vec![2, 30, 30]);

        q.update(0, vec![Some(1), Some(11), Some(11)]);
        q.update(1, vec![Some(2), Some(21), Some(21)]);
        q.update(1, vec![Some(3), Some(22), Some(22)]);
        q.update(2, vec![Some(3), Some(31), Some(31)]);

        table._merge();

        // Update after merge is called
        q.update(2, vec![Some(4), Some(32), Some(32)]);

        let merged0 = q.select(0, 0, vec![1, 1, 1]);
        let merged1 = q.select(1, 0, vec![1, 1, 1]);
        let merged2 = q.select(2, 0, vec![1, 1, 1]);

        // TODO: Need to update the schema encoding (I'm not sure how we are doing it/if we are doing it)
        assert_eq!(merged0.unwrap()[0], vec![Some(0), Some(0), Some(3), Some(1), Some(11), Some(11)]);
        assert_eq!(merged1.unwrap()[0], vec![Some(1), Some(0), Some(5), Some(2), Some(21), Some(21)]);
        assert_eq!(merged2.unwrap()[0], vec![Some(2), Some(0), Some(3), Some(3), Some(31), Some(31)]);

        // Check the update after merge
        // Updated record should point back to the base record
        let update_after_merge = q.select(2, 0, vec![1, 1, 1]);
        assert_eq!(update_after_merge.unwrap()[0], vec![Some(3), Some(0), Some(2), Some(3), Some(32), Some(32)]);
    }
}
