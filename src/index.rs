use pyo3::prelude::*;
use super::table::RTable;
use super::record::Record;
use super::pagerange::PageRange;
use std::collections::{BTreeMap, HashMap};

#[pyclass]
#[derive(Clone, Default)]
pub struct RIndex {
    /// Map a primary_key to a RID
    /// RIDs are used internally and are auto incremented
    /// The primary_key are given to the Python Query by the user of the library
    pub index: BTreeMap<i64, i64>,
    pub secondary_indices: HashMap<i64, BTreeMap<i64, Vec<i64>>>,
}

impl RIndex {
    pub fn new() -> RIndex {
        RIndex {
            index: BTreeMap::new(),
            secondary_indices: HashMap::new(),
        }
    }

    /// Create a mapping from primary_key to RID
    pub fn add(&mut self, primary_key: i64, rid: i64) {
        self.index.insert(primary_key, rid);
    }

    /// Return the RID that we get from the primary_key
    pub fn get(&self, primary_key: i64) -> Option<&i64> {
        self.index.get(&primary_key)
    }

    // Build a secondary index on a non-primary column. This is called by RTable.create_index
    pub fn create_index_internal(&mut self, col_index: i64, page_directory: &HashMap<i64, Record>, page_range: &PageRange) {
        let mut sec_index: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
        for (&rid, record) in page_directory.iter() {
            if let Some(record_data) = page_range.read(record.clone()) {
                if record_data.len() <= (col_index + 3) as usize {
                    // Skip if the record data is unexpectedly short.
                    continue;
                }
                // user columns start at offset 3
                let val = record_data[(col_index + 3) as usize];
                sec_index.entry(val).or_insert_with(Vec::new).push(rid);
            }
        }
        self.secondary_indices.insert(col_index, sec_index);
    }

    // Remove the secondary index on the given column. This is called by RTable.drop_index
    pub fn drop_index_internal(&mut self, col_index: i64) {
        self.secondary_indices.remove(&col_index);
    }

    // Update secondary indices when a record is inserted/updated/deleted.
    pub fn secondary_index_insert(&mut self, col_index: i64, rid: i64, value: i64) {
        if let Some(sec_index) = self.secondary_indices.get_mut(&col_index) {
            sec_index.entry(value).or_insert(Vec::new()).push(rid);
        }
    }
    
    pub fn secondary_index_update(&mut self, col_index: i64, rid: i64, old_value: i64, new_value: i64) {
        if let Some(sec_index) = self.secondary_indices.get_mut(&col_index) {
            if let Some(vec_rids) = sec_index.get_mut(&old_value) {
                vec_rids.retain(|&r| r != rid);
            }
            sec_index.entry(new_value).or_insert(Vec::new()).push(rid);
        }
    }
    
    pub fn secondary_index_delete(&mut self, col_index: i64, rid: i64, value: i64) {
        if let Some(sec_index) = self.secondary_indices.get_mut(&col_index) {
            if let Some(vec_rids) = sec_index.get_mut(&value) {
                vec_rids.retain(|&r| r != rid);
            }
        }
    }
    pub fn get_secondary_indices(&self) -> HashMap<i64, Vec<(i64, Vec<i64>)>> {
        let mut out = HashMap::new();
        for (&col, tree) in self.secondary_indices.iter() {
            let mut vec = Vec::new();
            for (&val, rids) in tree.iter() {
                vec.push((val, rids.clone()));
            }
            out.insert(col, vec);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_test() {
        let mut index = RIndex::new();

        index.add(1, 100);

        assert_eq!(index.get(1).unwrap(), &100);

        index.add(10, 1010101);

        assert_eq!(index.get(10).unwrap(), &1010101);
    }

    mod secondary_index_tests {
        use super::*;
        use crate::table::RTable;
        use crate::pagerange::PageRange;
        use std::collections::HashMap;
    
        #[test]
        fn test_create_and_drop_secondary_index_on_col1() {
            // Create a dummy table with 3 columns.
            let mut table = RTable {
                name: "dummy".to_string(),
                primary_key_column: 0,
                page_range: PageRange::new(3),
                page_directory: HashMap::new(),
                num_records: 0,
                num_columns: 3,
                index: RIndex::new(),
            };
    
            // Insert three records:
            // Record 1: [1, 10, 20]
            // Record 2: [2, 10, 30]
            // Record 3: [3, 20, 40]
            table.write(vec![1, 10, 20]);
            table.write(vec![2, 10, 30]);
            table.write(vec![3, 20, 40]);
            // Each stored record becomes [rid, 0, rid, user0, user1, user2].
            // Thus, for a record inserted as [1,10,20], read_record returns [0,0,0,1,10,20].
    
            // Build a secondary index on user column 1.
            // That accesses record_data[(1+3)] i.e. index 4.
            let mut index = RIndex::new();
            index.create_index_internal(1, &table.page_directory, &table.page_range);
            {
                let sec = index.secondary_indices.get(&1).expect("Index on col 1 not created");
                // Both record 1 and record 2 have user column1 value 10.
                assert_eq!(sec.get(&10).unwrap(), &vec![0, 1]);
                // Record 3 has user column1 value 20.
                assert_eq!(sec.get(&20).unwrap(), &vec![2]);
            }
    
            // Now drop the secondary index on column 1.
            index.drop_index_internal(1);
            assert!(index.secondary_indices.get(&1).is_none());
        }
    
        #[test]
        fn test_create_and_drop_secondary_index_on_col2() {
            // Create a dummy table with 3 user columns.
            let mut table = RTable {
                name: "dummy".to_string(),
                primary_key_column: 0,
                page_range: PageRange::new(3),
                page_directory: HashMap::new(),
                num_records: 0,
                num_columns: 3,
                index: RIndex::new(),
            };
    
            // Insert two records:
            // Record 1: [1, 10, 20]
            // Record 2: [2, 15, 20]
            table.write(vec![1, 10, 20]);
            table.write(vec![2, 15, 20]);
    
            // Build a secondary index on user column 2.
            // That accesses record_data[(2+3)] = record_data[5].
            let mut index = RIndex::new();
            index.create_index_internal(2, &table.page_directory, &table.page_range);
            {
                let sec = index.secondary_indices.get(&2).expect("Index on col 2 not created");
                // Both records have value 20 in column 2.
                assert_eq!(sec.get(&20).unwrap(), &vec![0, 1]);
            }
    
            // Drop the index.
            index.drop_index_internal(2);
            assert!(index.secondary_indices.get(&2).is_none());
        }
    }
    
}
