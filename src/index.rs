use super::table::RTable;
use super::table::{PageDirectory, RTable};
use crate::container::NUM_RESERVED_COLUMNS;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::sync::{Arc, RwLock, Weak};

#[pyclass]
#[derive(Clone, Default)]
pub struct RIndexHandle {
    pub index: Arc<RwLock<RIndex>>,
}

#[pymethods]
impl RIndexHandle {
    pub fn create_index(&mut self, col_index: i64) {
        let mut index = self.index.write().unwrap();
        if let Some(owner_weak) = &index.owner {
            if let Some(owner_arc) = owner_weak.upgrade() {
                let table = owner_arc.read().unwrap();
                index.create_index_internal(col_index, &table);
            } else {
                // Table was dropped, so this index should be considered invalid
                panic!("Table reference no longer valid");
            }
        } else {
            panic!("Owner not set for RIndex");
        }
    }

    pub fn drop_index(&mut self, col_index: i64) {
        let mut index = self.index.write().unwrap();
        index.drop_index_internal(col_index);
    }

    pub fn get_secondary_indices(&self) -> HashMap<i64, Vec<(i64, Vec<i64>)>> {
        let index = self.index.read().unwrap();
        let mut out = HashMap::new();
        for (&col, tree) in index.secondary_indices.iter() {
            let mut vec = Vec::new();
            for (&val, rids) in tree.iter() {
                vec.push((val, rids.clone()));
            }
            out.insert(col, vec);
        }
        out
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct RIndexMetadata {
    pub index: BTreeMap<i64, i64>,
    pub secondary_indices: HashMap<i64, BTreeMap<i64, Vec<i64>>>,
}

#[pyclass]
#[derive(Clone, Default)]
pub struct RIndex {
    #[pyo3(get, set)]
    pub index: BTreeMap<i64, i64>,

    #[pyo3(get, set)]
    pub secondary_indices: HashMap<i64, BTreeMap<i64, Vec<i64>>>,
    // Using Arc<RwLock<>> pattern which is safer than raw pointers
    // these fields are not python exposed
    pub owner: Option<Weak<RwLock<RTable>>>,
}

impl RIndex {
    pub fn new() -> RIndex {
        RIndex {
            index: BTreeMap::new(),
            secondary_indices: HashMap::new(),
            owner: None,
        }
    }

    // Set the owner (the table that "owns" this index)
    pub fn set_owner(&mut self, table_arc: std::sync::Weak<RwLock<RTable>>) {
        self.owner = Some(table_arc);
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
    pub fn create_index_internal(&mut self, col_index: i64, table: &RTable) {
        let mut sec_index: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
        for (&rid, record) in table.page_directory.directory.iter() {
            if let Some(record_data) = table.page_range.read(record.clone()) {
                if record_data.len() <= (col_index + NUM_RESERVED_COLUMNS) as usize {
                    // Skip if the record data is unexpectedly short.
                    continue;
                }
                // user columns start at offset 3
                let val = record_data[(col_index + NUM_RESERVED_COLUMNS) as usize];
                sec_index.entry(val).or_insert_with(Vec::new).push(rid);
            }
            // For each key in the secondary index, sort the vector so that tests compare in order.
            for vec in sec_index.values_mut() {
                vec.sort();
            }
        }
        self.secondary_indices.insert(col_index, sec_index);
    }

    // Remove the secondary index on the given column. This is called by RTable.drop_index
    pub fn drop_index_internal(&mut self, col_index: i64) {
        self.secondary_indices.remove(&col_index);
    }

    // Update secondary indices when a record is inserted/updated/deleted
    pub fn secondary_index_insert(&mut self, col_index: i64, rid: i64, value: i64) {
        if let Some(sec_index) = self.secondary_indices.get_mut(&col_index) {
            sec_index.entry(value).or_insert(Vec::new()).push(rid);
        }
    }

    pub fn secondary_index_update(
        &mut self,
        col_index: i64,
        rid: i64,
        old_value: i64,
        new_value: i64,
    ) {
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

    pub fn save_state(&self) {
        let hardcoded_filename = "./redoxdata/index.data";

        let index_meta = self.get_metadata();

        let index_bytes: Vec<u8> = bincode::serialize(&index_meta).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&index_bytes).expect("Should serialize.");
    }

    pub fn get_metadata(&self) -> RIndexMetadata {
        RIndexMetadata {
            index: self.index.clone(),
            secondary_indices: self.secondary_indices.clone(),
        }
    }

    pub fn load_state(table_ref: Weak<RwLock<RTable>>) -> RIndex {
        let hardcoded_filename = "./redoxdata/index.data";

        let file = BufReader::new(File::open(hardcoded_filename).expect("Should open file."));

        let index_meta: RIndexMetadata =
            bincode::deserialize_from(file).expect("Should deserialize.");

        RIndex {
            index: index_meta.index,
            secondary_indices: index_meta.secondary_indices,
            owner: Some(table_ref),
        }
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
        use crate::pagerange::PageRange;
        use crate::table::PageDirectory;
        use crate::table::RTable;

        #[test]
        fn test_create_and_drop_secondary_index_on_col1() {
            // Create a dummy table with 3 columns.
            let mut table = RTable {
                name: "dummy".to_string(),
                primary_key_column: 0,
                page_range: PageRange::new(3),
                page_directory: PageDirectory::new(),
                num_records: 0,
                num_columns: 3,
                index: Arc::new(RwLock::new(RIndex::new())),
                table_num: 0,
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
            index.create_index_internal(1, &table);
            {
                let sec = index
                    .secondary_indices
                    .get(&1)
                    .expect("Index on col 1 not created");
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
                page_directory: PageDirectory::new(),
                num_records: 0,
                num_columns: 3,
                index: Arc::new(RwLock::new(RIndex::new())),
                table_num: 0,
            };

            // Insert two records:
            // Record 1: [1, 10, 20]
            // Record 2: [2, 15, 20]
            table.write(vec![1, 10, 20]);
            table.write(vec![2, 15, 20]);

            // Build a secondary index on user column 2.
            // That accesses record_data[(2+3)] = record_data[5].
            let mut index = RIndex::new();
            index.create_index_internal(2, &table);
            {
                let sec = index
                    .secondary_indices
                    .get(&2)
                    .expect("Index on col 2 not created");
                // Both records have value 20 in column 2.
                assert_eq!(sec.get(&20).unwrap(), &vec![0, 1]);
            }

            // Drop the index.
            index.drop_index_internal(2);
            assert!(index.secondary_indices.get(&2).is_none());
        }
        #[test]
        fn test_set_owner() {
            // Create a dummy table
            let table = RTable {
                name: "dummy".to_string(),
                primary_key_column: 0,
                page_range: PageRange::new(3),
                page_directory: PageDirectory::new(),
                num_records: 0,
                num_columns: 3,
                index: Arc::new(RwLock::new(RIndex::new())),
                table_num: 0,
            };
            let arc_table = Arc::new(RwLock::new(table));

            let mut index = RIndex::new();

            {
                // let mut table_guard = arc_table.write().unwrap();
                index.set_owner(Arc::downgrade(&arc_table));
            }

            // Verify the owner is set by checking it can be upgraded
            assert!(index.owner.is_some());
            let owner_weak = index.owner.as_ref().unwrap();
            assert!(owner_weak.upgrade().is_some());
        }

        #[test]
        fn test_secondary_index_insert() {
            let mut index = RIndex::new();
            let col_index = 1;

            // Create an empty secondary index
            index.secondary_indices.insert(col_index, BTreeMap::new());

            // Insert a value
            index.secondary_index_insert(col_index, 5, 100);

            // Verify the value was inserted
            let sec_index = index.secondary_indices.get(&col_index).unwrap();
            assert_eq!(sec_index.get(&100).unwrap(), &vec![5]);

            // Insert another value with the same key
            index.secondary_index_insert(col_index, 10, 100);
            let sec_index = index.secondary_indices.get(&col_index).unwrap();
            assert_eq!(sec_index.get(&100).unwrap(), &vec![5, 10]);
        }

        #[test]
        fn test_secondary_index_update() {
            let mut index = RIndex::new();
            let col_index = 1;

            // Create a secondary index with initial values
            let mut btree = BTreeMap::new();
            btree.insert(100, vec![5, 10]);
            btree.insert(200, vec![15]);
            index.secondary_indices.insert(col_index, btree);

            // Update a value (move rid 10 from value 100 to 200)
            index.secondary_index_update(col_index, 10, 100, 200);

            // Verify the update
            let sec_index = index.secondary_indices.get(&col_index).unwrap();
            // Check that we have the right values in each index
            let vec_100 = sec_index.get(&100).unwrap();
            let vec_200 = sec_index.get(&200).unwrap();

            assert_eq!(vec_100.len(), 1);
            assert!(vec_100.contains(&5));

            assert_eq!(vec_200.len(), 2);
            assert!(vec_200.contains(&10));
            assert!(vec_200.contains(&15));
        }

        #[test]
        fn test_secondary_index_delete() {
            let mut index = RIndex::new();
            let col_index = 1;

            // Create a secondary index with initial values
            let mut btree = BTreeMap::new();
            btree.insert(100, vec![5, 10]);
            index.secondary_indices.insert(col_index, btree);

            // Delete a value
            index.secondary_index_delete(col_index, 10, 100);

            // Verify the deletion
            let sec_index = index.secondary_indices.get(&col_index).unwrap();
            assert_eq!(sec_index.get(&100).unwrap(), &vec![5]);

            // Delete the last value
            index.secondary_index_delete(col_index, 5, 100);
            let sec_index = index.secondary_indices.get(&col_index).unwrap();
            assert!(sec_index.get(&100).unwrap().is_empty());
        }
    }
}
