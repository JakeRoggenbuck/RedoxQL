use crate::database::{Column, RTable};
use pyo3::prelude::*;
use std::collections::BTreeMap;

/*  A data strucutre holding indices for various columns of a table.
Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well. */

#[pyclass]
pub struct Index {
    /*  EXPLANATION OF BTree, NOT TOO SURE ABOUT THIS.

        A vector of BTreeMaps, can be either Some::BTreeMap or None as its elements.

        BTreeMap<i64, Vec<[usize; 3]>>:
        -- BTreeMap: A balanced binary search tree (B-Tree), for maintaining sorted key-value pairs. --
        -- i64: The key type of the map (the column value being indexed). --

        Vec<[usize; 3]>: Will later change it to the RID of the record.
     */
    indices: Vec<Option<BTreeMap<i64, Vec<[usize; 3]>>>>, // change <[usize; 3]> to RID
    table: RTable,
}

impl Index {
    // Init
    // Mandatory: One index for each table. All our empty initially.
    pub fn new(table: RTable) -> Index {
        let mut indices = vec![None; table.columns.len()];
        indices[table.primary_key_column as usize] = Some(BTreeMap::new());
        Index {
            indices,
            table,
        }
    }

    /// Returns the location of all records with the given value on column "column"
    pub fn locate(&self, column: usize, value: i64) -> Option<&Vec<[usize; 3]>> { // change <[usize; 3]> to RID
        if let Some(tree) = &self.indices[column] {
            return tree.get(&value);
        }
        None
    }

    /// Returns the RIDs of all records with values in column "column" between "begin" and "end"
    pub fn locate_range(&self, begin: i64, end: i64, column: usize) -> Vec<[usize; 3]> { // change <[usize; 3]> to RID
        if let Some(tree) = &self.indices[column] {
            // Gets all entries where the key is between begin and end
            let keys = tree.range(begin..=end);
            // .flat_map() flattens the vector of vector RID's into one vector
            // _ in (_, pointers) ignores the key
            // pointers.clone().collect() extracts vectors of RIDs
            let all_records: Vec<[usize; 3]> = keys.flat_map(|(_, pointers)| pointers.clone()).collect();
            return all_records
        }
        Vec::new()
    }

    /// Create index on specific column
    pub fn create_index(&mut self, column: usize) {
        // Create BTree for column
        if self.indices[column].is_none() {
            self.indices[column] = Some(BTreeMap::new());
            // Populate new index with existing records

            // let table = self.table.lock().unwrap();
            // for rid in table.page_directory.keys() {
            //     let row = table.fetch_row(*rid);
            //     let value = row[column];

            //     // Add RID to index
            //     self.update_index(value, [*rid as usize, column, 0], column).unwrap();
            // }
            }
    }

    /// Insert or update index for a specific column
    pub fn update_index(&mut self, key: i64, pointer: [usize; 3], column: usize) -> Result<(), String> { // change <[usize; 3]> to RID
        if column >= self.indices.len() {
            return Err(format!("Column {} does not exist in table '{}'", column, self.table.name));
        }
        // Gets column Some::BTreeMap, creates one if None
        let tree = self.indices[column].get_or_insert_with(BTreeMap::new);
        // Insert or update key
        // Searches for the given key in the BTree, If the key exists, it returns a mutable reference to the corresponding value, 
        // If the key does not exist, it creates a new entry in the BTree for the key, If the key does not exist, this initializes an empty vector (Vec::new) as the value for the key.
        // Appends the provided RID to the vector associated with the key
        tree.entry(key).or_insert_with(Vec::new).push(pointer);
        Ok(())
    }

    pub fn delete_from_index(&mut self, column: usize, key: i64, pointer: [usize; 3]) { // change <[usize; 3]> to RID
        if let Some(tree) = &mut self.indices[column] {
            if let Some(pointers) = tree.get_mut(&key) {
                // Find the position of the RID to remove
                if let Some(pos) = pointers.iter().position(|&p| p == pointer) {
                    pointers.remove(pos);
                }
                // If no more RID's exist for this key, remove the key
                if pointers.is_empty() {
                    tree.remove(&key);
                }
            }
        }
    }

    /// Drop index of specific column
    pub fn drop_index(&mut self, column: usize) {
        self.indices[column] = None;
    }
}

