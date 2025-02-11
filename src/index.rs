use crate::database::{Column, RTable};
use pyo3::prelude::*;
use std::collections::BTreeMap;

/*  A data strucutre holding indices for various columns of a table.
Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well. */

#[pyclass]
pub struct Index {
    /*  EXPLANATION OF BTree, NOT TOO SURE ABOUT THIS.
       Option<BTreeMap<...>>: Each element in the vector can be either:
       -- None (no index exists for the column), or --
       -- ome(BTreeMap<...>) (an index exists). --

       BTreeMap<i64, Vec<[usize; 3]>>:
       -- BTreeMap: A balanced binary search tree (B-Tree), for maintaining sorted key-value pairs. --
       -- i64: The key type of the map (e.g., the column value being indexed). --

       Vec<[usize; 3]>: The value type of the map, representing a list of pointers to records.
        -- Each pointer is represented as a 3-element array of usize, referring to:
           -- Page range index --
           -- Page index within that range --
           -- Record offset within the page --
    */
    indices: Vec<Option<BTreeMap<i64, Vec<[usize; 3]>>>>, // One BTree per column
    table: RTable,
}

impl Index {
    // Init
    // Mandatory: One index for each table. All our empty initially.
    pub fn new(table: RTable) -> Index {
        let mut indices = vec![None; table.columns.len()]; // init empty indices
        Index { indices, table }
    }
    // returns the location of all records with the given value on column "column"
    pub fn locate(&self) {}
    // Returns the RIDs of all records with values in column "column" between "begin" and "end"
    pub fn locate_range(&self) {}
    // optional: Create index on specific column
    pub fn create_index(&mut self) {}
    // optional: Drop index of specific column
    pub fn drop_index(&mut self) {}
}
