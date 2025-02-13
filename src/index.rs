use pyo3::prelude::*;
use std::collections::BTreeMap;

#[pyclass]
#[derive(Clone)]
pub struct RIndex {
    /// Map a primary_key to a RID
    /// RIDs are used internally and are auto incremented
    /// The primary_key are given to the Python Query by the user of the library
    index: BTreeMap<i64, i64>,
}

impl RIndex {
    pub fn new() -> RIndex {
        RIndex {
            index: BTreeMap::new(),
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
}
