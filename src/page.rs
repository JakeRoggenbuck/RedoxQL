use pyo3::prelude::*;

use super::database::{Record, RecordAddress};
use std::collections::HashMap;

use std::sync::{Arc, Mutex};

type RID = u64;

static MAX_SIZE_RECORD: u64 = u64::MAX;

#[pyclass]
#[derive(Clone, Debug)]
pub struct PhysicalPage {
    pub data: Vec<u64>,
    pub num_records: u64,
}

impl PhysicalPage {
    // Init
    pub fn new() -> Self {
        PhysicalPage {
            data: Vec::<u64>::new(),
            num_records: 0,
        }
    }

    pub fn has_capacity(&self) -> bool {
        return self.num_records < MAX_SIZE_RECORD;
    }

    pub fn write(&mut self, value: u64) {
        if self.has_capacity() {
            self.data.push(value);
            self.num_records += 1;
        }
    }

    pub fn read(&self, index: usize) -> Option<u64> {
        Some(self.data[index])
    }
}

pub struct PageDirectory {
    pub records: HashMap<RID, Arc<Mutex<Vec<RecordAddress>>>>,
}

impl PageDirectory {
    pub fn new() -> Self {
        PageDirectory {
            records: HashMap::new(),
        }
    }

    pub fn insert(&mut self, record: Record) {
        self.records.insert(record.rid, record.addresses);
    }

    pub fn get(&self, rid: RID) -> Option<Arc<Mutex<Vec<RecordAddress>>>> {
        self.records.get(&rid).cloned()
    }
}

#[cfg(test)]
mod tests {}
