use super::page::PhysicalPage;
use crate::container::{ReservedColumns, NUM_RESERVED_COLUMNS};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// This is the Python object that we return in `select` and `select_version`
/// Making this in Rust improved speed by 30%
#[pyclass]
#[derive(Debug)]
pub struct RReturnRecord {
    #[pyo3(get)]
    pub columns: Vec<Option<i64>>,
}

#[pymethods]
impl RReturnRecord {
    fn __str__(&self) -> String {
        format!("RReturnRecord(columns={:?})", self.columns)
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct RecordAddress {
    pub page: Arc<Mutex<PhysicalPage>>,
    pub offset: i64,
}

impl RecordAddress {
    pub fn get_metadata(&self) -> RecordAddressMetadata {
        RecordAddressMetadata {
            // TODO: Get the index of each page
            page_index: -1,
            offset: self.offset,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RecordAddressMetadata {
    // What page (basically the column index)
    pub page_index: i64,

    pub offset: i64,
}

impl RecordAddressMetadata {
    pub fn load_state(&self, phys_page_ref: Arc<Mutex<PhysicalPage>>) -> RecordAddress {
        RecordAddress {
            page: phys_page_ref,
            offset: self.offset,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RecordMetadata {
    pub rid: i64,

    pub addresses: Vec<RecordAddressMetadata>,
}

impl RecordMetadata {
    pub fn load_state(
        &self,
        base_pages: &HashMap<i64, Arc<Mutex<PhysicalPage>>>,
        _tail_pages: &HashMap<i64, Arc<Mutex<PhysicalPage>>>,
    ) -> Record {
        let mut rec_addrs = Vec::new();

        // Create the RecordAddresses from the metadata
        // This eventually gets propagated through load_state
        // calls all the way to PageDirectory
        let mut index = 0;
        for rec_addr in &self.addresses {
            let p = base_pages.get(&index).expect("Should be a page here.");
            rec_addrs.push(rec_addr.load_state(p.clone()));
            index += 1;
        }

        Record {
            rid: self.rid,
            addresses: Arc::new(Mutex::new(rec_addrs)),
        }
    }
}

#[derive(Debug, Clone, Default)]
#[pyclass]
pub struct Record {
    /// Each Record has a RID and we can retrieve the Record via RTable.page_directory
    #[pyo3(get)]
    pub rid: i64,
    /// The Record keeps a Vector of the RecordAddress, which allow us to actually call
    /// RecordAddress.page.read() to get the value stored at the page using the offset
    pub addresses: Arc<Mutex<Vec<RecordAddress>>>,
}

impl Record {
    pub fn get_metadata(&self) -> RecordMetadata {
        let mut rm = RecordMetadata {
            rid: self.rid,
            addresses: Vec::new(),
        };

        let m = self.addresses.lock().unwrap();
        let addrs = m.iter();

        for addr in addrs {
            // Get the metadata for each RecordAddress
            rm.addresses.push(addr.get_metadata());
        }

        return rm;
    }
}

#[pymethods]
impl Record {
    fn __str__(&self) -> String {
        // Print the Addresses from RecordAddress
        let addresses = self.addresses.lock().unwrap();
        let mut addrs = Vec::<String>::new();
        let addr_vec: Vec<RecordAddress> = addresses.clone();

        for addr in addr_vec {
            let page = addr.page;
            addrs.push(format!(
                "0x{:?} + {}",
                &page as *const Arc<Mutex<PhysicalPage>> as usize, addr.offset
            ));
        }

        format!("Record(rid={}, addresses={:?})", self.rid, addrs)
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn rid(&self) -> i64 {
        self.rid
    }

    pub fn schema_encoding(&self) -> RecordAddress {
        let addresses = self.addresses.lock().unwrap();
        addresses[ReservedColumns::SchemaEncoding as usize].clone()
    }

    pub fn indirection(&self) -> RecordAddress {
        let addresses = self.addresses.lock().unwrap();
        addresses[ReservedColumns::Indirection as usize].clone()
    }

    pub fn base_rid(&self) -> RecordAddress {
        let addresses = self.addresses.lock().unwrap();
        addresses[ReservedColumns::BaseRID as usize].clone()
    }

    pub fn columns(&self) -> Vec<RecordAddress> {
        let addresses = self.addresses.lock().unwrap();

        addresses[(NUM_RESERVED_COLUMNS as usize)..].to_vec()
    }
}
