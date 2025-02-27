use super::page::PhysicalPage;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
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
        tail_pages: &HashMap<i64, Arc<Mutex<PhysicalPage>>>,
    ) -> Record {
        let mut rec_addrs = Vec::new();

        // Create the RecordAddresses from the metadata
        // This eventually gets propagated through load_state
        // calls all the way to PageDirectory
        let mut index = 0;
        for rec_addr in &self.addresses {
            // TODO: Choose if it's supposed to be a base page or a tail page
            // Maybe I can use schema_encoding for this
            // TODO: Should I try to do a .get for a tail page and use the base page if it's not
            // found? That might be a great way of doing this!
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

#[derive(Debug, Clone)]
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
}
