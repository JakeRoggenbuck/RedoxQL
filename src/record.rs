use super::page::PhysicalPage;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RecordMetadata {
    pub rid: i64,
    // TODO: We can find out what PhysicalPage and what Offset are used to then make RecordAddress
    // To get the PhysicalPage, we can just look at the index of the column
    // Offset, I am not exactly sure
    //
    // Maybe when we save the state, we just store all of it
    pub addresses: Vec<RecordAddressMetadata>,
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
