use super::page::PhysicalPage;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct RecordAddress {
    pub page: Arc<Mutex<PhysicalPage>>,
    pub offset: i64,
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
