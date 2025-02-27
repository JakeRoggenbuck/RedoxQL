use super::page::PhysicalPage;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use super::container::{BaseContainer, TailContainer};

#[derive(Debug, Clone)]
#[pyclass]
pub struct RecordAddress {
    pub page: Arc<Mutex<PhysicalPage>>,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub enum RecordType {
    Base,
    Tail,
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

    pub record_type: RecordType,
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

    fn rid(&self) -> i64 {
        self.rid
    }

    fn schema_encoding(&self) -> RecordAddress {
        let addresses = self.addresses.lock().unwrap();
        addresses[1].clone()
    }

    fn indirection(&self) -> RecordAddress {
        let addresses = self.addresses.lock().unwrap();
        addresses[2].clone()
    }

    fn columns(&self) -> Vec<RecordAddress> {
        let addresses = self.addresses.lock().unwrap();

        let offset = match &self.record_type {
            RecordType::Base => BaseContainer::NUM_RESERVED_COLUMNS,
            RecordType::Tail => TailContainer::NUM_RESERVED_COLUMNS,
        };

        addresses[(offset as usize)..].to_vec()
    }


}
