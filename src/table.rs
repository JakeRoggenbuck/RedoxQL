use pyo3::prelude::*;

use super::record::{RecordAddress, Record};
use std::sync::{Arc, Mutex};
use super::page::PhysicalPage;

type RID = u64;

#[pyclass]
pub struct RTable {
    pub name: String,
    pub primary_key_column: u64,
    pub page_ranges: Vec<Arc<Mutex<PageRange>>>,
    // TODO: Fix this to be the correct usage
    pub page_directory: HashMap<RID, Record>,
    // TODO: Add index
}

impl RTable {
    fn create_column(&mut self) -> usize {
        0usize
    }

    fn _merge() {
        unreachable!("Not used in milestone 1")
    }
}