use pyo3::prelude::*;
use super::page::PhysicalPage;

type RID = u64;

pub struct RecordAddress {
    pub page: &PhysicalPage,
    pub offset: u64,
}

#[pyclass]
pub struct Record {
    pub rid: RID,
    pub addresses: Vec<RecordAddress>,
}