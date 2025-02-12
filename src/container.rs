use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::page::PhysicalPage;

pub struct BaseContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // number of additional columns
    pub num_cols: u64,

    // reserved columns
    pub RID_COLUMN: u64 = 0,
    pub SCHEMA_ENCODING_COLUMN: u64 = 1,
    pub INDIRECTION_COLUMN: u64 = 2,
}

impl BaseContainer {
    pub fn new(num_cols: u64) -> Self {
        BaseContainer {
            physical_pages: Vec::new(),
            num_cols
        }
    }

    pub fn initialize(&mut self) {
        // initialize the three reserved columns
        let mut rid_page = PhysicalPage::new();
        let mut schema_encoding_page = PhysicalPage::new();
        let mut indirection_page = PhysicalPage::new();

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages.push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages.push(Arc::new(Mutex::new(indirection_page)));

        // initialize the rest of the columns
        for _ in 0..self.num_cols {
            let mut new_page = PhysicalPage::new();
            self.physical_pages.push(Arc::new(Mutex::new(new_page)));
        }
    }
}

pub struct TailContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // number of additional columns
    pub num_cols: u64,

    // reserved columns
    pub RID_COLUMN: u64 = 0,
    pub SCHEMA_ENCODING_COLUMN: u64 = 1,
    pub INDIRECTION_COLUMN: u64 = 2,
} //same thing as BaseContainer, but with a different name

impl TailContainer {
    pub fn new(num_cols: u64) -> Self {
        TailContainer {
            physical_pages: Vec::new(),
            num_cols
        }
    }

    pub fn initialize(&mut self) {
        // initialize the three reserved columns
        let mut rid_page = PhysicalPage::new();
        let mut schema_encoding_page = PhysicalPage::new();
        let mut indirection_page = PhysicalPage::new();

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages.push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages.push(Arc::new(Mutex::new(indirection_page)));

        // initialize the rest of the columns
        for _ in 0..self.num_cols {
            let mut new_page = PhysicalPage::new();
            self.physical_pages.push(Arc::new(Mutex::new(new_page)));
        }

    }
}