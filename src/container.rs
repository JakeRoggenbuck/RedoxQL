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
    pub RID_COLUMN: u64,
    pub SCHEMA_ENCODING_COLUMN: u64,
    pub INDIRECTION_COLUMN: u64,
}

/// A base container structure for managing physical pages in a database system.
/// 
/// # Fields
/// 
/// * `physical_pages` - A vector of thread-safe physical pages
/// * `num_cols` - Number of columns in the container
/// * `RID_COLUMN` - Reserved column index for Record ID (0)
/// * `SCHEMA_ENCODING_COLUMN` - Reserved column index for Schema Encoding (1)
/// * `INDIRECTION_COLUMN` - Reserved column index for Indirection (2)
/// 
/// # Methods
/// 
/// ## new
/// Creates a new BaseContainer with the specified number of columns.
/// 
/// # Arguments
/// 
/// * `num_cols` - The number of columns to initialize
/// 
/// # Returns
/// 
/// Returns a new BaseContainer instance
///
/// ## initialize
/// Initializes the container by creating physical pages for:
/// 1. Three reserved system columns (RID, Schema Encoding, and Indirection)
/// 2. User-defined columns based on num_cols
///
/// Each physical page is wrapped in Arc<Mutex<>> for thread-safe access.
impl BaseContainer {
    pub fn new(num_cols: u64) -> Self {
        BaseContainer {
            physical_pages: Vec::new(),
            num_cols,
            RID_COLUMN: 0,
            SCHEMA_ENCODING_COLUMN: 1,
            INDIRECTION_COLUMN: 2,
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
    pub RID_COLUMN: u64,
    pub SCHEMA_ENCODING_COLUMN: u64,
    pub INDIRECTION_COLUMN: u64,
} //same thing as BaseContainer, but with a different name

/// A container for managing tail records in a database table
///
/// The TailContainer maintains physical pages for storing tail records, including three reserved columns:
/// - RID (Record ID) column at index 0
/// - Schema Encoding column at index 1
/// - Indirection column at index 2
///
/// # Fields
/// * `physical_pages` - Vector of thread-safe physical pages storing the actual data
/// * `num_cols` - Number of user-defined columns (excluding reserved columns)
/// * `RID_COLUMN` - Constant index (0) for the RID column
/// * `SCHEMA_ENCODING_COLUMN` - Constant index (1) for the schema encoding column
/// * `INDIRECTION_COLUMN` - Constant index (2) for the indirection column
///
/// # Methods
/// * `new(num_cols: u64)` - Creates a new TailContainer with specified number of columns
/// * `initialize()` - Initializes the container by creating physical pages for all columns
impl TailContainer {
    pub fn new(num_cols: u64) -> Self {
        TailContainer {
            physical_pages: Vec::new(),
            num_cols,
            RID_COLUMN: 0,
            SCHEMA_ENCODING_COLUMN: 1,
            INDIRECTION_COLUMN: 2,
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

mod tests {
    use super::*;

    #[test]
    fn test_base_container_new() {
        let base_container = BaseContainer::new(5);
        assert_eq!(base_container.num_cols, 5);
        assert_eq!(base_container.physical_pages.len(), 0);
    }

    #[test]
    fn test_base_container_initialize() {
        let mut base_container = BaseContainer::new(5);
        base_container.initialize();
        assert_eq!(base_container.physical_pages.len(), 8);
    }

    #[test]
    fn test_tail_container_new() {
        let tail_container = TailContainer::new(5);
        assert_eq!(tail_container.num_cols, 5);
        assert_eq!(tail_container.physical_pages.len(), 0);
    }

    #[test]
    fn test_tail_container_initialize() {
        let mut tail_container = TailContainer::new(5);
        tail_container.initialize();
        assert_eq!(tail_container.physical_pages.len(), 8);
    }
}