use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::page::PhysicalPage;

#[derive(Clone)]
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

/// A container that manages physical pages for storing data in columns
/// 
/// The `BaseContainer` maintains a collection of physical pages where each page represents
/// a column of data. It reserves the first three columns for special purposes:
/// 
/// - RID_COLUMN (0): Record IDs
/// - SCHEMA_ENCODING_COLUMN (1): Schema encoding information
/// - INDIRECTION_COLUMN (2): Indirection records
/// 
/// # Fields
/// 
/// - `physical_pages`: A vector of physical pages
/// - `num_cols`: The number of additional columns
/// - `RID_COLUMN`: The index of the RID column
/// - `SCHEMA_ENCODING_COLUMN`: The index of the schema encoding column
/// - `INDIRECTION_COLUMN`: The index of the indirection column
impl BaseContainer {

    /// Creates a new `BaseContainer` with the specified number of columns
    /// 
    /// # Arguments
    /// 
    /// - `num_cols`: The number of additional columns
    /// 
    /// # Returns
    /// 
    /// A new `BaseContainer` instance
    pub fn new(num_cols: u64) -> Self {
        BaseContainer {
            physical_pages: Vec::new(),
            num_cols,
            RID_COLUMN: 0,
            SCHEMA_ENCODING_COLUMN: 1,
            INDIRECTION_COLUMN: 2,
        }
    }

    /// Initializes the container by creating physical pages for each column
    /// 
    /// The `initialize` method creates physical pages for each column in the container.
    /// It reserves the first three columns for special purposes and initializes the rest
    /// of the columns with empty pages.
    /// 
    /// # Example
    /// 
    /// ```
    /// let mut container = BaseContainer::new(5);
    /// container.initialize();
    /// ```
    /// 
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

    /// Returns a reference to the RID column page
    pub fn rid_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.RID_COLUMN as usize].clone()
    }

    /// Returns a reference to the schema encoding column page
    pub fn schema_encoding_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.SCHEMA_ENCODING_COLUMN as usize].clone()
    }

    /// Returns a reference to the indirection column page
    pub fn indirection_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.INDIRECTION_COLUMN as usize].clone()
    }

    /// Returns a reference to the specified column page
    pub fn column_page(&self, col_idx: u64) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[(col_idx + 3) as usize].clone()
    }
}

#[derive(Clone)]
pub struct TailContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // number of additional columns
    pub num_cols: u64,

    // reserved columns
    pub RID_COLUMN: u64,
    pub SCHEMA_ENCODING_COLUMN: u64,
    pub INDIRECTION_COLUMN: u64,
}

/// A container that manages physical pages for storing data in columns
/// 
/// The `TailContainer` maintains a collection of physical pages where each page represents
/// a column of data. It reserves the first three columns for special purposes:
/// 
/// - RID_COLUMN (0): Record IDs
/// - SCHEMA_ENCODING_COLUMN (1): Schema encoding information
/// - INDIRECTION_COLUMN (2): Indirection records
/// 
/// # Fields
/// 
/// - `physical_pages`: A vector of physical pages
/// - `num_cols`: The number of additional columns
/// - `RID_COLUMN`: The index of the RID column
/// - `SCHEMA_ENCODING_COLUMN`: The index of the schema encoding column
/// - `INDIRECTION_COLUMN`: The index of the indirection column
impl TailContainer {

    /// Creates a new `TailContainer` with the specified number of columns
    /// 
    /// # Arguments
    /// 
    /// - `num_cols`: The number of additional columns
    /// 
    /// # Returns
    /// 
    /// A new `TailContainer` instance
    pub fn new(num_cols: u64) -> Self {
        TailContainer {
            physical_pages: Vec::new(),
            num_cols,
            RID_COLUMN: 0,
            SCHEMA_ENCODING_COLUMN: 1,
            INDIRECTION_COLUMN: 2,
        }
    }

    /// Initializes the container by creating physical pages for each column
    /// 
    /// The `initialize` method creates physical pages for each column in the container.
    /// It reserves the first three columns for special purposes and initializes the rest
    /// of the columns with empty pages.
    /// 
    /// # Example
    /// 
    /// ```
    /// let mut container = TailContainer::new(5);
    /// container.initialize();
    /// ```
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

    /// Returns a reference to the RID column page
    pub fn rid_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.RID_COLUMN as usize].clone()
    }

    /// Returns a reference to the schema encoding column page
    pub fn schema_encoding_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.SCHEMA_ENCODING_COLUMN as usize].clone()
    }

    /// Returns a reference to the indirection column page
    pub fn indirection_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.INDIRECTION_COLUMN as usize].clone()
    }

    /// Returns a reference to the specified column page
    pub fn column_page(&self, col_idx: u64) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[(col_idx + 3) as usize].clone()
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_container_creation() {
        let container = BaseContainer::new(5);
        assert_eq!(container.num_cols, 5);
        assert_eq!(container.RID_COLUMN, 0);
        assert_eq!(container.SCHEMA_ENCODING_COLUMN, 1);
        assert_eq!(container.INDIRECTION_COLUMN, 2);
        assert!(container.physical_pages.is_empty());
    }

    #[test] 
    fn test_base_container_initialization() {
        let mut container = BaseContainer::new(5);
        container.initialize();
        assert_eq!(container.physical_pages.len(), 8); // 3 reserved + 5 data columns
    }

    #[test]
    fn test_tail_container_creation() {
        let container = TailContainer::new(5);
        assert_eq!(container.num_cols, 5);
        assert_eq!(container.RID_COLUMN, 0);
        assert_eq!(container.SCHEMA_ENCODING_COLUMN, 1);
        assert_eq!(container.INDIRECTION_COLUMN, 2);
        assert!(container.physical_pages.is_empty());
    }

    #[test]
    fn test_tail_container_initialization() {
        let mut container = TailContainer::new(5);
        container.initialize();
        assert_eq!(container.physical_pages.len(), 8); // 3 reserved + 5 data columns
    }

    #[test]
    fn test_container_page_getters() {
        let mut container = BaseContainer::new(2);
        container.initialize();
        
        assert!(Arc::ptr_eq(&container.rid_page(), &container.physical_pages[0]));
        assert!(Arc::ptr_eq(&container.schema_encoding_page(), &container.physical_pages[1]));
        assert!(Arc::ptr_eq(&container.indirection_page(), &container.physical_pages[2]));
        assert!(Arc::ptr_eq(&container.column_page(0), &container.physical_pages[3]));
        assert!(Arc::ptr_eq(&container.column_page(1), &container.physical_pages[4]));
    }
}
