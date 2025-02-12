use std::sync::{Arc, Mutex};

use super::database::{Record, RecordAddress};
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
        let rid_page = PhysicalPage::new();
        let schema_encoding_page = PhysicalPage::new();
        let indirection_page = PhysicalPage::new();

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(indirection_page)));

        // initialize the rest of the columns
        for _ in 0..self.num_cols {
            let new_page = PhysicalPage::new();
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
    ///
    /// ### Arguments
    ///
    /// - `col_idx`: The index of the column
    pub fn column_page(&self, col_idx: u64) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[(col_idx + 3) as usize].clone()
    }

    pub fn insert_record(&mut self, rid: u64, values: Vec<u64>) -> Record {
        if values.len() != self.num_cols as usize {
            panic!("Number of values does not match number of columns");
        }

        let rid_page = self.rid_page();
        let mut rp = rid_page.lock().unwrap();

        rp.write(rid);

        let schema_encoding_page = self.schema_encoding_page();
        let mut sep = schema_encoding_page.lock().unwrap();
        sep.write(0);

        let indirection_page = self.indirection_page();
        let mut ip = indirection_page.lock().unwrap();

        ip.write(rid);

        for i in 0..self.num_cols {
            let col_page = self.column_page(i);
            let mut col_page = col_page.lock().unwrap();
            col_page.write(values[i as usize]);
        }

        let addresses: Arc<Mutex<Vec<RecordAddress>>> = Arc::new(Mutex::new(Vec::new()));
        let mut a = addresses.lock().unwrap();

        a.push(RecordAddress {
            page: rid_page.clone(),
            offset: rp.num_records - 1,
        });

        a.push(RecordAddress {
            page: schema_encoding_page.clone(),
            offset: sep.num_records - 1,
        });

        a.push(RecordAddress {
            page: indirection_page.clone(),
            offset: ip.num_records - 1,
        });

        for i in 0..self.num_cols {
            let col_page = self.column_page(i);
            let cp = col_page.lock().unwrap();
            a.push(RecordAddress {
                page: col_page.clone(),
                offset: cp.num_records - 1,
            });
        }

        Record {
            rid,
            addresses: addresses.clone(),
        }
    }

    pub fn read_record(&self, record: Record) -> Vec<u64> {
        let mut values = Vec::<u64>::new();

        let addrs = record.addresses.lock().unwrap();
        let addrs_clone = addrs.clone();
        for addr in addrs_clone {
            let a = addr.page.lock().unwrap();
            let b = a.read(addr.offset as usize);
            values.push(b.expect("Value should be there"));
        }

        values
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
        let rid_page = PhysicalPage::new();
        let schema_encoding_page = PhysicalPage::new();
        let indirection_page = PhysicalPage::new();

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(indirection_page)));

        // initialize the rest of the columns
        for _ in 0..self.num_cols {
            let new_page = PhysicalPage::new();
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

    pub fn insert_record(&mut self, rid: u64, values: Vec<u64>) -> Record {
        if values.len() != self.num_cols as usize {
            panic!("Number of values does not match number of columns");
        }

        let rid_page = self.rid_page();
        let mut rp = rid_page.lock().unwrap();

        rp.write(rid);

        let schema_encoding_page = self.schema_encoding_page();
        let mut sep = schema_encoding_page.lock().unwrap();
        sep.write(0);

        let indirection_page = self.indirection_page();
        let mut ip = indirection_page.lock().unwrap();

        ip.write(rid);

        for i in 0..self.num_cols {
            let col_page = self.column_page(i);
            let mut col_page = col_page.lock().unwrap();
            col_page.write(values[i as usize]);
        }

        let addresses: Arc<Mutex<Vec<RecordAddress>>> = Arc::new(Mutex::new(Vec::new()));
        let mut a = addresses.lock().unwrap();

        a.push(RecordAddress {
            page: rid_page.clone(),
            offset: rp.num_records - 1,
        });

        a.push(RecordAddress {
            page: schema_encoding_page.clone(),
            offset: sep.num_records - 1,
        });

        a.push(RecordAddress {
            page: indirection_page.clone(),
            offset: ip.num_records - 1,
        });

        for i in 0..self.num_cols {
            let col_page = self.column_page(i);
            let cp = col_page.lock().unwrap();
            a.push(RecordAddress {
                page: col_page.clone(),
                offset: cp.num_records - 1,
            });
        }

        Record {
            rid,
            addresses: addresses.clone(),
        }
    }

    pub fn read_record(&self, record: Record) -> Vec<u64> {
        let mut values = Vec::<u64>::new();

        let addrs = record.addresses.lock().unwrap();
        let addrs_clone = addrs.clone();
        for addr in addrs_clone {
            let a = addr.page.lock().unwrap();
            let b = a.read(addr.offset as usize);
            values.push(b.expect("Value should be there"));
        }

        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_container_creation() {
        let container = BaseContainer::new(5);
        assert_eq!(container.num_cols, 5);
        assert_eq!(container.physical_pages.len(), 0);
    }

    #[test]
    fn test_base_container_initialize() {
        let mut container = BaseContainer::new(5);
        container.initialize();
        assert_eq!(container.physical_pages.len(), 8); // 3 reserved + 5 data columns
    }

    #[test]
    fn test_base_container_insert() {
        let mut container = BaseContainer::new(2);
        container.initialize();

        let values = vec![42, 43];
        let record = container.insert_record(1, values);

        assert_eq!(record.rid, 1);
        let addresses = record.addresses.lock().unwrap();
        assert_eq!(addresses.len(), 5); // 3 reserved + 2 data columns
    }

    #[test]
    fn test_base_container_insert_513() {
        let mut container = BaseContainer::new(2);
        container.initialize();

        for x in 0..513 {
            let values = vec![42, 43];
            let _record = container.insert_record(1, values);
        }
    }

    #[test]
    #[should_panic(expected = "Number of values does not match number of columns")]
    fn test_base_container_insert_wrong_columns() {
        let mut container = BaseContainer::new(2);
        container.initialize();
        let values = vec![42];
        container.insert_record(1, values);
    }

    #[test]
    fn test_tail_container_creation() {
        let container = TailContainer::new(5);
        assert_eq!(container.num_cols, 5);
        assert_eq!(container.physical_pages.len(), 0);
    }

    #[test]
    fn test_tail_container_initialize() {
        let mut container = TailContainer::new(5);
        container.initialize();
        assert_eq!(container.physical_pages.len(), 8); // 3 reserved + 5 data columns
    }

    #[test]
    fn test_tail_container_insert() {
        let mut container = TailContainer::new(2);
        container.initialize();

        let values = vec![42, 43];
        let record = container.insert_record(1, values);

        assert_eq!(record.rid, 1);
        let addresses = record.addresses.lock().unwrap();
        assert_eq!(addresses.len(), 5); // 3 reserved + 2 data columns
    }

    #[test]
    #[should_panic(expected = "Number of values does not match number of columns")]
    fn test_tail_container_insert_wrong_columns() {
        let mut container = TailContainer::new(2);
        container.initialize();
        let values = vec![42];
        container.insert_record(1, values);
    }
}
