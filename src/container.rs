use super::page::PhysicalPage;
use super::record::{Record, RecordAddress};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct BaseContainerMetadata {
    // This takes the place of the actual pages in the disk version
    // With this number, we are able to load all of the pages
    num_pages: usize,

    num_cols: i64,

    rid_column: i64,
    schema_encoding_column: i64,
    indirection_column: i64,
}

impl BaseContainerMetadata {
    pub fn load_state(&self) -> BaseContainer {
        let mut base = BaseContainer::new(self.num_cols);

        for i in 0..self.num_pages {
            // Load the page
            let p = PhysicalPage::load_state(i as i64);
            // Put the page into an Arc Mutex
            let m = Arc::new(Mutex::new(p));

            // Add the physical page
            base.physical_pages.push(m);
        }

        base.rid_column = self.rid_column;
        base.schema_encoding_column = self.schema_encoding_column;
        base.indirection_column = self.indirection_column;

        return base;
    }
}

#[derive(Clone, Default)]
pub struct BaseContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // number of additional columns
    pub num_cols: i64,

    // reserved columns
    pub rid_column: i64,
    pub schema_encoding_column: i64,
    pub indirection_column: i64,
}

/// A container that manages physical pages for storing data in columns
///
/// The `BaseContainer` maintains a collection of physical pages where each page represents
/// a column of data. It reserves the first three columns for special purposes:
///
/// - rid_column (0): Record IDs
/// - schema_encoding_column (1): Schema encoding information
/// - indirection_column (2): Indirection records
///
/// # Fields
///
/// - `physical_pages`: A vector of physical pages
/// - `num_cols`: The number of additional columns
/// - `rid_column`: The index of the RID column
/// - `schema_encoding_column`: The index of the schema encoding column
/// - `indirection_column`: The index of the indirection column
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
    pub fn new(num_cols: i64) -> Self {
        BaseContainer {
            physical_pages: Vec::new(),
            num_cols,
            rid_column: 0,
            schema_encoding_column: 1,
            indirection_column: 2,
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
    /// use redoxql::container::BaseContainer;
    ///
    /// let mut container = BaseContainer::new(5);
    /// container.initialize();
    /// ```
    ///
    pub fn initialize(&mut self) {
        // initialize the three reserved columns
        let rid_page = PhysicalPage::new(0);
        let schema_encoding_page = PhysicalPage::new(1);
        let indirection_page = PhysicalPage::new(2);

        // Currently we have 3 internal columns, but we are soon to have 4
        let number_of_internal_columns = 3;

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(indirection_page)));

        // initialize the rest of the columns
        for i in 0..self.num_cols {
            let new_page = PhysicalPage::new(number_of_internal_columns + i);
            self.physical_pages.push(Arc::new(Mutex::new(new_page)));
        }
    }

    /// Returns a reference to the RID column page
    pub fn rid_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.rid_column as usize].clone()
    }

    /// Returns a reference to the schema encoding column page
    pub fn schema_encoding_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.schema_encoding_column as usize].clone()
    }

    /// Returns a reference to the indirection column page
    pub fn indirection_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.indirection_column as usize].clone()
    }

    /// Returns a reference to the specified column page
    ///
    /// ### Arguments
    ///
    /// - `col_idx`: The index of the column
    pub fn column_page(&self, col_idx: i64) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[(col_idx + 3) as usize].clone()
    }

    pub fn insert_record(&mut self, rid: i64, values: Vec<i64>) -> Record {
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

    pub fn read_record(&self, record: Record) -> Vec<i64> {
        let mut values = Vec::<i64>::new();

        let addrs = record.addresses.lock().unwrap();
        let addrs_clone = addrs.clone();
        for addr in addrs_clone {
            let a = addr.page.lock().unwrap();
            let b = a.read(addr.offset as usize);
            values.push(b.expect("Value should be there"));
        }

        values
    }

    pub fn save_state(&self) {
        let base_meta = self.get_metadata();
        let hardcoded_filename = "./redoxdata/base_container.data";

        let mut index = 0;
        // The Rust compiler suggested that I clone here but it's definitely way better to not copy
        // all of the data and just use a reference
        for p in &self.physical_pages {
            // Save the page
            let m = p.lock().unwrap();
            m.save_state(index);
            index += 1;
        }

        let base_bytes: Vec<u8> = bincode::serialize(&base_meta).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&base_bytes).expect("Should serialize.");
    }

    pub fn get_metadata(&self) -> BaseContainerMetadata {
        BaseContainerMetadata {
            num_pages: self.physical_pages.len(),
            num_cols: self.num_cols,
            rid_column: self.rid_column,
            schema_encoding_column: self.schema_encoding_column,
            indirection_column: self.indirection_column,
        }
    }
}

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct TailContainerMetadata {
    // This takes the place of the actual pages in the disk version
    // With this number, we are able to load all of the pages
    num_pages: usize,

    num_cols: i64,

    rid_column: i64,
    schema_encoding_column: i64,
    indirection_column: i64,
}

impl TailContainerMetadata {
    pub fn load_state(&self) -> TailContainer {
        let mut tail = TailContainer::new(self.num_cols);

        for i in 0..self.num_pages {
            // Load the page
            let p = PhysicalPage::load_state(i as i64);
            // Put the page into an Arc Mutex
            let m = Arc::new(Mutex::new(p));

            // Add the physical page
            tail.physical_pages.push(m);
        }

        tail.rid_column = self.rid_column;
        tail.schema_encoding_column = self.schema_encoding_column;
        tail.indirection_column = self.indirection_column;

        return tail;
    }
}

#[derive(Clone, Default)]
pub struct TailContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // number of additional columns
    pub num_cols: i64,

    // reserved columns
    pub rid_column: i64,
    pub schema_encoding_column: i64,
    pub indirection_column: i64,
}

/// A container that manages physical pages for storing data in columns
///
/// The `TailContainer` maintains a collection of physical pages where each page represents
/// a column of data. It reserves the first three columns for special purposes:
///
/// - rid_column (0): Record IDs
/// - schema_encoding_column (1): Schema encoding information
/// - indirection_column (2): Indirection records
///
/// # Fields
///
/// - `physical_pages`: A vector of physical pages
/// - `num_cols`: The number of additional columns
/// - `rid_column`: The index of the RID column
/// - `schema_encoding_column`: The index of the schema encoding column
/// - `indirection_column`: The index of the indirection column
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
    pub fn new(num_cols: i64) -> Self {
        TailContainer {
            physical_pages: Vec::new(),
            num_cols,
            rid_column: 0,
            schema_encoding_column: 1,
            indirection_column: 2,
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
    /// use redoxql::container::TailContainer;
    ///
    /// let mut container = TailContainer::new(5);
    /// container.initialize();
    /// ```
    pub fn initialize(&mut self) {
        // initialize the three reserved columns
        let rid_page = PhysicalPage::new(0);
        let schema_encoding_page = PhysicalPage::new(1);
        let indirection_page = PhysicalPage::new(2);

        // Currently we have 3 internal columns, but we are soon to have 4
        let number_of_internal_columns = 3;

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(indirection_page)));

        // initialize the rest of the columns
        for i in 0..self.num_cols {
            let new_page = PhysicalPage::new(number_of_internal_columns + i);
            self.physical_pages.push(Arc::new(Mutex::new(new_page)));
        }
    }

    /// Returns a reference to the RID column page
    pub fn rid_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.rid_column as usize].clone()
    }

    /// Returns a reference to the schema encoding column page
    pub fn schema_encoding_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.schema_encoding_column as usize].clone()
    }

    /// Returns a reference to the indirection column page
    pub fn indirection_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[self.indirection_column as usize].clone()
    }

    /// Returns a reference to the specified column page
    pub fn column_page(&self, col_idx: i64) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[(col_idx + 3) as usize].clone()
    }

    pub fn insert_record(&mut self, rid: i64, indirection_rid: i64, values: Vec<i64>) -> Record {
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

        ip.write(indirection_rid as i64);

        for i in 0..self.num_cols {
            let col_page = self.column_page(i);
            let mut col_page = col_page.lock().unwrap();
            col_page.write(values[i as usize] as i64);
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

    pub fn read_record(&self, record: Record) -> Vec<i64> {
        let mut values = Vec::<i64>::new();

        let addrs = record.addresses.lock().unwrap();
        let addrs_clone = addrs.clone();
        for addr in addrs_clone {
            let a = addr.page.lock().unwrap();
            let b = a.read(addr.offset as usize);
            values.push(b.expect("Value should be there"));
        }

        values
    }

    pub fn save_state(&self) {
        let tail_meta = self.get_metadata();
        let hardcoded_filename = "./redoxdata/tail_container.data";

        let mut index = self.physical_pages.len() as i64;
        // The Rust compiler suggested that I clone here but it's definitely way better to not copy
        // all of the data and just use a reference
        for p in &self.physical_pages {
            // Save the page
            let m = p.lock().unwrap();
            m.save_state(index);
            index += 1;
        }

        let tail_bytes: Vec<u8> = bincode::serialize(&tail_meta).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&tail_bytes).expect("Should serialize.");
    }

    pub fn get_metadata(&self) -> TailContainerMetadata {
        TailContainerMetadata {
            num_pages: self.physical_pages.len(),
            num_cols: self.num_cols,
            rid_column: self.rid_column,
            schema_encoding_column: self.schema_encoding_column,
            indirection_column: self.indirection_column,
        }
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

        for _ in 0..513 {
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
        // TODO: ensure the indirection RID is set correctly (needs a base record)
        let record = container.insert_record(1, 0, values);

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
        // TODO: ensure the indirection RID is set correctly (needs a base record)
        container.insert_record(1, 0, values);
    }
}
