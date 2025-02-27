use super::page::PhysicalPage;
use super::record::{Record, RecordAddress};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::sync::{Arc, Mutex};

pub fn helper_bitwise(a: Vec<Option<i64>>) -> i64 {
    let mut schema_encoding: i64 = 0;
    for (i, val) in a.iter().enumerate() {
        if val.is_some() {
            schema_encoding |= 1 << i;
        }
    }
    schema_encoding
}

pub fn default_mask(record: &Record, metadata_only: bool) -> Vec<i64> {
    if !metadata_only {
        return vec![1; record.addresses.lock().unwrap().len()];
    }

    let mut mask = vec![0; record.addresses.lock().unwrap().len()];
    // Set the metadata columns to 1 (always read metadata)
    let num_meta_cols = if record.is_tail { 4 } else { 3 };
    for i in 0..num_meta_cols {
        mask[i] = 1;
    }

    mask
}

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct BaseContainerMetadata {
    // This takes the place of the actual pages in the disk version
    // With this number, we are able to load all of the pages
    num_pages: usize,

    // metadata
    pub num_meta_cols: usize,

    // metadata indicies
    pub rid_col: usize,
    pub schema_encoding_col: usize,
    pub indirection_col: usize,

    // number of additional columns
    pub num_cols: usize,
}

impl BaseContainerMetadata {
    pub fn load_state(&self) -> BaseContainer {
        let mut base = BaseContainer::new(self.num_cols as i64);

        for i in 0..self.num_pages {
            // Load the page
            let p = PhysicalPage::load_state(i as i64);
            // Put the page into an Arc Mutex
            let m = Arc::new(Mutex::new(p));

            // Add the physical page
            base.physical_pages.push(m);
        }

        base.num_meta_cols = self.num_meta_cols;

        base.rid_col = self.rid_col;
        base.schema_encoding_col = self.schema_encoding_col;
        base.indirection_col = self.indirection_col;

        base.num_cols = self.num_cols;

        return base;
    }
}

#[derive(Clone, Default)]
pub struct BaseContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // metadata
    pub num_meta_cols: usize,

    // metadata indicies
    pub rid_col: usize,
    pub schema_encoding_col: usize,
    pub indirection_col: usize,

    // number of additional columns
    pub num_cols: usize,
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
            num_meta_cols: 3,
            rid_col: 0,
            schema_encoding_col: 1,
            indirection_col: 2,
            num_cols: num_cols as usize,
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

    pub fn meta_page(&self, col_idx: usize) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[col_idx].clone()
    }

    pub fn column_page(&self, col_idx: usize) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[col_idx + self.num_meta_cols].clone()
    }

    pub fn insert_record(&mut self, rid: i64, values: Vec<i64>) -> Record {
        if values.len() != self.num_cols as usize {
            panic!("Number of values does not match number of columns");
        }

        let rid_page = self.meta_page(self.rid_col);
        let mut rid_page_lock = rid_page.lock().unwrap();

        rid_page_lock.write(rid);

        let schema_encoding_page = self.meta_page(self.schema_encoding_col);
        let mut schema_encoding_page_lock = schema_encoding_page.lock().unwrap();

        // initial schema encoding is 0 (no columns have been updated in base record)
        schema_encoding_page_lock.write(0);

        let indirection_page = self.meta_page(self.indirection_col);
        let mut indirection_page_lock = indirection_page.lock().unwrap();
        indirection_page_lock.write(rid);

        for i in 0..self.num_cols {
            let col_page = self.column_page(i);
            let mut col_page = col_page.lock().unwrap();
            col_page.write(values[i]);
        }

        let addresses: Arc<Mutex<Vec<RecordAddress>>> = Arc::new(Mutex::new(Vec::new()));
        let mut a = addresses.lock().unwrap();

        a.push(RecordAddress {
            page: rid_page.clone(),
            offset: rid_page_lock.num_records - 1,
        });

        a.push(RecordAddress {
            page: schema_encoding_page.clone(),
            offset: schema_encoding_page_lock.num_records - 1,
        });

        a.push(RecordAddress {
            page: indirection_page.clone(),
            offset: indirection_page_lock.num_records - 1,
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
            is_tail: false,
            addresses: addresses.clone(),
        }
    }

    pub fn update_field(&self, record: &Record, raw_col_idx: usize, value: i64) {
        let addrs_base = record.addresses.lock().unwrap();
        let mut indirection_page = addrs_base[raw_col_idx].page.lock().unwrap();
        indirection_page.overwrite(
            addrs_base[raw_col_idx].offset as usize,
            value,
        );
    }

    pub fn read_record(&self, record: Record, column_mask: Vec<i64>) -> Vec<Option<i64>> {
        let mut values = Vec::<Option<i64>>::new();

        let addrs = record.addresses.lock().unwrap();

        // Check if the column mask has the correct length
        if column_mask.len() != addrs.len() {
            panic!("Column mask length does not match number of columns");
        }

        for (i, addr) in addrs.iter().enumerate() {
            // Skip columns we don't want to read (marked with 0)
            if column_mask[i] == 0 {
                values.push(None);
            } else {
                let a = addr.page.lock().unwrap();
                match a.read(addr.offset as usize) {
                    Some(val) => values.push(Some(val)),
                    None => values.push(None),
                }
            }
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
            num_meta_cols: self.num_meta_cols,
            num_cols: self.num_cols,
            rid_col: self.rid_col,
            schema_encoding_col: self.schema_encoding_col,
            indirection_col: self.indirection_col,
        }
    }
}

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct TailContainerMetadata {
    // This takes the place of the actual pages in the disk version
    // With this number, we are able to load all of the pages
    num_pages: usize,

    // metadata
    pub num_meta_cols: usize,

    // metadata indicies
    pub rid_col: usize,
    pub schema_encoding_col: usize,
    pub indirection_col: usize,
    pub base_rid_col: usize,

    // number of additional columns
    pub num_cols: usize,
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

        tail.num_meta_cols = self.num_meta_cols;

        tail.rid_col = self.rid_col;
        tail.schema_encoding_col = self.schema_encoding_col;
        tail.indirection_col = self.indirection_col;
        tail.base_rid_col = self.base_rid_col;

        tail.num_cols = self.num_cols;

        return tail;
    }
}

#[derive(Clone, Default)]
pub struct TailContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // metadata
    pub num_meta_cols: usize,

    // metadata indicies
    pub rid_col: usize,
    pub schema_encoding_col: usize,
    pub indirection_col: usize,
    pub base_rid_col: usize,

    // number of additional columns
    pub num_cols: usize,
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
    pub fn new(num_cols: usize) -> Self {
        TailContainer {
            physical_pages: Vec::new(),
            num_meta_cols: 4,
            rid_col: 0,
            schema_encoding_col: 1,
            indirection_col: 2,
            base_rid_col: 3,
            num_cols: num_cols,
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
        let rid_page = PhysicalPage::new();
        let schema_encoding_page = PhysicalPage::new();
        let indirection_page = PhysicalPage::new();
        let base_rid_page = PhysicalPage::new();

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(indirection_page)));
        self.physical_pages.push(Arc::new(Mutex::new(base_rid_page)));

        // initialize the rest of the columns
        for _ in 0..self.num_cols {
            let new_page = PhysicalPage::new();
            self.physical_pages.push(Arc::new(Mutex::new(new_page)));
        }
    }

    pub fn meta_page(&self, col_idx: usize) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[col_idx].clone()
    }

    pub fn column_page(&self, col_idx: usize) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[col_idx + self.num_meta_cols].clone()
    }

    pub fn insert_record(
        &mut self,
        rid: i64,
        indirection_rid: i64,
        base_rid: i64,
        values: Vec<Option<i64>>,
    ) -> Record {
        if values.len() != self.num_cols {
            panic!("Number of values does not match number of columns");
        }

        let rid_page = self.meta_page(self.rid_col);
        let mut rid_page_lock = rid_page.lock().unwrap();

        rid_page_lock.write(rid);

        let schema_encoding_page = self.meta_page(self.schema_encoding_col);
        let mut schema_encoding_page_lock = schema_encoding_page.lock().unwrap();
        // Calculate schema encoding based on which columns have values
        schema_encoding_page_lock.write(helper_bitwise(values.clone()));

        let indirection_page = self.meta_page(self.indirection_col);
        let mut indirection_page_lock = indirection_page.lock().unwrap();

        indirection_page_lock.write(indirection_rid);

        let base_id_page = self.meta_page(self.base_rid_col);
        let mut base_id_page_lock = base_id_page.lock().unwrap();

        base_id_page_lock.write(base_rid);

        for i in 0..self.num_cols {
            let col_page = self.column_page(i);
            let mut col_page = col_page.lock().unwrap();
            // Unwrap the Option or use 0 as default if None (NULL)
            col_page.write(values[i].unwrap_or(0));
        }

        let addresses: Arc<Mutex<Vec<RecordAddress>>> = Arc::new(Mutex::new(Vec::new()));
        let mut a = addresses.lock().unwrap();

        a.push(RecordAddress {
            page: rid_page.clone(),
            offset: rid_page_lock.num_records - 1,
        });

        a.push(RecordAddress {
            page: schema_encoding_page.clone(),
            offset: schema_encoding_page_lock.num_records - 1,
        });

        a.push(RecordAddress {
            page: indirection_page.clone(),
            offset: indirection_page_lock.num_records - 1,
        });

        a.push(RecordAddress {
            page: base_id_page.clone(),
            offset: base_id_page_lock.num_records - 1,
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
            is_tail: true,
            addresses: addresses.clone(),
        }
    }

    pub fn read_record(&self, record: Record, column_mask: Vec<i64>) -> Vec<Option<i64>> {
        let mut values = Vec::<Option<i64>>::new();

        let addrs = record.addresses.lock().unwrap();

        // Check if the column mask has the correct length
        if column_mask.len() != addrs.len() {
            panic!("Column mask length does not match number of columns");
        }

        for (i, addr) in addrs.iter().enumerate() {
            // Skip columns we don't want to read (marked with 0)
            if column_mask[i] == 0 {
                values.push(None);
            } else {
                let a = addr.page.lock().unwrap();
                match a.read(addr.offset as usize) {
                    Some(val) => values.push(Some(val)),
                    None => values.push(None),
                }
            }
        }

        values
    }

    pub fn save_state(&self) {
        let tail_meta = self.get_metadata();
        let hardcoded_filename = "./redoxdata/tail_container.data";

        let mut index = 0;
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
            num_meta_cols: self.num_meta_cols,
            rid_col: self.rid_col,
            schema_encoding_col: self.schema_encoding_col,
            indirection_col: self.indirection_col,
            base_rid_col: self.base_rid_col,
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
        assert_eq!(container.physical_pages.len(), container.num_cols+container.num_meta_cols);
    }

    #[test]
    fn test_tail_container_insert() {
        let num_cols = 2;
        let mut container = TailContainer::new(num_cols);
        container.initialize();

        let values = vec![Some(42), Some(43)];
        // TODO: ensure the indirection RID is set correctly (needs a base record)
        let record = container.insert_record(1, 0, 0, values);

        assert_eq!(record.rid, 1);
        let addresses = record.addresses.lock().unwrap();
        assert_eq!(addresses.len(), 4 + num_cols);
    }

    #[test]
    #[should_panic(expected = "Number of values does not match number of columns")]
    fn test_tail_container_insert_wrong_columns() {
        let mut container = TailContainer::new(2);
        container.initialize();
        let values = vec![Some(42)];
        // TODO: ensure the indirection RID is set correctly (needs a base record)
        container.insert_record(1, 0, 0, values);
    }
}
