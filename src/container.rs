use super::page::PhysicalPage;
use super::record::{Record, RecordAddress};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct BaseContainerMetadata {
    // This takes the place of the actual pages in the disk version
    // With this number, we are able to load all of the pages
    num_pages: usize,

    tail_page_sequence: i64,

    num_cols: i64
}

pub enum ReservedColumns {
    RID,
    SchemaEncoding,
    Indirection,
    BaseRID,
}

pub static NUM_RESERVED_COLUMNS: i64 = 4;

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

        return base;
    }
}

#[derive(Clone, Default)]
pub struct BaseContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // tail-page sequence number
    pub tail_page_sequence: i64,

    // number of additional columns
    pub num_cols: i64,
}


impl BaseContainer {

    pub fn new(num_cols: i64) -> Self {
        BaseContainer {
            physical_pages: Vec::new(),
            tail_page_sequence: 0,
            num_cols
        }
    }


    pub fn initialize(&mut self) {
        // initialize reserved columns
        let rid_page = PhysicalPage::new(ReservedColumns::RID as i64);
        let schema_encoding_page = PhysicalPage::new(ReservedColumns::SchemaEncoding as i64);
        let indirection_page = PhysicalPage::new(ReservedColumns::Indirection as i64);
        let base_rid_page = PhysicalPage::new(ReservedColumns::BaseRID as i64);

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(indirection_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(base_rid_page)));

        // initialize the rest of the columns
        for i in 0..self.num_cols {
            let new_page = PhysicalPage::new(NUM_RESERVED_COLUMNS + i);
            self.physical_pages.push(Arc::new(Mutex::new(new_page)));
        }
    }

    /// Returns a reference to the RID column page
    pub fn rid_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[ReservedColumns::RID as usize].clone()
    }

    /// Returns a reference to the schema encoding column page
    pub fn schema_encoding_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[ReservedColumns::SchemaEncoding as usize].clone()
    }

    /// Returns a reference to the indirection column page
    pub fn indirection_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[ReservedColumns::Indirection as usize].clone()
    }

    /// Returns a reference to the base RID column page
    pub fn base_rid_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[ReservedColumns::BaseRID as usize].clone()
    }

    /// Returns a reference to the specified column page
    ///
    /// ### Arguments
    ///
    /// - `col_idx`: The index of the column
    pub fn column_page(&self, col_idx: i64) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[(col_idx + NUM_RESERVED_COLUMNS) as usize].clone()
    }

    pub fn deep_copy(&self) -> BaseContainer {
        let mut base = BaseContainer::new(self.num_cols);

        for i in 0..self.physical_pages.len() {
            let p = self.physical_pages[i].lock().unwrap();
            let new_page = p.clone();
            let m = Arc::new(Mutex::new(new_page));
            base.physical_pages.push(m);
        }

        base
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

        let base_rid_page = self.base_rid_page();
        let mut brp = base_rid_page.lock().unwrap();
        brp.write(rid);

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

        a.push(RecordAddress {
            page: base_rid_page.clone(),
            offset: brp.num_records - 1,
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
            addresses: addresses.clone()
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

    pub fn find_rid_offset(&mut self, rid: i64) -> usize {
        let mut offset: i64 = -1;

        for i in 0..self.rid_page().lock().unwrap().data.len() {
            if self.rid_page().lock().unwrap().data[i] == rid {
                offset = i as i64;
                break;
            }
        }

        if offset == -1 {
            panic!("Could not find RID in RID page");
        }

        offset as usize
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
            tail_page_sequence: self.tail_page_sequence,
            num_cols: self.num_cols
        }
    }
}

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct TailContainerMetadata {
    // This takes the place of the actual pages in the disk version
    // With this number, we are able to load all of the pages
    num_pages: usize,

    num_cols: i64
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

        return tail;
    }
}

#[derive(Clone, Default)]
pub struct TailContainer {
    // pages
    pub physical_pages: Vec<Arc<Mutex<PhysicalPage>>>,

    // number of additional columns
    pub num_cols: i64,
}

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
            num_cols     
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
        let rid_page = PhysicalPage::new(ReservedColumns::RID as i64);
        let schema_encoding_page = PhysicalPage::new(ReservedColumns::SchemaEncoding as i64);
        let indirection_page = PhysicalPage::new(ReservedColumns::Indirection as i64);
        let base_rid_page = PhysicalPage::new(ReservedColumns::BaseRID as i64);

        self.physical_pages.push(Arc::new(Mutex::new(rid_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(schema_encoding_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(indirection_page)));
        self.physical_pages
            .push(Arc::new(Mutex::new(base_rid_page)));

        // initialize the rest of the columns
        for i in 0..self.num_cols {
            let new_page = PhysicalPage::new(NUM_RESERVED_COLUMNS + i);
            self.physical_pages.push(Arc::new(Mutex::new(new_page)));
        }
    }

    /// Returns a reference to the RID column page
    pub fn rid_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[ReservedColumns::RID as usize].clone()
    }

    /// Returns a reference to the schema encoding column page
    pub fn schema_encoding_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[ReservedColumns::SchemaEncoding as usize].clone()
    }

    /// Returns a reference to the indirection column page
    pub fn indirection_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[ReservedColumns::Indirection as usize].clone()
    }

    // Returns a reference to the base RID column page
    pub fn base_rid_page(&self) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[ReservedColumns::BaseRID as usize].clone()
    }

    /// Returns a reference to the specified column page
    pub fn column_page(&self, col_idx: i64) -> Arc<Mutex<PhysicalPage>> {
        self.physical_pages[(col_idx + NUM_RESERVED_COLUMNS) as usize].clone()
    }

    pub fn insert_record(&mut self, rid: i64, indirection_rid: i64, base_rid: i64, values: Vec<i64>) -> Record {
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

        let base_rid_page = self.base_rid_page();
        let mut brp = base_rid_page.lock().unwrap();
        brp.write(base_rid as i64);

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

        a.push(RecordAddress {
            page: base_rid_page.clone(),
            offset: brp.num_records - 1,
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
            addresses: addresses.clone()
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
            num_cols: self.num_cols
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Arc, Mutex};

    // Note: These tests assume that the implementations of PhysicalPage,
    // Record, RecordAddress, and RecordType behave as expected (e.g.:
    // PhysicalPage::new() creates a page with num_records starting at 0,
    // write(x) writes x and increments num_records, and read(offset) returns x).

    // -------------------------
    // Tests for BaseContainer
    // -------------------------
    #[test]
    fn test_base_container_initialize() {
        let num_cols = 5;
        let mut base = BaseContainer::new(num_cols);
        base.initialize();

        // In BaseContainer::initialize we push four reserved pages plus one page per additional column.
        let expected_pages = 4 + num_cols as usize;
        assert_eq!(
            base.physical_pages.len(),
            expected_pages,
            "Unexpected number of pages after initialization"
        );

        // Check that the reserved page methods return the expected pages.
        // (Note: ReservedColumns variants use default discriminants (0,1,2,3).)
        assert!(Arc::ptr_eq(&base.rid_page(), &base.physical_pages[0]));
        assert!(Arc::ptr_eq(&base.schema_encoding_page(), &base.physical_pages[1]));
        assert!(Arc::ptr_eq(&base.indirection_page(), &base.physical_pages[2]));
        assert!(Arc::ptr_eq(&base.base_rid_page(), &base.physical_pages[3]));

        assert!(Arc::ptr_eq(&base.column_page(0), &base.physical_pages[NUM_RESERVED_COLUMNS as usize]),
            "Base container column page 0 should be at index 4");
    }

    #[test]
    fn test_base_container_insert_and_read() {
        let num_cols = 3;
        let mut base = BaseContainer::new(num_cols);
        base.initialize();

        let rid = 42;
        let values = vec![10, 20, 30];
        let record = base.insert_record(rid, values.clone());

        // According to insert_record:
        // - The RID column gets 'rid'
        // - The schema encoding page gets 0
        // - The indirection and base RID pages get 'rid'
        // - The additional column pages get the values provided.
        let mut expected = Vec::new();
        expected.push(rid); // RID page
        expected.push(0);   // schema encoding page
        expected.push(rid); // indirection page
        expected.push(rid); // base RID page
        expected.extend(values);

        let read_values = base.read_record(record);
        assert_eq!(read_values, expected, "Read values do not match inserted values");
    }

    #[test]
    #[should_panic(expected = "Number of values does not match number of columns")]
    fn test_base_container_insert_record_panic_on_wrong_columns() {
        let num_cols = 3;
        let mut base = BaseContainer::new(num_cols);
        base.initialize();
        // Provide an incorrect number of values to trigger the panic.
        let wrong_values = vec![1, 2]; 
        let _ = base.insert_record(1, wrong_values);
    }

    #[test]
    fn test_base_container_get_metadata() {
        let num_cols = 4;
        let mut base = BaseContainer::new(num_cols);
        base.initialize();
        base.tail_page_sequence = 99;

        let meta = base.get_metadata();
        assert_eq!(meta.num_pages, base.physical_pages.len());
        assert_eq!(meta.tail_page_sequence, 99);
        assert_eq!(meta.num_cols, num_cols);
    }

    // #[test]
    // fn test_base_container_save_state() {
    //     // Use a temporary directory so as not to interfere with real data.
    //     let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    //     let file_path = temp_dir.path().join("base_container.data");

    //     // Temporarily change current directory to the temp directory.
    //     let original_dir = std::env::current_dir().expect("Failed to get current directory");
    //     std::env::set_current_dir(temp_dir.path()).expect("Failed to change directory");

    //     let num_cols = 2;
    //     let mut base = BaseContainer::new(num_cols);
    //     base.initialize();
    //     // Insert a record to ensure pages have data.
    //     let _ = base.insert_record(1, vec![100, 200]);
    //     base.save_state();

    //     // Verify that the state file exists and is non-empty.
    //     assert!(file_path.exists(), "State file was not created");
    //     let metadata = fs::metadata(&file_path).expect("Failed to get file metadata");
    //     assert!(metadata.len() > 0, "State file is empty");

    //     // Restore original directory.
    //     std::env::set_current_dir(original_dir).expect("Failed to restore original directory");
    // }

    // -------------------------
    // Tests for TailContainer
    // -------------------------
    #[test]
    fn test_tail_container_initialize() {
        let num_cols = 4;
        let mut tail = TailContainer::new(num_cols);
        tail.initialize();

        // For TailContainer, four reserved pages plus one per additional column.
        let expected_pages = 4 + num_cols as usize;
        assert_eq!(
            tail.physical_pages.len(),
            expected_pages,
            "Unexpected number of pages in tail container after initialization"
        );

        // Verify reserved pages.
        assert!(Arc::ptr_eq(&tail.rid_page(), &tail.physical_pages[0]));
        assert!(Arc::ptr_eq(&tail.schema_encoding_page(), &tail.physical_pages[1]));
        assert!(Arc::ptr_eq(&tail.indirection_page(), &tail.physical_pages[2]));
        assert!(Arc::ptr_eq(&tail.base_rid_page(), &tail.physical_pages[3]));

        // For TailContainer, column_page(0) should return physical_pages[4]
        assert!(Arc::ptr_eq(&tail.column_page(0), &tail.physical_pages[4]),
            "Tail container column page 0 should be at index 4");
    }

    #[test]
    fn test_tail_container_insert_and_read() {
        let num_cols = 2;
        let mut tail = TailContainer::new(num_cols);
        tail.initialize();

        let rid = 5;
        let indirection_rid = 50;
        let base_rid = 500;
        let values = vec![7, 8];
        let record = tail.insert_record(rid, indirection_rid, base_rid, values.clone());

        // Expected order of written values:
        // Reserved pages: RID page gets rid, schema encoding gets 0, indirection gets indirection_rid,
        // base RID gets base_rid, then additional columns get the provided values.
        let mut expected = Vec::new();
        expected.push(rid);
        expected.push(0);
        expected.push(indirection_rid);
        expected.push(base_rid);
        expected.extend(values);

        let read_values = tail.read_record(record);
        assert_eq!(read_values, expected, "Tail container read values do not match inserted values");
    }

    #[test]
    #[should_panic(expected = "Number of values does not match number of columns")]
    fn test_tail_container_insert_record_panic_on_wrong_columns() {
        let num_cols = 2;
        let mut tail = TailContainer::new(num_cols);
        tail.initialize();
        // Pass in a vector with fewer values than expected.
        let wrong_values = vec![10];
        let _ = tail.insert_record(1, 2, 3, wrong_values);
    }

    #[test]
    fn test_tail_container_get_metadata() {
        let num_cols = 3;
        let mut tail = TailContainer::new(num_cols);
        tail.initialize();

        let meta = tail.get_metadata();
        assert_eq!(meta.num_pages, tail.physical_pages.len());
        assert_eq!(meta.num_cols, num_cols);
    }

    // #[test]
    // fn test_tail_container_save_state() {
    //     let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    //     let file_path = temp_dir.path().join("tail_container.data");

    //     let original_dir = std::env::current_dir().expect("Failed to get current directory");
    //     std::env::set_current_dir(temp_dir.path()).expect("Failed to change directory");

    //     let num_cols = 1;
    //     let mut tail = TailContainer::new(num_cols);
    //     tail.initialize();
    //     let _ = tail.insert_record(2, 20, 200, vec![300]);
    //     tail.save_state();

    //     assert!(file_path.exists(), "Tail state file was not created");
    //     let metadata = fs::metadata(&file_path).expect("Failed to get file metadata");
    //     assert!(metadata.len() > 0, "Tail state file is empty");

    //     std::env::set_current_dir(original_dir).expect("Failed to restore original directory");
    // }
}

