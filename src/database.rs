use super::container::{BaseContainer, TailContainer};
use super::page::PhysicalPage;
use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[pyclass]
#[derive(Clone)]
pub struct RIndex {
    /// Map a primary_key to a RID
    /// RIDs are used internally and are auto incremented
    /// The primary_key are given to the Python Query by the user of the library
    index: BTreeMap<i64, i64>,
}

impl RIndex {
    pub fn new() -> RIndex {
        RIndex {
            index: BTreeMap::new(),
        }
    }

    /// Create a mapping from primary_key to RID
    pub fn add(&mut self, primary_key: i64, rid: i64) {
        self.index.insert(primary_key, rid);
    }

    /// Return the RID that we get from the primary_key
    pub fn get(&self, primary_key: i64) -> Option<&i64> {
        self.index.get(&primary_key)
    }
}

#[derive(Clone)]
pub struct PageRange {
    pub base_container: BaseContainer,
    pub tail_container: TailContainer,
}

impl PageRange {
    fn new(num_cols: i64) -> Self {
        let mut base = BaseContainer::new(num_cols);
        base.initialize();

        let mut tail = TailContainer::new(num_cols);
        tail.initialize();

        PageRange {
            base_container: base,
            tail_container: tail,
        }
    }

    /// Write an entire record of values
    fn write(&mut self, new_rid: i64, values: Vec<i64>) -> Record {
        self.base_container.insert_record(new_rid, values)
    }

    pub fn read(&self, record: Record) -> Option<Vec<i64>> {
        Some(self.base_container.read_record(record))
    }
}

#[derive(Debug, Clone)]
pub struct RecordAddress {
    pub page: Arc<Mutex<PhysicalPage>>,
    pub offset: i64,
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
}

#[derive(Clone)]
#[pyclass]
pub struct RTable {
    /// The name given in RDatabase.create_table
    pub name: String,

    /// The column that will act as the primary_key
    pub primary_key_column: usize,

    pub page_range: PageRange,

    // Map RIDs to Records
    pub page_directory: HashMap<i64, Record>,

    /// This is how we created the RID
    /// We use this value directly as the RID and increment after ever insert
    pub num_records: i64,

    /// This is the count of columns in the RTable
    #[pyo3(get)]
    pub num_columns: usize,

    /// This is how we map the given primary_key to the internal RID
    pub index: RIndex,
}

impl RTable {
    pub fn write(&mut self, values: Vec<i64>) -> Record {
        // Use the primary_key_column'th value as the given key
        let primary_key = values[self.primary_key_column];

        let rid = self.num_records;
        self.index.add(primary_key, rid);

        let rec = self.page_range.write(rid, values);

        // Save the RID -> Record so it can later be read
        self.page_directory.insert(rid, rec.clone());

        self.num_records += 1;
        return rec;
    }

    pub fn read_base(&self, primary_key: i64) -> Option<Vec<i64>> {
        // Lookup RID from primary_key
        let rid = self.index.get(primary_key);

        if let Some(r) = rid {
            let rec = self.page_directory.get(&r);

            // If the rec exists in the page_directory, return the read values
            match rec {
                Some(r) => return self.page_range.read(r.clone()),
                None => return None,
            }
        }

        None
    }

    pub fn read(&self, primary_key: i64) -> Option<Vec<i64>> {
        let Some(result) = self.read_base(primary_key as i64) else {
            return None;
        };
        let base_rid = result[self.page_range.base_container.rid_column as usize];
        let base_indirection_column =
            result[self.page_range.base_container.indirection_column as usize];

        if base_rid == base_indirection_column {
            return Some(result);
        }

        let Some(tail_record) = self.page_directory.get(&base_indirection_column) else {
            return None;
        };

        return self.page_range.read(tail_record.clone());
    }

    pub fn read_relative(&self, primary_key: i64, relative_version: i64) -> Option<Vec<i64>> {
        let Some(base) = self.read_base(primary_key as i64) else {
            return None;
        };
        let base_rid = base[self.page_range.base_container.rid_column as usize];
        let base_indirection_column = base[self.page_range.base_container.indirection_column as usize];
        if base_rid == base_indirection_column {
            return Some(base);
        }

        let mut current_rid = base_indirection_column;
        let mut versions_back = 0;
        let target_version = relative_version.abs() as i64; 

        while versions_back < target_version {
            let Some(current_record) = self.page_directory.get(&current_rid) else {
                return None;
            };

            // read the current record
            let Some(record_data) = self.page_range.read(current_record.clone()) else {
                return None;
            };

            // get the indirection of the previous version
            let prev_indirection: i64 =
                record_data[self.page_range.tail_container.indirection_column as usize];

            // if we've reached the base record, stop here
            if prev_indirection == base_rid {
                current_rid = base_rid;
                break;
            }

            current_rid = prev_indirection;
            versions_back += 1;
        }

        // read the final record we want
        let Some(final_record) = self.page_directory.get(&current_rid) else {
            return None;
        };

        return self.page_range.read(final_record.clone());
    }

    pub fn delete(&mut self, primary_key: i64) {
        // Lookup RID from primary_key
        let rid = self.index.get(primary_key);

        if let Some(r) = rid {
            self.page_directory.remove(&r);
        }
    }

    pub fn sum(&mut self, start_primary_key: i64, end_primary_key: i64, col_index: i64) -> i64 {
        let mut agg = 0i64;

        for primary_key in start_primary_key..end_primary_key + 1 {
            if let Some(v) = self.read(primary_key) {
                agg += v[(col_index + 3) as usize] as i64;
            }
        }

        return agg;
    }

    pub fn sum_version(
        &mut self,
        start_primary_key: i64,
        end_primary_key: i64,
        col_index: i64,
        relative_version: i64,
    ) -> i64 {
        let mut agg = 0i64;

        for primary_key in start_primary_key..end_primary_key + 1 {
            if let Some(v) = self.read_relative(primary_key, relative_version) {
                agg += v[(col_index + 3) as usize] as i64;
            }
        }

        return agg;
    }

    fn _merge() {
        unreachable!("Not used in milestone 1")
    }
}

#[pyclass]
pub struct RDatabase {
    /// This is where we keep all of the tables
    tables: Vec<RTable>,
    // Map table names to index on the tables: Vec<RTable>
    tables_hashmap: HashMap<String, usize>,
}

#[pymethods]
impl RDatabase {
    #[new]
    pub fn new() -> Self {
        RDatabase {
            tables: vec![],
            tables_hashmap: HashMap::new(),
        }
    }

    fn open(&self, _path: String) {
        unreachable!("Not used in milestone 1");
    }

    fn close(&self) {
        unreachable!("Not used in milestone 1");
    }

    pub fn create_table(
        &mut self,
        name: String,
        num_columns: i64,
        primary_key_column: i64,
    ) -> RTable {
        let t = RTable {
            name: name.clone(),
            page_range: PageRange::new(num_columns as i64),
            primary_key_column: primary_key_column as usize,
            page_directory: HashMap::new(),
            num_columns: num_columns as usize,
            num_records: 0,
            index: RIndex::new(),
        };

        let i = self.tables.len();

        // Map a name of a table to it's index on the self.tables field
        self.tables_hashmap.insert(name, i);

        self.tables.push(t);

        // Should it really be cloning here?
        // I guess since it has just an Arc Mutex, the underlying data should persist
        return self.tables[i].clone();
    }

    fn get_table(&self, name: String) -> RTable {
        let i = self.tables_hashmap.get(&name).expect("Should exist");
        // Should it really be cloning here?
        return self.tables[*i].clone();
    }

    fn get_table_from_index(&self, i: i64) -> RTable {
        return self.tables[i as usize].clone();
    }

    fn drop_table(&mut self, name: String) {
        let i_ref = self.tables_hashmap.get(&name).expect("Should exist");
        let i = *i_ref;

        // Remove from tables vec
        self.tables.remove(i);

        // c0, c1, c2, c3, c4
        // .remove(2)
        // c0, c1, c3, c4

        // Decrement id
        // c0, c1, c2, c3
        for (_, id) in self.tables_hashmap.iter_mut() {
            if *id > i {
                *id -= 1;
            }
        }

        // Remove from tables hashmap
        self.tables_hashmap.remove(&name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_table_test() {
        let mut db = RDatabase::new();

        // Create a table "users"
        db.create_table(String::from("users"), 1, 0);

        assert_eq!(db.tables.len(), 1);

        db.drop_table("users".to_string());

        assert_eq!(db.tables.len(), 0);
    }

    #[test]
    fn drop_on_of_many_tables_test() {
        let mut db = RDatabase::new();

        db.create_table(String::from("users"), 1, 0);
        db.create_table(String::from("accounts"), 2, 0);
        db.create_table(String::from("bikes"), 4, 0);

        assert_eq!(db.tables.len(), 3);

        db.drop_table("users".to_string());

        assert_eq!(db.tables.len(), 2);
    }

    #[test]
    fn index_test() {
        let mut index = RIndex::new();

        index.add(1, 100);

        assert_eq!(index.get(1).unwrap(), &100);

        index.add(10, 1010101);

        assert_eq!(index.get(10).unwrap(), &1010101);
    }
}
