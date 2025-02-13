use super::container::{BaseContainer, TailContainer};
use super::page::PhysicalPage;
use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/*  A data strucutre holding indices for various columns of a table.
Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well. */

#[pyclass]
#[derive(Clone)]
pub struct RIndex {
    /*  EXPLANATION OF BTree, NOT TOO SURE ABOUT THIS.

       A vector of BTreeMaps, can be either Some::BTreeMap or None as its elements.

       BTreeMap<i64, Vec<[usize; 3]>>:
       -- BTreeMap: A balanced binary search tree (B-Tree), for maintaining sorted key-value pairs. --

        Map primary key to RID
    */
    indices: Vec<Option<BTreeMap<u64, Vec<u64>>>>,
}

impl RIndex {
    // Mandatory: One index for each table. All our empty initially.
    pub fn new(primary_key_column: u64, num_columns: usize) -> RIndex {
        let mut indices = vec![None; num_columns]; // table.columns.len()
        indices[primary_key_column as usize] = Some(BTreeMap::new());
        RIndex { indices }
    }

    /// Returns the location of all records with the given value on column "column"
    pub fn locate(&self, column: usize, value: u64) -> Option<&Vec<u64>> {
        if let Some(tree) = &self.indices[column] {
            return tree.get(&value);
        }
        None
    }

    /// Returns the RIDs of all records with values in column "column" between "begin" and "end"
    pub fn locate_range(&self, begin: u64, end: u64, column: usize) -> Vec<u64> {
        if let Some(tree) = &self.indices[column] {
            // Gets all entries where the key is between begin and end
            let keys = tree.range(begin..=end);

            let all_records: Vec<u64> = keys.flat_map(|(_, rids)| rids.clone()).collect();
            return all_records;
        }
        Vec::new()
    }

    /// Create index on specific column
    pub fn create_index(&mut self, column: usize) {
        // Create BTree for column
        if self.indices[column].is_none() {
            self.indices[column] = Some(BTreeMap::new());
            // Populate new index with existing records

            // let table = self.table.lock().unwrap();
            // for rid in table.page_directory.keys() {
            //     let row = table.fetch_row(*rid);
            //     let value = row[column];

            //     // Add RID to index
            //     self.update_index(value, [*rid as usize, column, 0], column).unwrap();
            // }
        }
    }

    /// Insert or update index for a specific column
    pub fn update_index(&mut self, key: u64, rid: u64, column: usize) -> Result<(), String> {
        if column >= self.indices.len() {
            return Err(format!("Column {} does not exist'", column));
        }
        // Gets column Some::BTreeMap, creates one if None
        let tree = self.indices[column].get_or_insert_with(BTreeMap::new);

        // Insert or update key
        // Searches for the given key in the BTree, If the key exists, it returns a mutable reference to the corresponding value,
        // If the key does not exist, it creates a new entry in the BTree for the key, If the key does not exist, this initializes an empty vector (Vec::new) as the value for the key.
        // Appends the provided RID to the vector associated with the key
        tree.entry(key).or_insert_with(Vec::new).push(rid);
        Ok(())
    }

    pub fn delete_from_index(&mut self, column: usize, key: u64, rid: u64) {
        if let Some(tree) = &mut self.indices[column] {
            if let Some(rids) = tree.get_mut(&key) {
                // Find the position of the RID to remove
                if let Some(pos) = rids.iter().position(|&p| p == rid) {
                    rids.remove(pos);
                }
                // If no more RID's exist for this key, remove the key
                if rids.is_empty() {
                    tree.remove(&key);
                }
            }
        }
    }

    /// Drop index of specific column
    pub fn drop_index(&mut self, column: usize) {
        self.indices[column] = None;
    }
}

#[derive(Clone)]
pub struct PageRange {
    base_container: BaseContainer,
    tail_container: TailContainer,
}

impl PageRange {
    fn new(num_cols: u64) -> Self {
        let mut base = BaseContainer::new(num_cols);
        base.initialize();

        let mut tail = TailContainer::new(num_cols);
        tail.initialize();

        PageRange {
            base_container: base,
            tail_container: tail,
        }
    }

    fn write(&mut self, new_rid: u64, values: Vec<u64>) -> Record {
        self.base_container.insert_record(new_rid, values)
    }

    fn read(&self, record: Record) -> Option<Vec<u64>> {
        Some(self.base_container.read_record(record))
    }
}

#[derive(Debug, Clone)]
pub struct RecordAddress {
    pub page: Arc<Mutex<PhysicalPage>>,
    pub offset: u64,
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct Record {
    #[pyo3(get)]
    pub rid: u64,
    pub addresses: Arc<Mutex<Vec<RecordAddress>>>,
}

#[pymethods]
impl Record {
    fn __str__(&self) -> String {
        // Print the Addresses from RecordAddress
        let addresses = self.addresses.lock().unwrap();
        let mut addrs = Vec::<String>::new();
        let b: Vec<RecordAddress> = addresses.clone();

        for a in b {
            let c: RecordAddress = a;
            let d = c.page;
            addrs.push(format!(
                "0x{:?} + {}",
                &d as *const Arc<Mutex<PhysicalPage>> as usize, c.offset
            ));
        }

        format!("Record(rid={}, addresses={:?})", self.rid, addrs)
    }
}

#[derive(Clone)]
#[pyclass]
pub struct RTable {
    pub name: String,
    pub primary_key_column: usize,
    pub page_range: PageRange,

    // Map RIDs to Records
    pub page_directory: HashMap<u64, Record>,
    pub num_records: u64,

    #[pyo3(get)]
    pub num_columns: usize,

    pub index: RIndex,
}

impl RTable {
    pub fn write(&mut self, values: Vec<u64>) -> Record {
        // Use the primary_key_column'th value as the given key
        let given_key = values[self.primary_key_column];
        let rec = self.page_range.write(self.num_records, values);

        // Save the RID -> Record so it can later be read
        self.page_directory.insert(given_key, rec.clone());

        self.num_records += 1;
        return rec;
    }

    pub fn read(&self, rid: u64) -> Option<Vec<u64>> {
        let rec = self.page_directory.get(&rid);

        // If the rec exists in the page_directory, return the read values
        match rec {
            Some(r) => self.page_range.read(r.clone()),
            None => None,
        }
    }

    pub fn delete(&mut self, rid: u64) {
        self.page_directory.remove(&rid);
    }

    pub fn sum(&mut self, start: u64, end: u64, col_index: u64) -> i64 {
        let mut agg = 0i64;

        // Make sum range inclusive
        // TODO: Validate this assumption if it should actually be inclusive
        for rid in start..end + 1 {
            if let Some(v) = self.read(rid) {
                agg += v[col_index as usize] as i64;
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
    tables: Vec<RTable>,
    // Map table names to index on the tables: Vec<RTable>
    tables_hashmap: HashMap<String, usize>,
}

#[pymethods]
impl RDatabase {
    #[new]
    fn new() -> Self {
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

    fn create_table(&mut self, name: String, num_columns: u64, primary_key_column: u64) -> RTable {
        let t = RTable {
            name: name.clone(),
            page_range: PageRange::new(num_columns as u64),
            primary_key_column: primary_key_column as usize,
            page_directory: HashMap::new(),
            num_columns: num_columns as usize,
            num_records: 0,
            index: RIndex::new(primary_key_column, num_columns as usize),
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
}
