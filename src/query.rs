use super::container::{ReservedColumns, NUM_RESERVED_COLUMNS};
use super::record::{RReturnRecord, Record};
use super::table::RTableHandle;
use pyo3::prelude::*;
use std::iter::zip;
use std::sync::atomic::{AtomicBool, Ordering};

#[pyclass]
pub struct RQuery {
    // pub table: RTable,
    pub handle: RTableHandle,
    merging: AtomicBool,
}

/// Use the projected vector to decide which columns to set to None
fn filter_projected(column_values: Vec<i64>, projected: Vec<i64>) -> Vec<Option<i64>> {
    // TODO: HUGE SPEED IMPROVEMENT AVAILABLE - Don't read columns that will be None anyway!!!

    // Add the 4 columns used internally
    let mut projected_cols: Vec<i64> = vec![1, 1, 1, 1];
    projected_cols.extend(projected.clone());

    let mut out: Vec<Option<i64>> = vec![];

    for (a, b) in zip(column_values, projected_cols) {
        out.push(match b {
            1 => Some(a),
            _ => None,
        });
    }

    return out;
}

#[pymethods]
impl RQuery {
    #[new]
    pub fn new(handle: RTableHandle) -> Self {
        let binding = handle.table.clone();
        let mut t = binding.write().unwrap();

        if t.num_records > 0 && t.updates_since_merge > 100000 {
            t.merge();
            t.updates_since_merge = 0;
        }

        RQuery {
            handle,
            merging: AtomicBool::new(false),
        }
    }

    pub fn delete(&mut self, primary_key: i64) {
        let mut table = self.handle.table.write().unwrap();
        table.delete(primary_key);
    }

    pub fn insert(&mut self, values: Vec<i64>) -> bool {
        self.internal_insert(values).is_some()
    }

    fn internal_insert(&mut self, values: Vec<i64>) -> Option<Record> {
        let mut table = self.handle.table.write().unwrap();
        // check if primary key already exists
        {
            let index = table.index.read().unwrap();
            if index.get(values[table.primary_key_column]) != None {
                return None;
            }
        }

        Some(table.write(values))
    }

    pub fn select(
        &mut self,
        search_key: i64,
        search_key_index: i64,
        projected_columns_index: Vec<i64>,
    ) -> Option<Vec<Option<RReturnRecord>>> {
        let ret = self.internal_select(search_key, search_key_index, projected_columns_index);

        let mut out = vec![];

        match ret {
            Some(rows) => {
                for row in rows {
                    let mut a = row;
                    // Remove the first 4 columns that are used only internally
                    a.drain(0..4);
                    // Return the columns encased in the RReturnRecord struct
                    out.push(Some(RReturnRecord { columns: a }))
                }
            }
            None => {}
        }

        return Some(out);
    }

    /// Formerly just `select` does a select on the database
    fn internal_select(
        &mut self,
        search_key: i64,
        search_key_index: i64,
        projected_columns_index: Vec<i64>,
    ) -> Option<Vec<Vec<Option<i64>>>> {
        let table = self.handle.table.read().unwrap();

        // table.page_directory.display(); // -> This shows the full page dir!!

        // Case 1: Searching on the primary key column
        if search_key_index == table.primary_key_column as i64 {
            if let Some(ret) = table.read(search_key) {
                return Some(vec![filter_projected(ret, projected_columns_index)]);
            } else {
                return None;
            }
        }
        // Case 2: Searching on a non-primary column
        else {
            // If a secondary index exists, use it
            let index = table.index.read().unwrap();
            if let Some(sec_index) = index.secondary_indices.get(&search_key_index) {
                if let Some(rids) = sec_index.get(&search_key) {
                    let mut results = Vec::new();
                    for &rid in rids {
                        if let Some(record_data) = table.read_by_rid(rid) {
                            results.push(filter_projected(
                                record_data,
                                projected_columns_index.clone(),
                            ));
                        }
                    }
                    return Some(results);
                } else {
                    return Some(vec![]); // No records match
                }
            }
            // Otherwise, do a full scan
            else {
                let mut results = Vec::new();
                for (_rid, record) in table.page_directory.directory.iter() {
                    if let Some(record_data) = table.page_range.read(record.clone()) {
                        if record_data[(search_key_index + NUM_RESERVED_COLUMNS) as usize]
                            == search_key
                        {
                            results.push(filter_projected(
                                record_data,
                                projected_columns_index.clone(),
                            ));
                        }
                    }
                }
                return Some(results);
            }
        }
    }

    /// Formerly just `select_version` does a select and picks which version of the record
    fn internal_select_version(
        &mut self,
        primary_key: i64,
        _search_key_index: i64,
        projected_columns_index: Vec<i64>,
        relative_version: i64,
    ) -> Option<Vec<Option<i64>>> {
        let table = self.handle.table.read().unwrap();
        let Some(ret) = table.read_relative(primary_key, relative_version) else {
            return None;
        };

        Some(filter_projected(ret, projected_columns_index))
    }

    pub fn select_version(
        &mut self,
        primary_key: i64,
        _search_key_index: i64,
        projected_columns_index: Vec<i64>,
        relative_version: i64,
    ) -> Option<Vec<Option<RReturnRecord>>> {
        match self.internal_select_version(
            primary_key,
            0,
            projected_columns_index,
            relative_version,
        ) {
            // Return the columns encased in the RReturnRecord struct
            Some(columns) => Some(vec![Some(RReturnRecord { columns })]),
            None => None,
        }
    }

    pub fn update(&mut self, primary_key: i64, columns: Vec<Option<i64>>) -> bool {
        let mut table = self.handle.table.write().unwrap();

        {
            if table.num_records > 0
                && table.updates_since_merge > 100000
                && !self.merging.load(Ordering::Relaxed)
            {
                self.merging.store(true, Ordering::Relaxed);

                table.merge();
                table.updates_since_merge = 0;

                self.merging.store(false, Ordering::Relaxed);
            }

            table.updates_since_merge += 1;
        }

        // This functin expects an expact number of columns as table has
        if columns.len() != table.num_columns {
            return false;
        }

        let mut new_columns: Vec<i64>;

        // Check if the record found by primary_key exists
        let index = table.index.read().unwrap();
        let Some(rid) = index.get(primary_key) else {
            return false;
        };

        // do not allow primary key to be changed to an existing primary key
        if let Some(new_primary_key) = columns[table.primary_key_column as usize] {
            if primary_key != new_primary_key && index.get(new_primary_key) != None {
                return false;
            }
        }

        // Get record by RID
        let record = match table.page_directory.directory.get(&rid).cloned() {
            Some(r) => r,
            None => return false,
        };
        drop(index);

        let Some(result) = table.page_range.read(record.clone()) else {
            return false;
        };

        let indirection_column = ReservedColumns::Indirection as usize;

        // Get values from record for the 4 internal columns
        let base_rid = result[ReservedColumns::RID as usize];
        let base_schema_encoding = result[ReservedColumns::SchemaEncoding as usize];
        let base_indirection_column = result[ReservedColumns::Indirection as usize];

        // base record addresses
        let addrs_base = record.addresses.lock().unwrap();

        if base_rid == base_indirection_column {
            // first update
            if base_schema_encoding == 0 {
                let mut base_schema_encoding = addrs_base[ReservedColumns::SchemaEncoding as usize]
                    .page
                    .lock()
                    .unwrap();
                base_schema_encoding.overwrite(
                    addrs_base[ReservedColumns::SchemaEncoding as usize].offset as usize,
                    1,
                );
            }

            new_columns = result;
        } else {
            // second and subsequent updates
            let Some(existing_tail_record) =
                table.page_directory.directory.get(&base_indirection_column)
            else {
                return false;
            };

            {
                let _tail_cont = &table.page_range.tail_container;
                // update schema encoding of the tail to be 1 (since record has changed)
                let addrs_existing = existing_tail_record.addresses.lock().unwrap();
                let mut schema_encoding = addrs_existing[ReservedColumns::SchemaEncoding as usize]
                    .page
                    .lock()
                    .unwrap();
                schema_encoding.overwrite(
                    addrs_existing[ReservedColumns::SchemaEncoding as usize].offset as usize,
                    1,
                );
            }

            let Some(result) = table.page_range.read(existing_tail_record.clone()) else {
                return false;
            };

            new_columns = result;
        }
        // drop(base_cont);

        // Extract the new primary key (if provided)
        let mut new_primary_key = primary_key;
        if let Some(pk) = columns[table.primary_key_column] {
            new_primary_key = pk;
        }

        // drop first 4 columns (rid, schema_encoding, indirection, base_rid)
        new_columns.drain(0..NUM_RESERVED_COLUMNS as usize);

        // overwrite columns values onto new_columns values (that unwrap successfully)
        for i in 0..new_columns.len() {
            if let Some(value) = columns[i] {
                new_columns[i] = value;
            }
        }

        let new_rid = table.num_records;

        let new_rec = table.page_range.tail_container.insert_record(
            new_rid,
            base_indirection_column,
            base_rid,
            new_columns,
        );

        // update the page directory with the new record
        table.page_directory.directory.insert(new_rid, new_rec);

        // update the index with the new primary key
        if new_primary_key != primary_key {
            let mut index = table.index.write().unwrap();
            index.index.remove(&primary_key);
            index.index.insert(new_primary_key, new_rid);
        }

        // update the indirection column of the base record
        let mut indirection_page = addrs_base[indirection_column as usize].page.lock().unwrap();
        indirection_page.overwrite(
            addrs_base[indirection_column as usize].offset as usize,
            new_rid,
        );

        table.num_records += 1;

        return true;
    }

    pub fn sum(&mut self, start_primary_key: i64, end_primary_key: i64, col_index: i64) -> i64 {
        let mut table = self.handle.table.write().unwrap();
        table.sum(start_primary_key, end_primary_key, col_index)
    }

    fn sum_version(
        &mut self,
        start_primary_key: i64,
        end_primary_key: i64,
        col_index: i64,
        relative_version: i64,
    ) -> i64 {
        let mut table = self.handle.table.write().unwrap();
        table.sum_version(
            start_primary_key,
            end_primary_key,
            col_index,
            relative_version,
        )
    }

    pub fn increment(&mut self, primary_key: i64, column: i64) -> bool {
        let num_cols = {
            let table = self.handle.table.read().unwrap();
            table.num_columns
        };

        // Select the value of the column before we increment
        let cols = vec![1i64; num_cols];

        let ret = self.internal_select(primary_key, 0, cols);

        if let Some(records) = ret {
            let record = &records[0];
            let current_value = record[(column + NUM_RESERVED_COLUMNS) as usize].unwrap();
            let mut to_update: Vec<Option<i64>> = vec![None; num_cols];
            to_update[column as usize] = Some(current_value + 1);
            return self.update(primary_key, to_update);
        }

        return false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::RDatabase;

    #[test]
    fn test_internal_insert_and_read_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.internal_insert(vec![1, 2, 3]);

        // Use primary_key of 1
        let vals = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals.unwrap()[0],
            vec![
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(2),
                Some(3)
            ]
        );
    }

    #[test]
    fn increment_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Counts"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.internal_insert(vec![1, 2, 3]); // Insert [Primary Key: 1, Col1: 2, Col2: 3]

        // Increment the first user column (column 1)
        q.increment(1, 1);

        let vals = q.internal_select(1, 0, vec![1, 1, 1]); // Select entire row
        assert_eq!(
            vals.unwrap()[0],
            vec![
                Some(1),
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(3),
                Some(3)
            ]
        );

        q.increment(1, 1);

        let vals2 = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals2.unwrap()[0],
            vec![
                Some(2),
                Some(0),
                Some(1),
                Some(0),
                Some(1),
                Some(4),
                Some(3)
            ]
        );

        q.increment(1, 1);

        let vals3 = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals3.unwrap()[0],
            vec![
                Some(3),
                Some(0),
                Some(2),
                Some(0),
                Some(1),
                Some(5),
                Some(3)
            ]
        );
    }

    /*
    #[test]
    fn increment_test() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Counts"), 3, 0);
        let mut q = RQuery::new(t);

        q.internal_insert(vec![1, 2, 3]); // Insert [Primary Key: 1, Col1: 2, Col2: 3]

        // Increment the first user column (column 1)
        q.increment(1, 1);

        let vals = q.select(1, 0, vec![1, 1, 1]); // Select entire row
        assert_eq!(vals.unwrap()[0], vec![Some(0), Some(0), Some(0), Some(1), Some(3), Some(3)]);

        q.increment(1, 1);
        let vals2 = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(vals2.unwrap()[0], vec![Some(1), Some(0), Some(1), Some(1), Some(4), Some(3)]);

        q.increment(1, 1);
        let vals3 = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(vals3.unwrap()[0], vec![Some(2), Some(0), Some(2), Some(1), Some(5), Some(3)]);
    }

     */

    #[test]
    fn test_update_read_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.insert(vec![1, 2, 3]);

        // Use primary_key of 1
        let vals = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals.unwrap()[0],
            vec![
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(2),
                Some(3)
            ]
        );

        let success = q.update(1, vec![Some(1), Some(5), Some(6)]);
        assert!(success);

        let vals2 = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals2.unwrap()[0],
            vec![
                Some(1),
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(5),
                Some(6)
            ]
        );
    }

    // #[test]
    // #[should_panic(expected = "Primary key cannot be changed")]
    // fn test_update_primary_key_should_panic() {
    //     let mut db = RDatabase::new();
    //     let t = db.create_table(String::from("Grades"), 3, 0);
    //     let mut q = RQuery::new(t);

    //     q.internal_insert(vec![1, 2, 3]);

    //     // Try to update primary key from 1 to 2
    //     q.update(1, vec![Some(2), Some(5), Some(6)]);
    // }

    #[test]
    fn test_multiple_updates() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.internal_insert(vec![1, 2, 3]);

        q.update(1, vec![Some(1), Some(4), Some(5)]);
        q.update(1, vec![Some(1), Some(6), Some(7)]);
        q.update(1, vec![Some(1), Some(8), Some(9)]);

        let vals = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals.unwrap()[0],
            vec![
                Some(3),
                Some(0),
                Some(2),
                Some(0),
                Some(1),
                Some(8),
                Some(9)
            ]
        );
    }

    #[test]
    fn test_delete_and_select() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.internal_insert(vec![1, 2, 3]);
        q.delete(1);

        assert_eq!(q.internal_select(1, 0, vec![1, 1, 1]), None);
    }

    #[test]
    fn test_select_version() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        // Insert initial record
        q.internal_insert(vec![1, 2, 3]);

        // Make multiple updates
        q.update(1, vec![Some(1), Some(4), Some(5)]); // Version 1
        q.update(1, vec![Some(1), Some(6), Some(7)]); // Version 2
        q.update(1, vec![Some(1), Some(8), Some(9)]); // Version 3

        // Test different versions
        let latest = q.internal_select_version(1, 0, vec![1, 1, 1], 0);
        assert_eq!(
            latest.unwrap(),
            vec![
                Some(3),
                Some(0),
                Some(2),
                Some(0),
                Some(1),
                Some(8),
                Some(9)
            ]
        ); // Most recent version

        let one_back = q.internal_select_version(1, 0, vec![1, 1, 1], 1);
        assert_eq!(
            one_back.unwrap(),
            vec![
                Some(2),
                Some(1),
                Some(1),
                Some(0),
                Some(1),
                Some(6),
                Some(7)
            ]
        ); // One version back

        let two_back = q.internal_select_version(1, 0, vec![1, 1, 1], 2);
        assert_eq!(
            two_back.unwrap(),
            vec![
                Some(1),
                Some(1),
                Some(0),
                Some(0),
                Some(1),
                Some(4),
                Some(5)
            ]
        ); // Two versions back

        let original = q.internal_select_version(1, 0, vec![1, 1, 1], 3);
        assert_eq!(
            original.unwrap(),
            vec![
                Some(0),
                Some(1),
                Some(3),
                Some(0),
                Some(1),
                Some(2),
                Some(3)
            ]
        ); // Original version
    }

    #[test]
    fn test_internal_insert_existing_primary_key() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("Grades".to_string(), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.internal_insert(vec![1, 2, 3]);

        // Attempt to internal_insert a record with an existing primary key
        let result = q.internal_insert(vec![1, 4, 5]);
        assert!(result.is_none());

        // Verify that the original record is still intact
        let vals = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals.unwrap()[0],
            vec![
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(2),
                Some(3)
            ]
        );
    }

    #[test]
    fn merge_one_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref.clone());

        // Insert initial record
        q.internal_insert(vec![1, 2, 3]);

        // Make multiple updates
        q.update(1, vec![Some(1), Some(4), Some(5)]); // Version 1
        q.update(1, vec![Some(1), Some(6), Some(7)]); // Version 2
        q.update(1, vec![Some(1), Some(8), Some(9)]); // Version 3

        q = RQuery::new(table_ref);

        let v = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vec![
                Some(3),
                Some(0),
                Some(2),
                Some(0),
                Some(1),
                Some(8),
                Some(9)
            ],
            v.unwrap()[0]
        );
    }

    #[test]
    fn merge_two_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades2"), 3, 0);
        let mut q = RQuery::new(table_ref.clone());

        // Insert initial record
        q.internal_insert(vec![1, 2, 3]);

        for _ in 0..600 {
            q.update(1, vec![Some(1), Some(4), Some(5)]); // Version 1
        }

        q = RQuery::new(table_ref.clone());

        let v = q.internal_select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vec![
                Some(600),
                Some(0),
                Some(599),
                Some(0),
                Some(1),
                Some(4),
                Some(5)
            ],
            v.unwrap()[0]
        );
    }

    /* Seems like M2 test wants us to delete the record if primary key is changed

    #[test]
    fn test_update_existing_primary_key() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(t);

        q.internal_insert(vec![1, 2, 3]);
        q.internal_insert(vec![4, 5, 6]);

        // Attempt to update the primary key of the first record to an existing primary key
        let result = q.update(1, vec![Some(4), Some(7), Some(8)]);
        assert!(!result);

        // Verify that the original records are still intact
        let vals1 = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(vals1.unwrap()[0], vec![Some(0), Some(0), Some(0), Some(1), Some(2), Some(3)]);

        let vals2 = q.select(4, 0, vec![1, 1, 1]);
        assert_eq!(vals2.unwrap()[0], vec![Some(1), Some(0), Some(1), Some(4), Some(5), Some(6)]);
    }
    */
}
