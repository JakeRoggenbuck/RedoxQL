use crate::container::NUM_RESERVED_COLUMNS;
use crate::table::RTableHandle;
use super::record::Record;
use super::table::RTable;
use pyo3::prelude::*;
use std::iter::zip;
use std::fmt;
use super::container::ReservedColumns;
use std::sync::{Arc, RwLock};

#[pyclass]
pub struct RQuery {
    pub handle: RTableHandle,
}

/// Filters the given column values based on the projected flags. Internally, we
/// prepend three flags (for reserved columns) to the provided projection.
fn filter_projected(column_values: &[i64], projected: &[i64]) -> Vec<Option<i64>> {
    let mut projected_flags = Vec::with_capacity(NUM_RESERVED_COLUMNS as usize + projected.len());
    projected_flags.extend_from_slice(&[1, 1, 1, 1]);
    projected_flags.extend_from_slice(projected);

    // For each value, if the corresponding flag is 1, include the value, else None.
    column_values
        .iter()
        .zip(projected_flags.into_iter())
        .map(|(&val, flag)| if flag == 1 { Some(val) } else { None })
        .collect()
}

#[pymethods]
impl RQuery {
    #[new]
    pub fn new(handle: RTableHandle) -> Self {
        RQuery { handle }
    }

    pub fn delete(&mut self, primary_key: i64) {
        let mut table = self.handle.table.write().unwrap();
        table.delete(primary_key);
    }

    pub fn insert(&mut self, values: Vec<i64>) -> Option<Record> {
        let mut table = self.handle.table.write().unwrap();

        // Check if the primary key (assumed to be at index table.primary_key_column)
        // already exists.
        {
            let index = table.index.read().unwrap();
            if index.get(values[table.primary_key_column]).is_some() {
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
    ) -> Option<Vec<Vec<Option<i64>>>> {
        let table = self.handle.table.read().unwrap();

        // Case 1: Searching by primary key.
        if search_key_index == table.primary_key_column as i64 {
            if let Some(ret) = table.read(search_key) {
                return Some(vec![filter_projected(&ret, &projected_columns_index)]);
            } else {
                return None;
            }
        }
        // Case 2: Searching by a non–primary column.
        else {
            let index = table.index.read().unwrap();
            if let Some(sec_index) = index.secondary_indices.get(&search_key_index) {
                if let Some(rids) = sec_index.get(&search_key) {
                    let results: Vec<_> = rids
                        .iter()
                        .filter_map(|&rid| {
                            table
                                .read_by_rid(rid)
                                .map(|record_data| filter_projected(&record_data, &projected_columns_index))
                        })
                        .collect();
                    return Some(results);
                } else {
                    return Some(vec![]); // No matching records.
                }
            }
            // If no secondary index exists, perform a full scan.
            else {
                let results: Vec<_> = table
                    .page_directory
                    .directory
                    .iter()
                    .filter_map(|(_rid, record)| {
                        table.page_range.read(record.clone()).and_then(|record_data| {
                            if record_data[(search_key_index + NUM_RESERVED_COLUMNS) as usize] == search_key {
                                Some(filter_projected(&record_data, &projected_columns_index))
                            } else {
                                None
                            }
                        })
                    })
                    .collect();
                return Some(results);
            }
        }
    }

    pub fn select_version(
        &mut self,
        primary_key: i64,
        _search_key_index: i64,
        projected_columns_index: Vec<i64>,
        relative_version: i64,
    ) -> Option<Vec<Option<i64>>> {
        let table = self.handle.table.read().unwrap();
        let ret = table.read_relative(primary_key, relative_version)?;
        Some(filter_projected(&ret, &projected_columns_index))
    }

    pub fn update(&mut self, primary_key: i64, columns: Vec<Option<i64>>) -> bool {
        let mut table = self.handle.table.write().unwrap();

        if columns.len() != table.num_columns {
            return false;
        }

        // Retrieve the record identifier (RID) for the given primary key.
        let rid = {
            let index = table.index.read().unwrap();
            match index.get(primary_key) {
                Some(rid) => *rid,
                None => return false,
            }
        };

        // Disallow updating the primary key to an existing key.
        if let Some(new_pk) = columns.get(table.primary_key_column as usize).and_then(|&v| v) {
            if new_pk != primary_key && table.index.read().unwrap().get(new_pk).is_some() {
                return false;
            }
        }

        let record = match table.page_directory.directory.get(&rid) {
            Some(rec) => rec.clone(),
            None => return false,
        };

        let current_record = match table.page_range.read(record.clone()) {
            Some(r) => r,
            None => return false,
        };

        let base_rid = current_record[ReservedColumns::RID as usize];
        let base_schema_encoding = current_record[ReservedColumns::SchemaEncoding as usize];
        let base_indirection_column = current_record[ReservedColumns::Indirection as usize];

        let addrs_base = record.addresses.lock().unwrap();
        let mut new_record_columns = if base_rid == base_indirection_column {
            // First update: if schema encoding is zero, update it.
            if base_schema_encoding == 0 {
                let mut schema_encoding = addrs_base[ReservedColumns::SchemaEncoding as usize]
                    .page
                    .lock()
                    .unwrap();
                schema_encoding.overwrite(
                    addrs_base[ReservedColumns::SchemaEncoding as usize].offset as usize,
                    1,
                );
            }
            current_record
        } else {
            // Subsequent updates: adjust the tail record’s schema encoding.
            let existing_tail_record = match table.page_directory.directory.get(&base_indirection_column) {
                Some(r) => r.clone(),
                None => return false,
            };
            {
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
            match table.page_range.read(existing_tail_record.clone()) {
                Some(r) => r,
                None => return false,
            }
        };

        let new_primary_key = columns
            .get(table.primary_key_column as usize)
            .and_then(|&v| v)
            .unwrap_or(primary_key);

        // Remove the reserved (internal) columns.
        new_record_columns.drain(0..NUM_RESERVED_COLUMNS as usize);

        // Overwrite the new record values with provided updates.
        for i in 0..new_record_columns.len() {
            if let Some(value) = columns.get(i).copied().flatten() {
                new_record_columns[i] = value;
            }
        }

        let new_rid = table.num_records;
        let new_rec = table.page_range.tail_container.insert_record(
            new_rid,
            base_indirection_column,
            rid,
            new_record_columns,
        );

        table.page_directory.directory.insert(new_rid, new_rec);

        // Update the primary key index if needed.
        if new_primary_key != primary_key {
            let mut index = table.index.write().unwrap();
            index.index.remove(&primary_key);
            index.index.insert(new_primary_key, new_rid);
        }

        // Update the indirection column in the base record.
        let mut indirection_page = addrs_base[ReservedColumns::Indirection as usize]
            .page
            .lock()
            .unwrap();
        indirection_page.overwrite(
            addrs_base[ReservedColumns::Indirection as usize].offset as usize,
            new_rid,
        );

        table.num_records += 1;
        true
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
        table.sum_version(start_primary_key, end_primary_key, col_index, relative_version)
    }

    pub fn increment(&mut self, primary_key: i64, column: i64) -> bool {
        // Determine the number of user columns.
        let num_cols = self.handle.table.read().unwrap().num_columns;
        let select_flags = vec![1; num_cols];

        if let Some(records) = self.select(primary_key, 0, select_flags) {
            let record = &records[0];
            let current_value = record[(column + NUM_RESERVED_COLUMNS) as usize].unwrap();
            let mut update_values = vec![None; num_cols];
            update_values[column as usize] = Some(current_value + 1);
            self.update(primary_key, update_values)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::RDatabase;

    #[test]
    fn test_insert_and_read_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.insert(vec![1, 2, 3]);

        // Use primary_key of 1
        let vals = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals.unwrap()[0],
            vec![Some(0), Some(0), Some(0), Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn increment_test() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Counts"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.insert(vec![1, 2, 3]); // Insert [Primary Key: 1, Col1: 2, Col2: 3]

        // Increment the first user column (column 1)
        q.increment(1, 1);

        let vals = q.select(1, 0, vec![1, 1, 1]); // Select entire row
        assert_eq!(
            vals.unwrap()[0],
            vec![Some(1), Some(0), Some(0), Some(1), Some(3), Some(3)]
        );

        q.increment(1, 1);
        let vals2 = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals2.unwrap()[0],
            vec![Some(2), Some(0), Some(1), Some(1), Some(4), Some(3)]
        );

        q.increment(1, 1);
        let vals3 = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals3.unwrap()[0],
            vec![Some(3), Some(0), Some(2), Some(1), Some(5), Some(3)]
        );
    }

    /*
    #[test]
    fn increment_test() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Counts"), 3, 0);
        let mut q = RQuery::new(t);

        q.insert(vec![1, 2, 3]); // Insert [Primary Key: 1, Col1: 2, Col2: 3]

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
        let vals = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals.unwrap()[0],
            vec![Some(0), Some(0), Some(0), Some(1), Some(2), Some(3)]
        );

        let success = q.update(1, vec![Some(1), Some(5), Some(6)]);
        assert!(success);

        let vals2 = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals2.unwrap()[0],
            vec![Some(1), Some(0), Some(0), Some(1), Some(5), Some(6)]
        );
    }

    // #[test]
    // #[should_panic(expected = "Primary key cannot be changed")]
    // fn test_update_primary_key_should_panic() {
    //     let mut db = RDatabase::new();
    //     let t = db.create_table(String::from("Grades"), 3, 0);
    //     let mut q = RQuery::new(t);

    //     q.insert(vec![1, 2, 3]);

    //     // Try to update primary key from 1 to 2
    //     q.update(1, vec![Some(2), Some(5), Some(6)]);
    // }

    #[test]
    fn test_multiple_updates() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.insert(vec![1, 2, 3]);

        q.update(1, vec![Some(1), Some(4), Some(5)]);
        q.update(1, vec![Some(1), Some(6), Some(7)]);
        q.update(1, vec![Some(1), Some(8), Some(9)]);

        let vals = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals.unwrap()[0],
            vec![Some(3), Some(0), Some(2), Some(1), Some(8), Some(9)]
        );
    }

    #[test]
    fn test_delete_and_select() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.insert(vec![1, 2, 3]);
        q.delete(1);

        assert_eq!(q.select(1, 0, vec![1, 1, 1]), None);
    }

    #[test]
    fn test_select_version() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(table_ref);

        // Insert initial record
        q.insert(vec![1, 2, 3]);

        // Make multiple updates
        q.update(1, vec![Some(1), Some(4), Some(5)]); // Version 1
        q.update(1, vec![Some(1), Some(6), Some(7)]); // Version 2
        q.update(1, vec![Some(1), Some(8), Some(9)]); // Version 3

        // Test different versions
        let latest = q.select_version(1, 0, vec![1, 1, 1], 0);
        assert_eq!(
            latest.unwrap(),
            vec![Some(3), Some(0), Some(2), Some(1), Some(8), Some(9)]
        ); // Most recent version

        let one_back = q.select_version(1, 0, vec![1, 1, 1], 1);
        assert_eq!(
            one_back.unwrap(),
            vec![Some(2), Some(1), Some(1), Some(1), Some(6), Some(7)]
        ); // One version back

        let two_back = q.select_version(1, 0, vec![1, 1, 1], 2);
        assert_eq!(
            two_back.unwrap(),
            vec![Some(1), Some(1), Some(0), Some(1), Some(4), Some(5)]
        ); // Two versions back

        let original = q.select_version(1, 0, vec![1, 1, 1], 3);
        assert_eq!(
            original.unwrap(),
            vec![Some(0), Some(1), Some(3), Some(1), Some(2), Some(3)]
        ); // Original version
    }

    #[test]
    fn test_insert_existing_primary_key() {
        let mut db = RDatabase::new();
        let table_ref = db.create_table("Grades".to_string(), 3, 0);
        let mut q = RQuery::new(table_ref);

        q.insert(vec![1, 2, 3]);

        // Attempt to insert a record with an existing primary key
        let result = q.insert(vec![1, 4, 5]);
        assert!(result.is_none());

        // Verify that the original record is still intact
        let vals = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(
            vals.unwrap()[0],
            vec![Some(0), Some(0), Some(0), Some(1), Some(2), Some(3)]
        );
    }

    /* Seems like M2 test wants us to delete the record if primary key is changed

    #[test]
    fn test_update_existing_primary_key() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(t);

        q.insert(vec![1, 2, 3]);
        q.insert(vec![4, 5, 6]);

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
