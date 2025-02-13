use super::database::{RDatabase, RTable, Record};
use pyo3::prelude::*;

#[pyclass]
pub struct RQuery {
    pub table: RTable,
}

#[pymethods]
impl RQuery {
    #[new]
    fn new(table: RTable) -> Self {
        RQuery { table }
    }

    fn delete(&mut self, primary_key: i64) {
        self.table.delete(primary_key)
    }

    fn insert(&mut self, values: Vec<i64>) -> Record {
        self.table.write(values)
    }

    fn select(
        &mut self,
        primary_key: i64,
        _search_key_index: i64,
        _projected_columns_index: Vec<i64>,
    ) -> Option<Vec<i64>> {
        let Some(result) = self.table.read(primary_key as i64) else {
            return None;
        };

        // check if indirection column is different from RID
        let base_rid = result[self.table.page_range.base_container.rid_column as usize];
        let base_indirection_column =
            result[self.table.page_range.base_container.indirection_column as usize];

        // if indirection column is different from RID, read the tail record
        if base_rid != base_indirection_column {
            let rec = self.table.page_directory.get(&base_indirection_column);

            match rec {
                Some(r) => return self.table.page_range.read(r.clone()),
                None => return None,
            }
        }

        return Some(result);
    }

    fn select_version(
        &mut self,
        primary_key: i64,
        _search_key_index: i64,
        _projected_columns_index: Vec<i64>,
        relative_version: i64,
    ) -> Option<Vec<i64>> {
        let Some(result) = self.table.read(primary_key as i64) else {
            return None;
        };

        // check if indirection column is different from RID
        let base_rid = result[self.table.page_range.base_container.rid_column as usize];
        let base_indirection_column =
            result[self.table.page_range.base_container.indirection_column as usize];

        // if base record hasn't been updated, return it
        if base_rid == base_indirection_column {
            return Some(result);
        }

        // start from the most recent tail record
        let mut current_rid = base_indirection_column;
        let mut versions_back = 0;
        let target_version = relative_version.abs() as i64; // Convert to positive and unsigned

        while versions_back < target_version {
            let Some(current_record) = self.table.page_directory.get(&current_rid) else {
                return None;
            };

            // read the current record
            let Some(record_data) = self.table.page_range.read(current_record.clone()) else {
                return None;
            };

            // get the indirection of the previous version
            let prev_indirection =
                record_data[self.table.page_range.tail_container.indirection_column as usize];

            // if we've reached the base record, stop here
            if prev_indirection == base_rid {
                current_rid = base_rid;
                break;
            }

            current_rid = prev_indirection;
            versions_back += 1;
        }

        // read the final record we want
        let Some(final_record) = self.table.page_directory.get(&current_rid) else {
            return None;
        };

        return self.table.page_range.read(final_record.clone());
    }

    fn update(&mut self, primary_key: i64, columns: Vec<i64>) -> bool {
        if columns.len() != self.table.num_columns {
            panic!("Columns length does not match table columns length");
        }
        if columns[self.table.primary_key_column as usize] != primary_key as i64 {
            panic!("Primary key cannot be changed");
        }
        let Some(rid) = self.table.index.get(primary_key as i64) else {
            return false;
        };

        let record = match self.table.page_directory.get(&rid).cloned() {
            Some(r) => r,
            None => return false,
        };

        let Some(result) = self.table.page_range.read(record.clone()) else {
            return false;
        };

        let base_rid = result[self.table.page_range.base_container.rid_column as usize];
        let base_schema_encoding =
            result[self.table.page_range.base_container.schema_encoding_column as usize];
        let base_indirection_column =
            result[self.table.page_range.base_container.indirection_column as usize];

        // base record addresses
        let addrs_base = record.addresses.lock().unwrap();

        if base_rid == base_indirection_column {
            // first update
            if base_schema_encoding == 0 {
                let mut base_schema_encoding = addrs_base
                    [self.table.page_range.base_container.schema_encoding_column as usize]
                    .page
                    .lock()
                    .unwrap();
                base_schema_encoding.overwrite(
                    addrs_base[self.table.page_range.base_container.schema_encoding_column as usize]
                        .offset as usize,
                    1,
                );
            }
        } else {
            // second and subsequent updates
            let Some(existing_tail_record) =
                self.table.page_directory.get(&base_indirection_column)
            else {
                return false;
            };

            // update schema encoding of the tail to be 1 (since record has changed)
            let addrs_existing = existing_tail_record.addresses.lock().unwrap();
            let mut schema_encoding = addrs_existing
                [self.table.page_range.tail_container.schema_encoding_column as usize]
                .page
                .lock()
                .unwrap();
            schema_encoding.overwrite(
                addrs_existing[self.table.page_range.tail_container.schema_encoding_column as usize]
                    .offset as usize,
                1,
            );
        }

        let new_rid = self.table.num_records;
        let new_rec = self.table.page_range.tail_container.insert_record(
            new_rid,
            base_indirection_column,
            columns,
        );

        self.table.page_directory.insert(new_rid, new_rec.clone());

        // update the indirection column of the base record
        let mut indirection_page = addrs_base
            [self.table.page_range.base_container.indirection_column as usize]
            .page
            .lock()
            .unwrap();
        indirection_page.overwrite(
            addrs_base[self.table.page_range.base_container.indirection_column as usize].offset
                as usize,
            new_rid,
        );

        self.table.num_records += 1;

        return true;
    }

    fn sum(&mut self, start_primary_key: i64, end_primary_key: i64, col_index: i64) -> i64 {
        self.table
            .sum(start_primary_key, end_primary_key, col_index)
    }

    fn sum_version(&mut self) {}

    fn increment(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_read_test() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(t);

        q.insert(vec![1, 2, 3]);

        // Use primary_key of 1
        let vals = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(vals.unwrap(), vec![0, 0, 0, 1, 2, 3]);
    }

    #[test]
    fn test_update_read_test() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(t);

        q.insert(vec![1, 2, 3]);

        // Use primary_key of 1
        let vals = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(vals.unwrap(), vec![0, 0, 0, 1, 2, 3]);

        let success = q.update(1, vec![1, 5, 6]);
        assert!(success);

        let vals2 = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(vals2.unwrap(), vec![1, 0, 0, 1, 5, 6]);
    }
    #[test]
    #[should_panic(expected = "Primary key cannot be changed")]
    fn test_update_primary_key_should_panic() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(t);

        q.insert(vec![1, 2, 3]);

        // Try to update primary key from 1 to 2
        q.update(1, vec![2, 5, 6]);
    }

    #[test]
    fn test_multiple_updates() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(t);

        q.insert(vec![1, 2, 3]);

        q.update(1, vec![1, 4, 5]);
        q.update(1, vec![1, 6, 7]);
        q.update(1, vec![1, 8, 9]);

        let vals = q.select(1, 0, vec![1, 1, 1]);
        assert_eq!(vals.unwrap(), vec![3, 0, 2, 1, 8, 9]);
    }

    #[test]
    fn test_delete_and_select() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(t);

        q.insert(vec![1, 2, 3]);
        q.delete(1);

        assert_eq!(q.select(1, 0, vec![1, 1, 1]), None);
    }

    #[test]
    fn test_select_version() {
        let mut db = RDatabase::new();
        let t = db.create_table(String::from("Grades"), 3, 0);
        let mut q = RQuery::new(t);

        // Insert initial record
        q.insert(vec![1, 2, 3]);

        // Make multiple updates
        q.update(1, vec![1, 4, 5]); // Version 1
        q.update(1, vec![1, 6, 7]); // Version 2
        q.update(1, vec![1, 8, 9]); // Version 3

        // Test different versions
        let latest = q.select_version(1, 0, vec![1, 1, 1], 0);
        assert_eq!(latest.unwrap(), vec![3, 0, 2, 1, 8, 9]); // Most recent version

        let one_back = q.select_version(1, 0, vec![1, 1, 1], 1);
        assert_eq!(one_back.unwrap(), vec![2, 1, 1, 1, 6, 7]); // One version back

        let two_back = q.select_version(1, 0, vec![1, 1, 1], 2);
        assert_eq!(two_back.unwrap(), vec![1, 1, 0, 1, 4, 5]); // Two versions back

        let original = q.select_version(1, 0, vec![1, 1, 1], 3);
        assert_eq!(original.unwrap(), vec![0, 1, 3, 1, 2, 3]); // Original version
    }
}
