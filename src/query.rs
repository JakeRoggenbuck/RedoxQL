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

    fn delete(&mut self, primary_key: u64) {
        self.table.delete(primary_key)
    }

    fn insert(&mut self, values: Vec<u64>) -> Record {
        self.table.write(values)
    }

    fn select(
        &mut self,
        primary_key: i64,
        _search_key_index: i64,
        _projected_columns_index: Vec<i64>,
    ) -> Option<Vec<u64>> {
        let Some(result) = self.table.read(primary_key as u64) else {
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
                Some(r) => return self.table.page_range.read_tail(r.clone()),
                None => return None,
            }
        }

        return Some(result);
    }

    fn select_version(&mut self) {}

    fn update(&mut self, primary_key: i64, columns: Vec<u64>) -> bool {
        let Some(rid) = self.table.index.get(primary_key as u64) else {
            return false;
        };

        let record = match self.table.page_directory.get(&rid).cloned() {
            Some(r) => r,
            None => return false,
        };

        let Some(result) = self.table.page_range.read_base(record.clone()) else {
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
                base_schema_encoding.overwrite(addrs_base[self.table.page_range.base_container.schema_encoding_column as usize].offset as usize, 1);
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
            let mut lastest_tail_schema_encoding = addrs_existing
                [self.table.page_range.base_container.schema_encoding_column as usize]
                .page
                .lock()
                .unwrap();
            lastest_tail_schema_encoding.overwrite(addrs_base[self.table.page_range.base_container.schema_encoding_column as usize].offset as usize, 1);
        }

        let new_rid = self.table.num_records;
        let new_rec = self.table
            .page_range
            .tail_container
            .insert_record(new_rid, base_indirection_column, columns);

        self.table.page_directory.insert(new_rid, new_rec.clone());

        // update the indirection column of the base record
        let mut indirection_page = addrs_base
            [self.table.page_range.base_container.indirection_column as usize]
            .page
            .lock()
            .unwrap();
        indirection_page.overwrite(addrs_base[self.table.page_range.base_container.indirection_column as usize].offset as usize, new_rid);

        self.table.num_records += 1;

        return true;
    }

    fn sum(&mut self, start_primary_key: u64, end_primary_key: u64, col_index: u64) -> i64 {
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
}
