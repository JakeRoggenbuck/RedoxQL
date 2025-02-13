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
        self.table.read(primary_key as u64)
    }

    fn select_version(&mut self) {}

    fn update(&mut self, primary_key: i64, columns: Vec<i64>) -> bool {
        let Some(rid) = self.table.index.get(primary_key as u64) else {
            return false;
        };

        let Some(record) = self.table.page_directory.get(&rid) else {
            return false;
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
                base_schema_encoding.write(1);
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
            lastest_tail_schema_encoding.write(1);
        }

        let new_rid = self.table.num_records;
        let mut values = vec![new_rid, 0, base_indirection_column];
        values.extend(columns.iter().map(|&x| x as u64));
        self.table
            .page_range
            .tail_container
            .insert_record(new_rid, values);

        // update the indirection column of the base record
        let mut indirection_base = addrs_base
            [self.table.page_range.base_container.indirection_column as usize]
            .page
            .lock()
            .unwrap();
        indirection_base.write(new_rid);

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
        assert_eq!(vals2.unwrap(), vec![0, 0, 0, 1, 5, 6]);
    }
}
