use super::filewriter::{build_binary_writer, Writer};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

static MAX_SIZE_RECORD: i64 = i64::MAX;

#[pyclass]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PhysicalPage {
    pub data: Vec<i64>,
    pub num_records: i64,
    pub column_index: i64,
}

impl PhysicalPage {
    pub fn new(column_index: i64) -> Self {
        PhysicalPage {
            data: Vec::<i64>::new(),
            num_records: 0,
            column_index,
        }
    }

    pub fn has_capacity(&self) -> bool {
        return self.num_records < MAX_SIZE_RECORD;
    }

    pub fn write(&mut self, value: i64) {
        if self.has_capacity() {
            self.data.push(value);
            self.num_records += 1;
        }
    }

    pub fn overwrite(&mut self, index: usize, value: i64) {
        self.data[index] = value;
    }

    pub fn read(&self, index: usize) -> Option<i64> {
        Some(self.data[index])
    }

    pub fn save_state(&self, id: i64) {
        let filename = format!("./redoxdata/{}-page.data", id);

        let writer: Writer<PhysicalPage> = build_binary_writer();
        writer.write_file(&filename, self);
    }

    pub fn load_state(id: i64) -> PhysicalPage {
        let hardcoded_filename = format!("./redoxdata/{}-page.data", id);

        let file = BufReader::new(File::open(hardcoded_filename).expect("Should open file."));
        let page: PhysicalPage = bincode::deserialize_from(file).expect("Should deserialize.");

        return page;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_page_test() {
        let mut phys_page = PhysicalPage::new(0);

        phys_page.write(10);
        assert_eq!(phys_page.read(0).unwrap(), 10);
    }

    #[test]
    fn save_load_test() {
        // Scope so that page_one and page_two get unallocated and leave scope
        {
            let mut page_one = PhysicalPage::new(0);
            let mut page_two = PhysicalPage::new(1);

            // Write to page_one
            page_one.write(100);
            page_one.write(200);
            page_one.write(300);

            // Write to page_two
            page_two.write(111);
            page_two.write(222);
            page_two.write(333);

            // Save page_one and page_two
            page_one.save_state(1);
            page_two.save_state(2);
        }

        // Load page_one and page_two
        let mut page_one = PhysicalPage::load_state(1);
        let mut page_two = PhysicalPage::load_state(2);

        // Write to both pages once more
        page_one.write(400);
        page_two.write(444);

        // Check that all the data is there
        assert_eq!(page_one.read(0), Some(100));
        assert_eq!(page_one.read(1), Some(200));
        assert_eq!(page_one.read(2), Some(300));
        assert_eq!(page_one.read(3), Some(400));

        assert_eq!(page_two.read(0), Some(111));
        assert_eq!(page_two.read(1), Some(222));
        assert_eq!(page_two.read(2), Some(333));
        assert_eq!(page_two.read(3), Some(444));
    }

    #[test]
    fn many_writes_page_test() {
        let mut phys_page = PhysicalPage::new(0);

        for x in 0..1000 {
            phys_page.write(x * 10);
            assert_eq!(phys_page.read(x as usize).unwrap(), x * 10);
        }
    }
}
