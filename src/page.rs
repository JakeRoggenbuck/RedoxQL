use pyo3::prelude::*;

static MAX_SIZE_RECORD: i64 = i64::MAX;

#[pyclass]
#[derive(Clone, Debug)]
pub struct PhysicalPage {
    pub data: Vec<i64>,
    pub num_records: i64,
}

impl PhysicalPage {
    // Init
    pub fn new() -> Self {
        PhysicalPage {
            data: Vec::<i64>::new(),
            num_records: 0,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_page_test() {
        let mut phys_page = PhysicalPage::new();

        phys_page.write(10);
        assert_eq!(phys_page.read(0).unwrap(), 10);
    }

    #[test]
    fn many_writes_page_test() {
        let mut phys_page = PhysicalPage::new();

        for x in 0..1000 {
            phys_page.write(x * 10);
            assert_eq!(phys_page.read(x as usize).unwrap(), x * 10);
        }
    }
}
