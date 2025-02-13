use pyo3::prelude::*;

static MAX_SIZE_RECORD: u64 = u64::MAX;

#[pyclass]
#[derive(Clone, Debug)]
pub struct PhysicalPage {
    pub data: Vec<u64>,
    pub num_records: u64,
}

impl PhysicalPage {
    // Init
    pub fn new() -> Self {
        PhysicalPage {
            data: Vec::<u64>::new(),
            num_records: 0,
        }
    }

    pub fn has_capacity(&self) -> bool {
        return self.num_records < MAX_SIZE_RECORD;
    }

    pub fn write(&mut self, value: u64) {
        if self.has_capacity() {
            self.data.push(value);
            self.num_records += 1;
        }
    }

    pub fn read(&self, index: usize) -> Option<u64> {
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
