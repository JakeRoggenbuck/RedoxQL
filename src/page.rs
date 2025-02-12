use pyo3::prelude::*;

static MAX_SIZE_RECORD: u64 = 512;

#[pyclass]
#[derive(Clone)]
pub struct PhysicalPage {
    pub data: [u64; 512],
    pub num_records: u64,
}

impl PhysicalPage {
    // Init
    pub fn new() -> Self {
        PhysicalPage {
            data: [0u64; 512],
            num_records: 0,
        }
    }

    pub fn has_capacity(&self) -> bool {
        return self.num_records < MAX_SIZE_RECORD;
    }

    pub fn write(&mut self, value: u64) {
        self.data[self.num_records as usize] = value;
        self.num_records += 1;
    }

    pub fn read(&self, index: usize) -> Option<u64> {
        Some(self.data[index])
    }
}

#[cfg(test)]
mod tests {}
