use pyo3::prelude::*;

#[pyclass]
pub struct Page {
    num_records: usize,
    data: [u8; 4096],  // 4 KB page bytearray
}

impl Page {
    pub fn new() -> Self {
        Page {
            num_records = 0,
            data: [0; 4096],
        }
    }
    
}
