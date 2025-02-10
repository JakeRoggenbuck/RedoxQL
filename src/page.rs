use pyo3::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering}; // 

static MAX_SIZE_RECORD: usize = 512;

#[pyclass]
pub struct Page {
    num_records: usize,
    data: [u8; 4096],  // 4 KB page bytearray
}

impl Page {
    // Init
    pub fn new() -> Self {
        Page {
            num_records = 0,
            data: [0; 4096],
            locked: AtomicUSize,
        }
    }

    // Returns if the page is full or not
    pub fn has_capacity(&self) -> bool {
        return self.num_records < MAX_SIZE_RECORD;
    }

    // Writes a record to the page
    // Returns Ok() or Err()
    pub fn write(&self, value: i64) -> Result<(), String> {
        // Wait until page is unlocked
        while self.locked.load(Ordering::Acquire) == 1 {
            continue;
        }
        self.locked.store(1, Ordering::Release);

        // Check if page has capacity
        if (!self.has_capacity()) {
            self.locked.store(0, Ordering::Release);
            return Err(String::from("Page is full."));
        }
        // Write record
        let position = self.num_records * 8;
        self.data[position..position + 8].copy_from_slice(&value.to_be_bytes());

        // Unlock and update state
        self.locked.store(0, Ordering::Release);
        self.num_records += 1;
        Ok(())
    }

    // Read bytes from index
    pub fn read(&self, index) -> Option<[u8; 8]> {
        // Check range
        if index >= self.num_records {
            return None;
        }
        // Read data
        let position = index * 8;
        let data = self.data[position..position + 8].try_into().unwrap();

        Some(data)
    }
}

