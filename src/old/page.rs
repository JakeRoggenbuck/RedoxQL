use pyo3::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering}; // lock mechanism

static MAX_NUM_RECORD: usize = 512;

#[pyclass]
pub struct PhysicalPage {
    num_records: usize,
    data: [u64; 512],
    locked: AtomicUsize,
}

impl PhysicalPage {
    // Init
    pub fn new() -> Self {
        PhysicalPage {
            num_records: 0,
            data: [0; 4096],
            locked: AtomicUsize::new(0),
        }
    }

    // Returns if the page is full or not
    pub fn has_capacity(&self) -> bool {
        return self.num_records < MAX_NUM_RECORD;
    }

    // Writes a record to the page
    // Returns Ok() or Err()
    pub fn write(&mut self, value: i64) -> Result<(), String> {
        // Wait until page is unlocked
        while self.locked.load(Ordering::Acquire) == 1 {
            continue;
        }
        self.locked.store(1, Ordering::Release);

        // Check if page has capacity
        if !self.has_capacity() {
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
    pub fn read(&self, index: usize) -> Option<i64> {
        // Check range
        if index >= self.num_records {
            return None;
        }
        // Read data
        let position = index * 8;
        let data: [u8; 8] = self.data[position..position + 8].try_into().unwrap();
        let num: i64 = i64::from_be_bytes(data);

        Some(num)
    }
}

#[pyclass]
pub struct BaseContainer {
    base_container: Vec<PhysicalPage>,
}

#[pyclass]
pub struct TailContainer {
    tail_container: Vec<PhysicalPage>
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read() {
        let mut page = PhysicalPage::new();

        // Write a value to page
        assert!(page.write(42).is_ok());

        // Read value back and check if it matches
        if let Some(data) = page.read(0) {
            assert_eq!(data, 42);
        } else {
            panic!("Failed to read value");
        }
    }

    #[test]
    fn test_capacity_limit() {
        let mut page = PhysicalPage::new();

        // Write 512 records to the page
        for i in 0..512 {
            assert!(page.write(i as i64).is_ok());
        }

        // Next write should fail
        assert!(page.write(513).is_err());
    }

    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn test_thread_safety() {
        let page = Arc::new(Mutex::new(PhysicalPage::new()));

        let mut handles = vec![];
        for i in 0..10 {
            let page_clone = Arc::clone(&page);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    let mut p = page_clone.lock().unwrap();
                    p.write(i as i64).unwrap_or_else(|_| ());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let p = page.lock().unwrap();
        assert!(p.num_records <= 512);
    }
}
