use super::filewriter::{build_binary_writer, Writer};
use super::page::PhysicalPage;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Deserialize, Serialize, Debug)]
pub struct BufferPool {
    // The physical directory on disk that data will be written to
    pub physical_directory: String,
}

impl BufferPool {
    pub fn new(directory: &str) -> Self {
        BufferPool {
            physical_directory: directory.to_string(),
        }
    }

    pub fn write_page(page: Arc<Mutex<PhysicalPage>>, value: i64) {
        let mut m = page.lock().unwrap();
        m.write(value);
    }

    pub fn read_page(page: Arc<Mutex<PhysicalPage>>, offset: i64) -> Option<i64> {
        let m = page.lock().unwrap();
        return m.read(offset as usize);
    }

    pub fn save_state(&self) {
        let writer: Writer<BufferPool> = build_binary_writer();
        writer.write_file("./redoxdata/bufferpull.data", self);
    }

    pub fn load_state(&self, _directory: &str) -> BufferPool {
        let writer: Writer<BufferPool> = build_binary_writer();
        writer.read_file("./redoxdata/bufferpull.data")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_and_save_test() {
        let b = BufferPool::new("/data");

        b.save_state();

        let new_b = b.load_state("/data");

        assert_eq!(
            b.physical_directory.to_string(),
            new_b.physical_directory.to_string()
        );
    }

    #[test]
    fn bufferpool_test() {
        assert!(true);
    }
}
