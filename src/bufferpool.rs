use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

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

    pub fn write_page(_page_id: usize) {
        // Figure out if the page is in memory or saved in a file
        // ?? Keep track of where the physical memory should be if it needs to read it again
        todo!();
    }

    pub fn read_page(_page_id: usize) {
        // Figure out if the page is in memory or saved in a file
        // If it's not in memory, we load it into memory (probably LRU)

        // TODO: How does it know the page id? Page ID being a name for knowing where the page is
        todo!();
    }

    pub fn save_state(&self) {
        let hardcoded_filename = "./redoxdata/bufferpull.data";

        let bufferpool_bytes: Vec<u8> = bincode::serialize(self).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&bufferpool_bytes)
            .expect("Should serialize.");
    }

    pub fn load_state(&self, directory: &str) -> BufferPool {
        let hardcoded_filename = "./redoxdata/bufferpull.data";

        let file = BufReader::new(File::open(hardcoded_filename).expect("Should open file."));

        let bufferpool: BufferPool = bincode::deserialize_from(file).expect("Should deserialize.");

        return bufferpool;
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
