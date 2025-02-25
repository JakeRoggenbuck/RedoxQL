use std::sync::{Arc, Mutex};

struct BufferPool {
    // The physical directory on disk that data will be written to
    pub physical_directory: Arc<Mutex<String>>,
}

impl BufferPool {
    fn new(directory: &str) -> Self {
        BufferPool {
            physical_directory: Arc::new(Mutex::new(directory.to_string())),
        }
    }

    fn write_page(page_id: usize) {
        todo!();
    }

    fn read_page(page_id: usize) {
        // TODO: How does it know the page id?
        todo!();
    }

    fn save_state(&self) {}

    fn load_state(&self, directory: &str) -> BufferPool {
        BufferPool {
            physical_directory: Arc::new(Mutex::new(directory.to_string())),
        }
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
            b.physical_directory.lock().unwrap().to_string(),
            new_b.physical_directory.lock().unwrap().to_string()
        );
    }

    #[test]
    fn bufferpool_test() {
        assert!(true);
    }
}
