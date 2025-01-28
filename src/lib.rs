use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::fs::{read_dir, read_to_string};
use std::path::Path;

// Hard code row schema - TODO: make this dynamic schema set by table or something similar
#[derive(Copy, Clone)]
#[pyclass]
struct Row {
    id: i64,
    value: i64,
}

#[pyclass]
enum RowOption {
    Empty(),
    Some(Row),
}


/// Return a list of all of the ddrives in /dev/
pub fn get_all_drives() -> Vec<String> {
    let mut drives = Vec::<String>::new();

    if let Ok(entries) = read_dir("/dev") {
        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(name) = entry.file_name().to_str() {
                    // Check if the file is a drive
                    if name.starts_with("nvme") || name.starts_with("sd") {
                        // Check if the drive exists in /sys/block
                        if Path::new(&format!("/sys/block/{}", name)).exists() {
                            drives.push(name.to_string());
                        }
                    }
                }
            }
        }
    }

    return drives;
}

/// Read the block size for the machine
fn read_block_size(drive: &str, type_of_block: &str) -> i16 {
    let path = format!("/sys/block/{}/queue/{}_block_size", drive, type_of_block);
    let mut block_size_str =
        read_to_string(path).expect("Should be able to read physical_block_size from file.");

    // Remove '\n' from end of file
    block_size_str.pop();

    // Cast the number into an i16 from the String
    let block_size = block_size_str
        .parse::<i16>()
        .expect("Should parse block size.");

    block_size
}

pub fn get_logical_block_size(drive: &str) -> i16 {
    read_block_size(drive, "logical")
}

pub fn get_physical_block_size(drive: &str) -> i16 {
    read_block_size(drive, "physical")
}

struct Page {
    rows: Vec<Row>,
}

#[pyclass]
struct Database {
    #[pyo3(get, set)]
    page_size: usize,

    pages: BTreeMap<i64, Page>,
}

#[pymethods]
impl Database {
    #[staticmethod]
    fn ping() -> String {
        return String::from("pong!");
    }

    fn insert(&mut self, id: i64, value: i64) {
        // Make a new row
        let r = Row { id, value };

        // Make a new page - TODO: look up page to add to existing page
        let p = Page { rows: vec![r] };

        self.pages.insert(id, p);
    }

    fn fetch(&mut self, id: i64) -> RowOption {
        let found = self.pages.get(&id);

        match found {
            Some(page) => {
                // Linear search through rows - TODO: Figure out how this is normally done, maybe
                // binary search?
                for row in &page.rows {
                    if row.id == id {
                        return RowOption::Some(*row);
                    }
                }

                return RowOption::Empty();
            }

            None => RowOption::Empty(),
        }
    }
}

/// Blazingly fast hello
#[pyfunction]
fn hello_from_rust() -> PyResult<String> {
    Ok(String::from("Hello from Rust!"))
}

/// A Python module implemented in Rust.
#[pymodule]
fn lstore(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<Row>()?;
    m.add_class::<RowOption>()?;
    m.add_function(wrap_pyfunction!(hello_from_rust, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_found_eq_test() {
        let mut db = Database {
            page_size: 4096,
            pages: BTreeMap::new(),
        };
        db.insert(0, 100);

        match db.fetch(0) {
            RowOption::Some(row) => assert_eq!(row.value, 100),
            RowOption::Empty() => assert!(false),
        }
    }

    #[test]
    fn insert_found_ne_test() {
        let mut db = Database {
            page_size: 4096,
            pages: BTreeMap::new(),
        };
        db.insert(0, 100);

        match db.fetch(0) {
            RowOption::Some(row) => assert_ne!(row.value, 111),
            RowOption::Empty() => assert!(false),
        }
    }

    #[test]
    fn fetch_not_found_test() {
        let mut db = Database {
            page_size: 4096,
            pages: BTreeMap::new(),
        };
        db.insert(0, 100);

        // Fetch the wrong index and assert true if it's Empty
        match db.fetch(1) {
            RowOption::Some(_) => assert!(false),
            RowOption::Empty() => assert!(true),
        }
    }

    #[test]
    fn get_logical_block_size_test() {
        let all_drives = get_all_drives();
        let drive = all_drives.first().expect("There should be one drive");

        assert!(vec![512, 1024, 4096, 8192, 16384].contains(&get_physical_block_size(drive)));
        assert!(vec![512, 1024, 4096, 8192, 16384].contains(&get_logical_block_size(drive)));
    }

    #[test]
    fn get_all_drives_test() {
        assert!(get_all_drives().len() != 0);
    }
}
