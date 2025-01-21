use pyo3::prelude::*;
use std::fs::{read_dir, read_to_string};

const DRIVE: &'static str = "nvme0n1";

/// Return a list of all of the ddrives in /dev/
pub fn get_all_drives() -> Vec<String> {
    let mut drives = Vec::<String>::new();

    if let Ok(entries) = read_dir("/dev") {
        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(name) = entry.file_name().to_str() {
                    drives.push(name.to_string());
                }
            }
        }
    }

    return drives;
}


/// Read the block size for the machine
fn read_block_size(type_of_block: &str) -> i16 {
    let path = format!("/sys/block/{}/queue/{}_block_size", DRIVE, type_of_block);
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

pub fn get_logical_block_size() -> i16 {
    read_block_size("logical")
}

pub fn get_physical_block_size() -> i16 {
    read_block_size("physical")
}

/// Blazingly fast hello
#[pyfunction]
fn hello_from_rust() -> PyResult<String> {
    Ok(String::from("Hello from Rust!"))
}

/// A Python module implemented in Rust.
#[pymodule]
fn lstore(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_from_rust, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_logical_block_size_test() {
        assert_eq!(get_logical_block_size(), 512);
        assert_eq!(get_physical_block_size(), 512);
    }

    #[test]
    fn get_all_drives_test() {
        assert!(get_all_drives().len() != 0);
    }
}
