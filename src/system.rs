use std::fs::{read_dir, read_to_string};
use std::path::Path;


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

