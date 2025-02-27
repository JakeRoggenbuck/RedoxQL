use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::{read_to_string, File};
use std::io::{BufReader, BufWriter, Write};

pub trait WriterStrategy<T: Serialize + for<'de> Deserialize<'de>> {
    fn write_file(&self, path: &str, object: T);
    fn read_file(&self, path: &str) -> T;
}

pub struct BinaryFileWriter {}
impl<T: Serialize + for<'de> Deserialize<'de>> WriterStrategy<T> for BinaryFileWriter {
    /// Write a binary file
    fn write_file(&self, path: &str, object: T) {
        let obj_bytes: Vec<u8> = bincode::serialize(&object).expect("Should serialize.");
        let mut file = BufWriter::new(File::create(path).expect("Should open file."));
        file.write_all(&obj_bytes).expect("Should write.");
    }
    // Read a binary file
    fn read_file(&self, path: &str) -> T {
        let file = BufReader::new(File::open(path).expect("Should open file."));

        let object: T = bincode::deserialize_from(file).expect("Should deserialize.");
        return object;
    }
}

pub struct JSONFileWriter {}
impl<T: Serialize + for<'de> Deserialize<'de>> WriterStrategy<T> for JSONFileWriter {
    // Write a JSON file
    fn write_file(&self, path: &str, object: T) {
        let json = serde_json::to_string_pretty(&object).expect("Should serialize.");
        let mut file = File::create(path).expect("Should open file.");
        file.write_all(json.as_bytes()).expect("Should write.");
    }
    // Read a JSON file
    fn read_file(&self, path: &str) -> T {
        let json = read_to_string(path).expect("Should open file.");
        let object: T = serde_json::from_str(&json).expect("Should deserialize.");
        return object;
    }
}

pub struct Writer<T> {
    strategy: Box<dyn WriterStrategy<T>>,
}

/// # Writer Example
///
/// ```
/// let json_writer = JSONFileWriter::new();
/// let mut writer = Writer::new(Box::new(json_writer));
///
/// let data = MyData { value: 42 };
/// writer.write_file("out.json", data)?;
///
/// let binary_writer = BinaryFileWriter::new();
/// writer.set_strategy(Box::new(binary_writer));
///
/// let more_data = MyData { value: 100 };
/// writer.write_file("out.data", more_data)?;
/// ```
impl<T: Serialize + for<'de> Deserialize<'de>> Writer<T> {
    pub fn new(strategy: Box<dyn WriterStrategy<T>>) -> Self {
        Writer { strategy }
    }

    pub fn set_strategy(&mut self, strategy: Box<dyn WriterStrategy<T>>) {
        self.strategy = strategy;
    }

    pub fn write_file(&self, path: &str, object: T) {
        self.strategy.write_file(path, object);
    }

    pub fn read_file(&self, path: &str) {
        self.strategy.read_file(path);
    }
}
