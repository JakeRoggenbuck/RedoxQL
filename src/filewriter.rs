use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::{read_to_string, File};
use std::io::{BufReader, BufWriter, Write};

pub trait WriterStrategy<T: Serialize + for<'de> Deserialize<'de>> {
    fn write_file(&self, path: &str, object: &T);
    fn read_file(&self, path: &str) -> T;
}

pub struct BinaryFileWriter {}
impl<T: Serialize + for<'de> Deserialize<'de>> WriterStrategy<T> for BinaryFileWriter {
    /// Write a binary file
    fn write_file(&self, path: &str, object: &T) {
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

impl BinaryFileWriter {
    pub fn new() -> Self {
        BinaryFileWriter {}
    }
}

pub struct JSONFileWriter {}
impl<T: Serialize + for<'de> Deserialize<'de>> WriterStrategy<T> for JSONFileWriter {
    // Write a JSON file
    fn write_file(&self, path: &str, object: &T) {
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

impl JSONFileWriter {
    pub fn new() -> Self {
        JSONFileWriter {}
    }
}

/// The point of this Writer is so that you can switch what type of file you want to write as
/// using. It implements the Strategy pattern allowing you to pass in JSONFileWriter or BinaryFileWriter.
/// The use of BinaryFileWriter is optimal for having small and fast files for the database and
/// having JSONFileWriter is great for debugging file writes because it is human readable.
pub struct Writer<T> {
    strategy: Box<dyn WriterStrategy<T>>,
}

/// # Writer Example
///
/// ```
/// use redoxql::filewriter::{JSONFileWriter, BinaryFileWriter, Writer};
///
/// let json_writer = JSONFileWriter::new();
/// let mut writer = Writer::new(Box::new(json_writer));
///
/// let data = vec![1, 2, 3];
/// writer.write_file("out.json", &data);
///
/// let binary_writer = BinaryFileWriter::new();
/// writer.set_strategy(Box::new(binary_writer));
///
/// let data2 = vec![1, 2, 3];
/// writer.write_file("out.data", &data2);
/// ```
impl<T: Serialize + for<'de> Deserialize<'de>> Writer<T> {
    pub fn new(strategy: Box<dyn WriterStrategy<T>>) -> Self {
        Writer { strategy }
    }

    pub fn set_strategy(&mut self, strategy: Box<dyn WriterStrategy<T>>) {
        self.strategy = strategy;
    }

    pub fn write_file(&self, path: &str, object: &T) {
        self.strategy.write_file(path, object);
    }

    pub fn read_file(&self, path: &str) -> T {
        self.strategy.read_file(path)
    }
}

pub fn build_binary_writer<T: Serialize + for<'de> Deserialize<'de>>() -> Writer<T> {
    let bin_writer = BinaryFileWriter::new();
    let writer = Writer::new(Box::new(bin_writer));

    return writer;
}

pub fn build_json_writer<T: Serialize + for<'de> Deserialize<'de>>() -> Writer<T> {
    let json_writer = BinaryFileWriter::new();
    let writer = Writer::new(Box::new(json_writer));

    return writer;
}
