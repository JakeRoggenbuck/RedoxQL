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
    #[inline(always)]
    fn write_file(&self, path: &str, object: &T) {
        let obj_bytes: Vec<u8> = bincode::serialize(&object).expect("Should serialize.");
        let mut file = BufWriter::new(File::create(path).expect("Should open file."));
        file.write_all(&obj_bytes).expect("Should write.");
    }
    /// Read a binary file
    #[inline(always)]
    fn read_file(&self, path: &str) -> T {
        let file = BufReader::new(File::open(path).expect("Should open file."));
        let object: T = bincode::deserialize_from(file).expect("Should deserialize.");
        return object;
    }
}

impl BinaryFileWriter {
    #[inline(always)]
    pub fn new() -> Self {
        BinaryFileWriter {}
    }
}

pub struct JSONFileWriter {}
impl<T: Serialize + for<'de> Deserialize<'de>> WriterStrategy<T> for JSONFileWriter {
    /// Write a JSON file
    #[inline(always)]
    fn write_file(&self, path: &str, object: &T) {
        let json = serde_json::to_string_pretty(&object).expect("Should serialize.");
        let mut file = File::create(path).expect("Should open file.");
        file.write_all(json.as_bytes()).expect("Should write.");
    }

    /// Read a JSON file
    #[inline(always)]
    fn read_file(&self, path: &str) -> T {
        let json = read_to_string(path).expect("Should open file.");
        let object: T = serde_json::from_str(&json).expect("Should deserialize.");

        return object;
    }
}

impl JSONFileWriter {
    #[inline(always)]
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
/// writer.write_file("./test-outputs/out.json", &data);
///
/// let binary_writer = BinaryFileWriter::new();
/// writer.set_strategy(Box::new(binary_writer));
///
/// let data2 = vec![1, 2, 3];
/// writer.write_file("./test-outputs/out.data", &data2);
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

/// Build a Writer<T> with a binary output format
/// This function implements the builder pattern
///
/// # Example of build_binary_writer
///
/// ```
/// use redoxql::filewriter::{build_binary_writer, Writer};
/// use redoxql::page::PhysicalPage;
///
/// // Write a data file
///
/// let page = PhysicalPage::new(0);
///
/// let writer: Writer<PhysicalPage> = build_binary_writer();
///
/// writer.write_file("./test-outputs/page.data", &page);
///
/// // Read a data file
///
/// let writer: Writer<PhysicalPage> = build_binary_writer();
///
/// let page: PhysicalPage = writer.read_file("./test-outputs/page.data");
/// ```
pub fn build_binary_writer<T: Serialize + for<'de> Deserialize<'de>>() -> Writer<T> {
    let bin_writer = BinaryFileWriter::new();
    let writer = Writer::new(Box::new(bin_writer));

    return writer;
}

/// Build a Writer<T> with a json output format
/// This function implements the builder pattern
///
/// # Example of build_json_writer
///
/// ```
/// use redoxql::filewriter::{build_json_writer, Writer};
/// use redoxql::page::PhysicalPage;
///
/// // Write a json file
///
/// let page = PhysicalPage::new(0);
///
/// let writer: Writer<PhysicalPage> = build_json_writer();
///
/// writer.write_file("./test-outputs/page.json", &page);
///
/// // Read a json file
///
/// let writer: Writer<PhysicalPage> = build_json_writer();
///
/// let page: PhysicalPage = writer.read_file("./test-outputs/page.json");
/// ```
pub fn build_json_writer<T: Serialize + for<'de> Deserialize<'de>>() -> Writer<T> {
    let json_writer = BinaryFileWriter::new();
    let writer = Writer::new(Box::new(json_writer));

    return writer;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PhysicalPage;

    #[test]
    fn build_write_and_read_json_test() {
        let mut page = PhysicalPage::new(0);
        page.write(101);
        page.write(202);
        page.write(303);

        let writer: Writer<PhysicalPage> = build_json_writer();
        writer.write_file("./test-outputs/test-page.json", &page);

        let writer: Writer<PhysicalPage> = build_json_writer();
        let page: PhysicalPage = writer.read_file("./test-outputs/test-page.json");

        assert_eq!(page.read(0), Some(101));
        assert_eq!(page.read(1), Some(202));
        assert_eq!(page.read(2), Some(303));
    }

    #[test]
    fn build_write_and_read_bit_test() {
        let mut page = PhysicalPage::new(0);
        page.write(401);
        page.write(402);
        page.write(403);

        let writer: Writer<PhysicalPage> = build_json_writer();
        writer.write_file("./test-outputs/test-page.data", &page);

        let writer: Writer<PhysicalPage> = build_json_writer();
        let page: PhysicalPage = writer.read_file("./test-outputs/test-page.data");

        assert_eq!(page.read(0), Some(401));
        assert_eq!(page.read(1), Some(402));
        assert_eq!(page.read(2), Some(403));
    }
}
