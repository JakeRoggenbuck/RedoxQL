use super::container::{
    BaseContainer, BaseContainerMetadata, TailContainer, TailContainerMetadata,
};
use super::record::Record;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct PageRangeMetadata {
    pub base_container: BaseContainerMetadata,
    pub tail_container: TailContainerMetadata,
}

#[derive(Clone, Default)]
pub struct PageRange {
    pub base_container: BaseContainer,
    pub tail_container: TailContainer,
}

impl PageRange {
    pub fn new(num_cols: i64) -> Self {
        let mut base = BaseContainer::new(num_cols);
        base.initialize();

        let mut tail = TailContainer::new(num_cols);
        tail.initialize();

        PageRange {
            base_container: base,
            tail_container: tail,
        }
    }

    /// Write an entire record of values
    pub fn write(&mut self, new_rid: i64, values: Vec<i64>) -> Record {
        self.base_container.insert_record(new_rid, values)
    }

    pub fn read(&self, record: Record) -> Option<Vec<i64>> {
        Some(self.base_container.read_record(record))
    }

    pub fn save_state(&self) {
        // Save the state of the two containers
        self.base_container.save_state();
        self.tail_container.save_state();

        let hardcoded_filename = "./redoxdata/pagerange.data";

        let pr_meta = self.get_metadata();

        let pr_bytes: Vec<u8> = bincode::serialize(&pr_meta).expect("Should serialize.");

        let mut file = BufWriter::new(File::create(hardcoded_filename).expect("Should open file."));
        file.write_all(&pr_bytes).expect("Should serialize.");
    }

    pub fn load_state() -> PageRange {
        let hardcoded_filename = "./redoxdata/pagerange.data";

        let file = BufReader::new(File::open(hardcoded_filename).expect("Should open file."));
        let pr_meta: PageRangeMetadata =
            bincode::deserialize_from(file).expect("Should deserialize.");

        PageRange {
            base_container: pr_meta.base_container.load_state(),
            tail_container: pr_meta.tail_container.load_state(),
        }
    }

    pub fn get_metadata(&self) -> PageRangeMetadata {
        PageRangeMetadata {
            base_container: self.base_container.get_metadata(),
            tail_container: self.tail_container.get_metadata(),
        }
    }
}
