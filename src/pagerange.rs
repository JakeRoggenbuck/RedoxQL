use crate::record::RecordAddress;
use crate::table::PageDirectory;

use super::container::{
    BaseContainer, BaseContainerMetadata, TailContainer, TailContainerMetadata,
};
use super::record::Record;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

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

    /// Merge the two containers in a separate thread
    pub fn merge(&mut self, page_directory: Arc<Mutex<PageDirectory>>) {
        let base_container = self.base_container.clone();
        let tail_container = self.tail_container.clone();
        let thread_pd = page_directory.clone();

        let handle = thread::spawn(move || {
            let mut new_records: Vec<Record> = Vec::new();
            let mut seen_rids: HashSet<i64> = HashSet::new();
            let mut tail_rids_to_process: Vec<i64> = tail_container
                .rid_page()
                .lock()
                .unwrap()
                .clone()
                .data
                .clone();

            if tail_rids_to_process.len() == 0 {
                return (base_container, Vec::new());
            }

            tail_rids_to_process.reverse();

            let mut new_base = base_container.deep_copy();
            let last_tail_rid = tail_rids_to_process[0];

            for tail_rid in tail_rids_to_process {
                // If we've seen all the rids, break
                if seen_rids.len() >= new_base.rid_page().lock().unwrap().num_records as usize {
                    break;
                }

                let pd_guard = thread_pd.lock().unwrap();
                let tail_record = pd_guard.directory.get(&tail_rid).unwrap();
                let base_rid_address = tail_record.base_rid();
                let base_rid =
                    base_rid_address.page.lock().unwrap().data[base_rid_address.offset as usize];

                if !seen_rids.contains(&base_rid) {
                    // find how much the rid is offsetted by
                    let offset = new_base.find_rid_offset(base_rid);

                    // update the new_base with the tail record data
                    new_base.schema_encoding_page().lock().unwrap().data[offset] = 0;
                    new_base.indirection_page().lock().unwrap().data[offset] = base_rid;

                    let new_record = Record {
                        rid: base_rid,
                        addresses: Arc::new(Mutex::new(Vec::new())),
                    };

                    new_record.addresses.lock().unwrap().push(RecordAddress {
                        page: new_base.rid_page(),
                        offset: offset as i64,
                    });

                    new_record.addresses.lock().unwrap().push(RecordAddress {
                        page: new_base.schema_encoding_page(),
                        offset: offset as i64,
                    });

                    new_record.addresses.lock().unwrap().push(RecordAddress {
                        page: new_base.indirection_page(),
                        offset: offset as i64,
                    });

                    new_record.addresses.lock().unwrap().push(RecordAddress {
                        page: new_base.base_rid_page(),
                        offset: offset as i64,
                    });

                    for i in 0..tail_record.columns().len() {
                        new_base.column_page(i as i64)
                            .lock()
                            .unwrap()
                            .data[offset] = tail_record.columns()[i]
                            .page
                            .lock()
                            .unwrap()
                            .data[tail_record.columns()[i].offset as usize];
                        
                        new_record.addresses.lock().unwrap().push(RecordAddress {
                            page: new_base.column_page(i as i64),
                            offset: offset as i64,
                        });
                    }
                    
                    new_records.push(new_record);
                    seen_rids.insert(base_rid);
                }
            }
            new_base.tail_page_sequence = last_tail_rid;

            (new_base, new_records)
        });

        let (new_base_container, new_records) = handle.join().unwrap();

        self.base_container = new_base_container;

        for record in new_records {
            let mut pd_guard = page_directory.lock().unwrap();
            let current_record = pd_guard.directory.get(&record.rid).unwrap();
            if current_record.indirection().page.lock().unwrap().data[current_record.indirection().offset as usize] > self.base_container.tail_page_sequence {
                record.indirection().page.lock().unwrap().data[record.indirection().offset as usize] = current_record.indirection().page.lock().unwrap().data[current_record.indirection().offset as usize];
            }
            pd_guard.directory.insert(record.rid, record);
        }
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
