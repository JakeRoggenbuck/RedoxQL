use super::container::{
    BaseContainer, BaseContainerMetadata, TailContainer, TailContainerMetadata,
};
use super::filewriter::{build_binary_writer, Writer};
use super::record::Record;
use crate::record::RecordAddress;
use crate::table::PageDirectory;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;

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

    pub fn merge(&mut self, page_directory: Arc<Mutex<PageDirectory>>) {
        // println!("Merge: Pre-starting merge operation in a separate thread");

        let base_container = self.base_container.clone();
        let tail_container = self.tail_container.clone();
        let thread_pd = page_directory.clone();

        // println!("Merge: Starting merge operation in a separate thread");

        let handle = thread::spawn(move || {
            // println!("Thread: Merge thread started");
            let mut new_records: Vec<Record> = Vec::new();
            let mut seen_rids: HashSet<i64> = HashSet::new();

            // println!("Thread: Locking tail_container.rid_page()");
            let tail_rid_page = tail_container.rid_page();
            let tail_rid_data = {
                let rid_guard = tail_rid_page.lock().unwrap();
                // println!("Thread: Acquired tail_container.rid_page lock, data length: {}", rid_guard.data.len());
                let data = rid_guard.data.clone();
                data
            };

            if tail_rid_data.is_empty() {
                // println!("Thread: tail_rid_data is empty, returning early");
                return (base_container, Vec::new());
            }

            let mut tail_rids_to_process = tail_rid_data;
            tail_rids_to_process.reverse();
            // println!("Thread: Reversed tail_rids_to_process");

            let mut new_base = base_container.deep_copy();
            let last_tail_rid = tail_rids_to_process[0];

            for tail_rid in tail_rids_to_process {
                // println!("Thread: Processing tail_rid: {}", tail_rid);

                // Check if we've seen all rids using new_base's RID page.
                {
                    let new_base_rid_page = new_base.rid_page();
                    let rid_guard = new_base_rid_page.lock().unwrap();
                    // println!("Thread: new_base.rid_page num_records: {}", rid_guard.num_records);
                    if seen_rids.len() >= rid_guard.num_records as usize {
                        // println!("Thread: Seen all rids, breaking loop");
                        break;
                    }
                }

                // println!("Thread: Locking page_directory to get tail_record for tail_rid: {}", tail_rid);
                let tail_record = {
                    let pd_guard = thread_pd.lock().unwrap();
                    // println!("Thread: Acquired page_directory lock for tail_rid: {}", tail_rid);
                    pd_guard.directory.get(&tail_rid).unwrap().clone()
                };
                // println!("Thread: Got tail_record for tail_rid: {}", tail_rid);

                let base_rid_address = tail_record.base_rid();
                let base_rid = {
                    // println!("Thread: Locking base_rid_address.page for base_rid");
                    let base_rid_page = base_rid_address.page.clone();
                    let page_guard = base_rid_page.lock().unwrap();
                    let brid = page_guard.data[base_rid_address.offset as usize];
                    // println!("Thread: Retrieved base_rid: {}", brid);
                    brid
                };

                if !seen_rids.contains(&base_rid) {
                    let offset = new_base.find_rid_offset(base_rid);
                    // println!("Thread: Found offset {} for base_rid: {}", offset, base_rid);

                    {
                        // println!("Thread: Locking new_base.schema_encoding_page");
                        let schema_page = new_base.schema_encoding_page();
                        let mut schema_guard = schema_page.lock().unwrap();
                        schema_guard.data[offset] = 0;
                        // println!("Thread: Updated schema_encoding_page at offset {}", offset);
                    }
                    {
                        // println!("Thread: Locking new_base.indirection_page");
                        let indirection_page = new_base.indirection_page();
                        let mut indir_guard = indirection_page.lock().unwrap();
                        indir_guard.data[offset] = base_rid;
                        // println!("Thread: Updated indirection_page at offset {} with base_rid {}", offset, base_rid);
                    }

                    // println!("Thread: Creating new record for base_rid: {}", base_rid);
                    let new_record = Record {
                        rid: base_rid,
                        addresses: Arc::new(Mutex::new(Vec::new())),
                    };

                    {
                        let new_rid_page = new_base.rid_page();
                        let mut addr_guard = new_record.addresses.lock().unwrap();
                        addr_guard.push(RecordAddress {
                            page: new_rid_page,
                            offset: offset as i64,
                        });
                        // println!("Thread: Pushed RID address for record {}", base_rid);
                    }
                    {
                        let schema_page = new_base.schema_encoding_page();
                        let mut addr_guard = new_record.addresses.lock().unwrap();
                        addr_guard.push(RecordAddress {
                            page: schema_page,
                            offset: offset as i64,
                        });
                        // println!("Thread: Pushed schema_encoding address for record {}", base_rid);
                    }
                    {
                        let indirection_page = new_base.indirection_page();
                        let mut addr_guard = new_record.addresses.lock().unwrap();
                        addr_guard.push(RecordAddress {
                            page: indirection_page,
                            offset: offset as i64,
                        });
                        // println!("Thread: Pushed indirection address for record {}", base_rid);
                    }
                    {
                        let base_rid_page = new_base.base_rid_page();
                        let mut addr_guard = new_record.addresses.lock().unwrap();
                        addr_guard.push(RecordAddress {
                            page: base_rid_page,
                            offset: offset as i64,
                        });
                        // println!("Thread: Pushed base_rid address for record {}", base_rid);
                    }

                    for i in 0..tail_record.columns().len() {
                        // println!("Thread: Processing column {} for tail_record with base_rid: {}", i, base_rid);
                        let tail_col_page = tail_record.columns()[i].page.clone();
                        let col_value = {
                            let col_guard = tail_col_page.lock().unwrap();
                            let val = col_guard.data[tail_record.columns()[i].offset as usize];
                            // println!("Thread: Got column value {} for column {}", val, i);
                            val
                        };

                        {
                            let new_col_page = new_base.column_page(i as i64);
                            let mut new_col_guard = new_col_page.lock().unwrap();
                            new_col_guard.data[offset] = col_value;
                            // println!("Thread: Updated new_base column {} at offset {} with value {}", i, offset, col_value);
                        }
                        {
                            let new_col_page = new_base.column_page(i as i64);
                            let mut addr_guard = new_record.addresses.lock().unwrap();
                            addr_guard.push(RecordAddress {
                                page: new_col_page,
                                offset: offset as i64,
                            });
                            // println!("Thread: Pushed column address for column {} for record {}", i, base_rid);
                        }
                    }

                    new_records.push(new_record);
                    // println!("Thread: New record for base_rid {} added", base_rid);
                    seen_rids.insert(base_rid);
                }
            }
            new_base.tail_page_sequence = last_tail_rid;
            // println!("Thread: Set new_base.tail_page_sequence to {}", last_tail_rid);
            // println!("Thread: Merge thread complete, returning new_base and new_records");
            (new_base, new_records)
        });

        let (new_base_container, new_records) = handle.join().unwrap();
        // println!("Main: Merge thread joined successfully");

        self.base_container = new_base_container;
        // println!("Main: Updated self.base_container");

        for record in new_records {
            // println!("Main: Processing record with rid: {}", record.rid);
            let mut pd_guard = page_directory.lock().unwrap();
            let current_record = pd_guard.directory.get(&record.rid).unwrap().clone();

            let current_indir_val = {
                // println!("Main: Locking current_record.indirection().page for record {}", record.rid);
                let indirection_page = current_record.indirection().page.clone();
                let indir_guard = indirection_page.lock().unwrap();
                let val = indir_guard.data[current_record.indirection().offset as usize];
                // println!("Main: Current indirection value for record {} is {}", record.rid, val);
                val
            };

            if current_indir_val > self.base_container.tail_page_sequence {
                // println!("Main: Updating record {} indirection with value {}", record.rid, current_indir_val);
                let record_indirection_page = record.indirection().page.clone();
                let mut rec_indir_guard = record_indirection_page.lock().unwrap();
                rec_indir_guard.data[record.indirection().offset as usize] = current_indir_val;
            }
            let rid = record.rid;
            pd_guard.directory.insert(rid, record);
            // println!("Main: Inserted record {} into page_directory", rid);
        }
    }

    pub fn optimized_merge(&mut self, page_directory: Arc<Mutex<PageDirectory>>) {
        info!("Starting merge!");

        // println!("Merge: Pre-starting merge operation in a separate thread");

        let base_container = self.base_container.clone();
        let tail_container = self.tail_container.clone();

        info!("Finished first clone of base_container and tail_container.");

        let thread_pd = page_directory.clone();

        // println!("Merge: Starting merge operation in a separate thread");

        let handle = thread::spawn(move || {
            // println!("Thread: Merge thread started");
            let mut new_records: Vec<Record> = Vec::new();
            let mut seen_rids: HashSet<i64> = HashSet::new();

            // println!("Thread: Locking tail_container.rid_page()");
            let tail_rid_page = tail_container.rid_page();
            let tail_rid_data = {
                let rid_guard = tail_rid_page.lock().unwrap();
                // println!("Thread: Acquired tail_container.rid_page lock, data length: {}", rid_guard.data.len());
                let data = rid_guard.data.clone();
                data
            };

            if tail_rid_data.is_empty() {
                // println!("Thread: tail_rid_data is empty, returning early");
                return (base_container, Vec::new());
            }

            let mut tail_rids_to_process = tail_rid_data;
            tail_rids_to_process.reverse();
            // println!("Thread: Reversed tail_rids_to_process");

            let mut new_base = base_container.clone();
            let last_tail_rid = tail_rids_to_process[0];

            info!(
                "Starting loop over tail_rids_to_process with len = {}.",
                tail_rids_to_process.len()
            );

            for tail_rid in tail_rids_to_process {
                // println!("Thread: Processing tail_rid: {}", tail_rid);

                // Check if we've seen all rids using new_base's RID page.
                {
                    let new_base_rid_page = new_base.rid_page();
                    let rid_guard = new_base_rid_page.lock().unwrap();
                    // println!("Thread: new_base.rid_page num_records: {}", rid_guard.num_records);
                    if seen_rids.len() >= rid_guard.num_records as usize {
                        // println!("Thread: Seen all rids, breaking loop");
                        break;
                    }
                }

                // println!("Thread: Locking page_directory to get tail_record for tail_rid: {}", tail_rid);
                let tail_record = {
                    let pd_guard = thread_pd.lock().unwrap();
                    // println!("Thread: Acquired page_directory lock for tail_rid: {}", tail_rid);
                    pd_guard.directory.get(&tail_rid).unwrap().clone()
                };
                // println!("Thread: Got tail_record for tail_rid: {}", tail_rid);

                let base_rid_address = tail_record.base_rid();
                let base_rid = {
                    // println!("Thread: Locking base_rid_address.page for base_rid");
                    let base_rid_page = base_rid_address.page;
                    let page_guard = base_rid_page.lock().unwrap();
                    let brid = page_guard.data[base_rid_address.offset as usize];
                    // println!("Thread: Retrieved base_rid: {}", brid);
                    brid
                };

                if !seen_rids.contains(&base_rid) {
                    let offset = new_base.find_rid_offset(base_rid);
                    // println!("Thread: Found offset {} for base_rid: {}", offset, base_rid);

                    {
                        // println!("Thread: Locking new_base.schema_encoding_page");
                        let schema_page = new_base.schema_encoding_page();
                        let mut schema_guard = schema_page.lock().unwrap();
                        schema_guard.data[offset] = 0;
                        // println!("Thread: Updated schema_encoding_page at offset {}", offset);
                    }
                    {
                        // println!("Thread: Locking new_base.indirection_page");
                        let indirection_page = new_base.indirection_page();
                        let mut indir_guard = indirection_page.lock().unwrap();
                        indir_guard.data[offset] = base_rid;
                        // println!("Thread: Updated indirection_page at offset {} with base_rid {}", offset, base_rid);
                    }

                    // println!("Thread: Creating new record for base_rid: {}", base_rid);
                    let new_record = Record {
                        rid: base_rid,
                        addresses: Arc::new(Mutex::new(Vec::new())),
                    };

                    {
                        let new_rid_page = new_base.rid_page();
                        let mut addr_guard = new_record.addresses.lock().unwrap();
                        addr_guard.push(RecordAddress {
                            page: new_rid_page,
                            offset: offset as i64,
                        });
                        // println!("Thread: Pushed RID address for record {}", base_rid);
                    }
                    {
                        let schema_page = new_base.schema_encoding_page();
                        let mut addr_guard = new_record.addresses.lock().unwrap();
                        addr_guard.push(RecordAddress {
                            page: schema_page,
                            offset: offset as i64,
                        });
                        // println!("Thread: Pushed schema_encoding address for record {}", base_rid);
                    }
                    {
                        let indirection_page = new_base.indirection_page();
                        let mut addr_guard = new_record.addresses.lock().unwrap();
                        addr_guard.push(RecordAddress {
                            page: indirection_page,
                            offset: offset as i64,
                        });
                        // println!("Thread: Pushed indirection address for record {}", base_rid);
                    }
                    {
                        let base_rid_page = new_base.base_rid_page();
                        let mut addr_guard = new_record.addresses.lock().unwrap();
                        addr_guard.push(RecordAddress {
                            page: base_rid_page,
                            offset: offset as i64,
                        });
                        // println!("Thread: Pushed base_rid address for record {}", base_rid);
                    }

                    for i in 0..tail_record.columns().len() {
                        // println!("Thread: Processing column {} for tail_record with base_rid: {}", i, base_rid);
                        let tail_col_page = &tail_record.columns()[i].page;
                        let col_value = {
                            let col_guard = tail_col_page.lock().unwrap();
                            let val = col_guard.data[tail_record.columns()[i].offset as usize];
                            // println!("Thread: Got column value {} for column {}", val, i);
                            val
                        };

                        {
                            let new_col_page = new_base.column_page(i as i64);
                            let mut new_col_guard = new_col_page.lock().unwrap();
                            new_col_guard.data[offset] = col_value;
                            // println!("Thread: Updated new_base column {} at offset {} with value {}", i, offset, col_value);
                        }
                        {
                            let new_col_page = new_base.column_page(i as i64);
                            let mut addr_guard = new_record.addresses.lock().unwrap();
                            addr_guard.push(RecordAddress {
                                page: new_col_page,
                                offset: offset as i64,
                            });
                            // println!("Thread: Pushed column address for column {} for record {}", i, base_rid);
                        }
                    }

                    new_records.push(new_record);
                    // println!("Thread: New record for base_rid {} added", base_rid);
                    seen_rids.insert(base_rid);
                }
            }
            new_base.tail_page_sequence = last_tail_rid;
            // println!("Thread: Set new_base.tail_page_sequence to {}", last_tail_rid);
            // println!("Thread: Merge thread complete, returning new_base and new_records");
            (new_base, new_records)
        });

        let (new_base_container, new_records) = handle.join().unwrap();
        // println!("Main: Merge thread joined successfully");

        // TODO: ~~WOW! The code gets to here really really quickly~~
        // ~~The speed issue is below this section~~
        // Unfortunately, I was wrong, and I didn't see the handle.join() that waits until the
        // above loop is done. If the code in the thread is the problem, I wonder if we can use 1
        // thread per column? This would definitely speed things up if we can.
        info!("Finished first loop section.");

        self.base_container = new_base_container;
        // println!("Main: Updated self.base_container");

        let mut pd_guard = page_directory.lock().unwrap();

        info!("About to get from and insert into page_directory {} times. Which are both O(log n). As well as reading and writing O(n) pages.", new_records.len());

        for record in new_records {
            // println!("Main: Processing record with rid: {}", record.rid);
            let current_record = pd_guard.directory.get(&record.rid).unwrap();

            let current_indir_val = {
                // println!("Main: Locking current_record.indirection().page for record {}", record.rid);
                let indirection_page = current_record.indirection().page;
                let indir_guard = indirection_page.lock().unwrap();
                let val = indir_guard.data[current_record.indirection().offset as usize];
                // println!("Main: Current indirection value for record {} is {}", record.rid, val);
                val
            };

            if current_indir_val > self.base_container.tail_page_sequence {
                // println!("Main: Updating record {} indirection with value {}", record.rid, current_indir_val);
                let record_indirection_page = record.indirection().page;
                let mut rec_indir_guard = record_indirection_page.lock().unwrap();
                rec_indir_guard.data[record.indirection().offset as usize] = current_indir_val;
            }
            let rid = record.rid;
            pd_guard.directory.insert(rid, record);
            // println!("Main: Inserted record {} into page_directory", rid);
        }

        info!("Merge finished!");
    }

    pub fn save_state(&self) {
        // Save the state of the two containers
        self.base_container.save_state();
        self.tail_container.save_state();

        let pr_meta = self.get_metadata();

        let writer: Writer<PageRangeMetadata> = build_binary_writer();
        writer.write_file("./redoxdata/pagerange.data", &pr_meta);
    }

    pub fn load_state() -> PageRange {
        let writer: Writer<PageRangeMetadata> = build_binary_writer();
        let pr_meta: PageRangeMetadata = writer.read_file("./redoxdata/pagerange.data");

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
