use super::query::RQuery;
use crate::table::RTable;
use crate::transaction::{self, RTransaction};
use pyo3::prelude::*;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Default)]

#[pyclass]

pub struct RTransactionWorker {
    stats: Vec<bool>, // Needed for python test?
    transactions: Mutex<Vec<RTransaction>>,
    result: i64,
}

impl RTransactionWorker {

    pub fn new() -> Self {
        RTransactionWorker {
            stats: Vec::new(),
            transactions: Mutex::new(Vec::new()),
            result: 0,
        }
    }

    pub fn add_transaction(&mut self, transaction: RTransaction) {
        let mut transactions = self.transactions.lock().unwrap();
        transactions.push(transaction);
    }

    // Not sure if we need
    pub fn join() {

    }

    pub fn run(&mut self) {
        let mut transactions = self.transactions.lock().unwrap();
        for transaction in transactions.iter_mut() {
            transaction.run();
        }
    }

}