use super::query::RQuery
use crate::table::RTable;
use crate::transaction::{self, RTransaction};
use pyo3::prelude::*;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Default)]

#[pyclass]

pub struct RTransactionWorker {
    stats: Vec<bool>,
    transactions: Vec<RTransaction>,
    result: i32,
}

impl RTransactionWorker {

    pub fn new() -> Self {
        RTransactionWorker {
            stats: Vec::new(),
            transactions: Vec::new(),
            result: 0,
        }
    }

    pub fn add_transaction(&mut self, transaction: RTransaction) {
        self.transactions.push(transaction);
    }

    pub fn join() {

    }

    pub fn run(&mut self) {
        
    }

}