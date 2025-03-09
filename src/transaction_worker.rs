use super::transaction::RTransaction;
use pyo3::prelude::*;

#[pyclass]
pub struct RTransactionWorker {
    transactions: Vec<RTransaction>,
}

#[pymethods]
impl RTransactionWorker {
    #[new]
    pub fn new() -> Self {
        RTransactionWorker {
            transactions: Vec::new(),
        }
    }

    pub fn add_transaction(&mut self, t: RTransaction) {
        self.transactions.push(t);
    }

    pub fn run(&mut self) {
        // TODO: Make a new thread!
    }

    pub fn join(&mut self) {
        // TODO: Wait until the transaction is done running
    }
}
