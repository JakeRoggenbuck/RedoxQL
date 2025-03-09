use super::transaction::RTransaction;
use pyo3::prelude::*;
use std::collections::VecDeque;

#[pyclass]
pub struct RTransactionWorker {
    transactions: VecDeque<RTransaction>,
}

#[pymethods]
impl RTransactionWorker {
    #[new]
    pub fn new() -> Self {
        RTransactionWorker {
            transactions: VecDeque::new(),
        }
    }

    pub fn add_transaction(&mut self, t: RTransaction) {
        self.transactions.push_back(t);
    }

    pub fn run(&mut self) {
        // TODO: Make a new thread!

        while self.transactions.len() > 0 {
            let transaction = self.transactions.pop_front();

            // TODO: Make this specifically a new thread for run
            //
            // TODO: I am going to need to push running threads into a new vec
            // so that .join can check to see if all of the threads are done
            if let Some(mut t) = transaction {
                t.run();
            }
        }
    }

    pub fn join(&mut self) {
        // TODO: Wait until the transaction is done running
    }
}
