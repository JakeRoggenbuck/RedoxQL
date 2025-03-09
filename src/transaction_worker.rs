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

    pub fn add_transaction(&mut self, t: PyObject) {
        // TODO: Find a way to do this better
        // It might be find since adding a transaction only happens a few times
        Python::with_gil(|py| {
            let r_transaction = t.extract(py);
            match r_transaction {
                Ok(rt) => {
                    self.transactions.push_back(rt);
                }
                Err(_) => {}
            }
        })
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
