use super::transaction::RTransaction;
use log::debug;
use pyo3::prelude::*;
use std::collections::VecDeque;
use std::thread::{self, JoinHandle};

#[pyclass]
pub struct RTransactionWorker {
    transactions: VecDeque<RTransaction>,
    handles: Vec<JoinHandle<()>>,
}

#[pymethods]
impl RTransactionWorker {
    #[new]
    pub fn new() -> Self {
        RTransactionWorker {
            transactions: VecDeque::new(),
            handles: Vec::new(),
        }
    }

    pub fn add_transaction(&mut self, t: PyObject) {
        // TODO: Find a way to do this better
        // It might be find since adding a transaction only happens a few times
        Python::with_gil(|py| {
            // Extract just the attribute `transaction` and serialize it to RTransaction
            let transaction_attr = t.getattr(py, "transaction");

            match transaction_attr {
                Ok(t_attr) => {
                    let ts: Result<RTransaction, _> = t_attr.extract(py);

                    match ts {
                        Ok(transaction) => {
                            self.transactions.push_back(transaction);
                        }
                        Err(e) => debug!("{}", e),
                    }
                }
                Err(e) => debug!("{}", e),
            }
        })
    }

    pub fn run(&mut self) {
        debug!("Transaction Worker: Running {} transactions.", self.transactions.len());

        while let Some(mut transaction) = self.transactions.pop_front() {
            let handle = thread::spawn(move || {
                let success = transaction.run();
    
                if success {
                    debug!("Transaction committed.");
                } else {
                    debug!("Transaction aborted.");
                }
            });
    
            self.handles.push(handle);
        }
    }

    pub fn join(&mut self) {
        while self.handles.len() > 0 {
            let handle = self.handles.pop();

            if let Some(h) = handle {
                let _ = h.join();
            }
        }
    }
}
