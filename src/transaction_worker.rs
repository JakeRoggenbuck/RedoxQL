use super::transaction::RTransaction;
use log::debug;
use pyo3::prelude::*;
use std::collections::VecDeque;
use std::thread;

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
        // TODO: Make a new thread!
        debug!(
            "Started run on transaction_worker! {}",
            self.transactions.len()
        );

        while self.transactions.len() > 0 {
            let transaction = self.transactions.pop_front();

            // TODO: Limit threads to total_threads - 1
            thread::spawn(|| {
                // TODO: Make this specifically a new thread for run
                //
                // TODO: I am going to need to push running threads into a new vec
                // so that .join can check to see if all of the threads are done
                if let Some(mut t) = transaction {
                    t.run();
                }
            });
        }
    }

    pub fn join(&mut self) {
        // TODO: Wait until the transaction is done running
    }
}
