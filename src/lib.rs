use database::RDatabase;
use pyo3::prelude::*;
use query::RQuery;
use record::Record;
use table::{RTable, RTableHandle};
use transaction::RTransaction;

pub mod bufferpool;
pub mod container;
pub mod database;
pub mod filewriter;
pub mod index;
pub mod page;
pub mod pagerange;
pub mod query;
pub mod record;
pub mod system;
pub mod table;
pub mod transaction;
pub mod transaction_worker;

/// Blazingly fast hello
#[pyfunction]
fn hello_from_rust() -> PyResult<String> {
    Ok(String::from("Hello from Rust!"))
}

/// A Python module implemented in Rust.
#[pymodule]
fn lstore(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RDatabase>()?;
    m.add_class::<RTransaction>()?;
    m.add_class::<RQuery>()?;
    m.add_class::<RTable>()?;
    m.add_class::<Record>()?;
    m.add_class::<RTableHandle>()?;
    m.add_function(wrap_pyfunction!(hello_from_rust, m)?)?;
    Ok(())
}
