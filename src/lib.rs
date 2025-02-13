use database::{RDatabase, RTable, Record};
use pyo3::prelude::*;
use query::RQuery;

pub mod container;
pub mod database;
pub mod page;
pub mod query;
pub mod system;

/// Blazingly fast hello
#[pyfunction]
fn hello_from_rust() -> PyResult<String> {
    Ok(String::from("Hello from Rust!"))
}

/// A Python module implemented in Rust.
#[pymodule]
fn lstore(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RDatabase>()?;
    m.add_class::<RQuery>()?;
    m.add_class::<RTable>()?;
    m.add_class::<Record>()?;
    m.add_function(wrap_pyfunction!(hello_from_rust, m)?)?;
    Ok(())
}
