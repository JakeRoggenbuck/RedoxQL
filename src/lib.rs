use database::{RDatabase};
use pyo3::prelude::*;
use query::Query;

pub mod database;
pub mod index;
pub mod record;
pub mod page;
pub mod query;
pub mod system;
pub mod table;

/// Blazingly fast hello
#[pyfunction]
fn hello_from_rust() -> PyResult<String> {
    Ok(String::from("Hello from Rust!"))
}

/// A Python module implemented in Rust.
#[pymodule]
fn lstore(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RDatabase>()?;
    m.add_class::<Query>()?;
    m.add_class::<RTable>()?;
    m.add_function(wrap_pyfunction!(hello_from_rust, m)?)?;
    Ok(())
}
