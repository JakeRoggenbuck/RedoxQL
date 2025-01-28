use database::{Database, Row, RowOption};
use pyo3::prelude::*;

pub mod database;
pub mod system;

/// Blazingly fast hello
#[pyfunction]
fn hello_from_rust() -> PyResult<String> {
    Ok(String::from("Hello from Rust!"))
}

/// A Python module implemented in Rust.
#[pymodule]
fn lstore(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<Row>()?;
    m.add_class::<RowOption>()?;
    m.add_function(wrap_pyfunction!(hello_from_rust, m)?)?;
    Ok(())
}
