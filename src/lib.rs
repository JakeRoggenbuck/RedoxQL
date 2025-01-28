use pyo3::prelude::*;
use std::collections::BTreeMap;

// Hard code row schema - TODO: make this dynamic schema set by table or something similar
#[derive(Copy, Clone)]
#[pyclass]
struct Row {
    id: i64,
    value: i64,
}

#[pyclass]
enum RowOption {
    Empty(),
    Some(Row),
}

struct Page {
    rows: Vec<Row>,
}

#[pyclass]
struct Database {
    #[pyo3(get, set)]
    page_size: usize,

    pages: BTreeMap<i64, Page>,
}

#[pymethods]
impl Database {
    #[staticmethod]
    fn ping() -> String {
        return String::from("pong!");
    }

    fn insert(&mut self, id: i64, value: i64) {
        // Make a new row
        let r = Row { id, value };

        // Make a new page - TODO: look up page to add to existing page
        let p = Page { rows: vec![r] };

        self.pages.insert(id, p);
    }

    fn fetch(&mut self, id: i64) -> RowOption {
        let found = self.pages.get(&id);

        match found {
            Some(page) => {
                // Linear search through rows - TODO: Figure out how this is normally done, maybe
                // binary search?
                for row in &page.rows {
                    if row.id == id {
                        return RowOption::Some(*row);
                    }
                }

                return RowOption::Empty();
            }

            None => RowOption::Empty(),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_found_eq_test() {
        let mut db = Database {
            page_size: 4096,
            pages: BTreeMap::new(),
        };
        db.insert(0, 100);

        match db.fetch(0) {
            RowOption::Some(row) => assert_eq!(row.value, 100),
            RowOption::Empty() => assert!(false),
        }
    }

    #[test]
    fn insert_found_ne_test() {
        let mut db = Database {
            page_size: 4096,
            pages: BTreeMap::new(),
        };
        db.insert(0, 100);

        match db.fetch(0) {
            RowOption::Some(row) => assert_ne!(row.value, 111),
            RowOption::Empty() => assert!(false),
        }
    }

    #[test]
    fn fetch_not_found_test() {
        let mut db = Database {
            page_size: 4096,
            pages: BTreeMap::new(),
        };
        db.insert(0, 100);

        // Fetch the wrong index and assert true if it's Empty
        match db.fetch(1) {
            RowOption::Some(_) => assert!(false),
            RowOption::Empty() => assert!(true),
        }
    }
}
