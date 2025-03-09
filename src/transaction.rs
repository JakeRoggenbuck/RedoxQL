use super::query::RQuery;
use super::table::RTableHandle;
use log::debug;
use pyo3::prelude::*;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
enum QueryFunctions {
    Delete,
    Insert,
    Update,
    SumVersion,
    None,
}

#[derive(Clone)]
struct SingleQuery {
    func: QueryFunctions,
    table: RTableHandle,
    args: Vec<Option<i64>>,
}

#[pyclass]
pub struct RTransaction {
    queries: VecDeque<SingleQuery>,
}

#[pymethods]
impl RTransaction {
    #[new]
    pub fn new() -> Self {
        RTransaction {
            queries: VecDeque::new(),
        }
    }

    pub fn add_query(&mut self, function_name: &str, table: RTableHandle, args: Vec<Option<i64>>) {
        let func = match function_name {
            "delete" => QueryFunctions::Delete,
            "insert" => QueryFunctions::Insert,
            "update" => QueryFunctions::Update,
            "sum_version" => QueryFunctions::SumVersion,
            _ => QueryFunctions::None,
        };

        let q = SingleQuery { func, table, args };

        self.queries.push_back(q.clone());
        debug!("Pushed {:?}", q.func);
    }

    pub fn run(&mut self) {
        debug!("Started run!");

        for q in self.queries.iter_mut() {
            // TODO: Don't make a new RQuery for each call
            // We need to right now because there might be a test where
            // different tables are in the same query
            let t = q.table.clone();
            let mut query = RQuery::new(t);

            match q.func {
                QueryFunctions::Delete => {
                    let args_clone = q.args.clone();
                    let first = args_clone.first();

                    if let Some(f) = first {
                        if let Some(a) = f {
                            query.delete(*a);
                        }
                    }
                }
                QueryFunctions::Insert => {
                    let mut args = Vec::new();
                    let args_clone = q.args.clone();

                    // Get only Some type from args
                    // Should be everything!
                    for a in args_clone {
                        if let Some(b) = a {
                            args.push(b);
                        } else {
                            debug!("Optional passes to insert incorrectly!");
                        }
                    }

                    query.insert(args);
                }
                QueryFunctions::Update => {
                    let args_clone = q.args.clone();
                    let first = args_clone.first();
                    let rest = args_clone.iter().skip(1).cloned().collect();

                    if let Some(f) = first {
                        if let Some(a) = f {
                            query.update(*a, rest);
                        }
                    }
                }
                QueryFunctions::SumVersion => {
                    let args_clone = q.args.clone();
                    if args_clone.len() > 4 {
                        // TODO: Figure out if indexing is actually slower than .get and then
                        // checking if it's optional
                        let start = args_clone[0];
                        let end = args_clone[1];
                        let cols = &args_clone[2..args_clone.len() - 1].to_vec();

                        let ver = args_clone[args_clone.len() - 1];

                        // Get just Some type
                        let mut new_cols = vec![];
                        for c in cols {
                            if let Some(a) = c {
                                new_cols.push(*a);
                            }
                        }

                        let newer_cols = Some(new_cols);

                        // Run select_version version safely only if all args are there
                        match (start, end, newer_cols, ver) {
                            (Some(s), Some(e), Some(c), Some(v)) => {
                                query.select_version(s, e, c, v);
                            }
                            _ => {}
                        }
                    }
                }
                QueryFunctions::None => {
                    debug!("Something went wrong.")
                }
            }
        }
    }
}
