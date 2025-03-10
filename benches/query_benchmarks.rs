use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redoxql::database::RDatabase;
use redoxql::query::RQuery;

fn bench_insert(c: &mut Criterion) {
    c.bench_function("single insert", |b| {
        b.iter_with_setup(
            || {
                let mut db = RDatabase::new();
                let table = db.create_table(String::from("Grades"), 3, 0);
                RQuery::new(table)
            },
            |mut query| {
                query.insert(black_box(vec![1, 2, 3]));
            },
        )
    });
}

fn bench_select(c: &mut Criterion) {
    c.bench_function("single select", |b| {
        b.iter_with_setup(
            || {
                let mut db = RDatabase::new();
                let table = db.create_table(String::from("Grades"), 3, 0);
                let mut query = RQuery::new(table);
                query.insert(vec![1, 2, 3]);
                query
            },
            |mut query| {
                black_box(query.select(black_box(1), black_box(0), black_box(vec![1, 1, 1])));
            },
        )
    });
}

fn bench_update(c: &mut Criterion) {
    c.bench_function("single update", |b| {
        b.iter_with_setup(
            || {
                let mut db = RDatabase::new();
                let table = db.create_table(String::from("Grades"), 3, 0);
                let mut query = RQuery::new(table);
                query.insert(vec![1, 2, 3]);
                query
            },
            |mut query| {
                query.update(black_box(1), black_box(vec![Some(1), Some(5), Some(6)]));
            },
        )
    });
}

fn bench_delete(c: &mut Criterion) {
    c.bench_function("single delete", |b| {
        b.iter_with_setup(
            || {
                let mut db = RDatabase::new();
                let table = db.create_table(String::from("Grades"), 3, 0);
                let mut query = RQuery::new(table);
                query.insert(vec![1, 2, 3]);
                query
            },
            |mut query| {
                query.delete(black_box(1));
            },
        )
    });
}

fn bench_increment(c: &mut Criterion) {
    c.bench_function("single increment", |b| {
        b.iter_with_setup(
            || {
                let mut db = RDatabase::new();
                let table = db.create_table(String::from("Grades"), 3, 0);
                let mut query = RQuery::new(table);
                query.insert(vec![1, 2, 3]);
                query
            },
            |mut query| {
                query.increment(black_box(1), black_box(0));
            },
        )
    });
}

fn bench_select_version(c: &mut Criterion) {
    c.bench_function("select version", |b| {
        b.iter_with_setup(
            || {
                let mut db = RDatabase::new();
                let table = db.create_table(String::from("Grades"), 3, 0);
                let mut query = RQuery::new(table);
                query.insert(vec![1, 2, 3]);
                query.update(1, vec![Some(1), Some(4), Some(5)]);
                query.update(1, vec![Some(1), Some(6), Some(7)]);
                query
            },
            |mut query| {
                black_box(query.select_version(
                    black_box(1),
                    black_box(0),
                    black_box(vec![1, 1, 1]),
                    black_box(1),
                ));
            },
        )
    });
}

fn bench_multiple_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("multiple updates");
    for updates in [1, 5, 10].iter() {
        group.bench_with_input(format!("{} updates", updates), updates, |b, &updates| {
            b.iter_with_setup(
                || {
                    let mut db = RDatabase::new();
                    let table = db.create_table(String::from("Grades"), 3, 0);
                    let mut query = RQuery::new(table);
                    query.insert(vec![1, 2, 3]);
                    query
                },
                |mut query| {
                    for _ in 0..updates {
                        query.update(1, vec![Some(1), Some(5), Some(6)]);
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum operation");
    for size in [10, 100, 1_000, 10_000, 100_000].iter() {
        group.bench_with_input(format!("sum {} records", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut db = RDatabase::new();
                    let table = db.create_table(String::from("Grades"), 3, 0);
                    let mut query = RQuery::new(table);
                    for i in 0..size {
                        query.insert(vec![i as i64, i as i64 * 2, i as i64 * 3]);
                    }
                    query
                },
                |mut query| {
                    black_box(query.sum(black_box(0), black_box(size as i64 - 1), black_box(1)));
                },
            )
        });
    }
    group.finish();
}

fn bench_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk inserts");
    for size in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(format!("{} inserts", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut db = RDatabase::new();
                    let table = db.create_table(String::from("Grades"), 3, 0);
                    RQuery::new(table)
                },
                |mut query| {
                    for i in 0..size {
                        query.insert(black_box(vec![i as i64, i as i64 * 2, i as i64 * 3]));
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_bulk_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk selects");
    for size in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(format!("{} selects", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut db = RDatabase::new();
                    let table = db.create_table(String::from("Grades"), 3, 0);
                    let mut query = RQuery::new(table);

                    for i in 0..size {
                        query.insert(vec![i as i64, i as i64 * 2, i as i64 * 3]);
                    }
                    query
                },
                |mut query| {
                    for i in 0..size {
                        black_box(query.select(
                            black_box(i as i64),
                            black_box(0),
                            black_box(vec![1, 1, 1]),
                        ));
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_bulk_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk updates");
    for size in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(format!("{} updates", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut db = RDatabase::new();
                    let table = db.create_table(String::from("Grades"), 3, 0);
                    let mut query = RQuery::new(table);

                    for i in 0..size {
                        query.insert(vec![i as i64, i as i64 * 2, i as i64 * 3]);
                    }
                    query
                },
                |mut query| {
                    for i in 0..size {
                        query.update(
                            black_box(i as i64),
                            black_box(vec![Some(i as i64), Some(i as i64 * 5), Some(i as i64 * 6)]),
                        );
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_bulk_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk deletes");
    for size in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(format!("{} deletes", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut db = RDatabase::new();
                    let table = db.create_table(String::from("Grades"), 3, 0);
                    let mut query = RQuery::new(table);

                    for i in 0..size {
                        query.insert(vec![i as i64, i as i64 * 2, i as i64 * 3]);
                    }
                    query
                },
                |mut query| {
                    for i in 0..size {
                        query.delete(black_box(i as i64));
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed operations");
    for size in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(format!("{} mixed operations", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut db = RDatabase::new();
                    let table = db.create_table(String::from("Grades"), 3, 0);
                    let mut query = RQuery::new(table);

                    for i in 0..size / 2 {
                        query.insert(vec![i as i64, i as i64 * 2, i as i64 * 3]);
                    }
                    query
                },
                |mut query| {
                    for i in 0..size {
                        match i % 4 {
                            0 => {
                                let _ = query.insert(black_box(vec![
                                    i as i64,
                                    i as i64 * 2,
                                    i as i64 * 3,
                                ]));
                            }
                            1 => {
                                let _ = query.select(
                                    black_box(i as i64 / 2),
                                    black_box(0),
                                    black_box(vec![1, 1, 1]),
                                );
                            }
                            2 => {
                                let _ = query.update(
                                    black_box(i as i64 / 2),
                                    black_box(vec![
                                        Some(i as i64),
                                        Some(i as i64 * 5),
                                        Some(i as i64 * 6),
                                    ]),
                                );
                            }
                            3 => {
                                query.delete(black_box(i as i64 / 2));
                            }
                            _ => unreachable!(),
                        }
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_version_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("version history");
    for updates in [5, 10, 50, 100].iter() {
        group.bench_with_input(
            format!("{} version history", updates),
            updates,
            |b, &updates| {
                b.iter_with_setup(
                    || {
                        let mut db = RDatabase::new();
                        let table = db.create_table(String::from("Grades"), 3, 0);
                        let mut query = RQuery::new(table);

                        query.insert(vec![1, 2, 3]);

                        for i in 0..updates {
                            query.update(1, vec![Some(1), Some(i), Some(i + 1)]);
                        }
                        query
                    },
                    |mut query| {
                        for version in 0..updates {
                            black_box(query.select_version(
                                black_box(1),
                                black_box(0),
                                black_box(vec![1, 1, 1]),
                                black_box(version),
                            ));
                        }
                    },
                )
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_select,
    bench_update,
    bench_delete,
    bench_increment,
    bench_select_version,
    bench_multiple_updates,
    bench_sum,
    bench_bulk_insert,
    bench_bulk_select,
    bench_bulk_update,
    bench_bulk_delete,
    bench_mixed_workload,
    bench_version_history
);
criterion_main!(benches);
