use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use redoxql::pagerange::PageRange;
use redoxql::record::{Record, RecordAddress, RecordLock};
use redoxql::table::PageDirectory;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

fn setup_benchmark_data(
    num_records: usize,
    num_cols: i64,
) -> (PageRange, Arc<Mutex<PageDirectory>>) {
    let page_range = PageRange::new(num_cols);
    let page_directory = Arc::new(Mutex::new(PageDirectory::new()));

    {
        let mut pd = page_directory.lock().unwrap();

        // We usually have less columns (30 seems realistic)
        for i in 0..30 {
            let rid = i as i64;
            let mut addresses = Vec::new();

            addresses.push(RecordAddress {
                page: page_range.base_container.rid_page(),
                offset: i as i64,
            });

            let record = Record {
                rid,
                addresses: Arc::new(Mutex::new(addresses)),
                lock: Arc::new(RwLock::new(RecordLock::default())),
            };

            pd.directory.insert(rid, record);
        }

        for i in 0..num_records {
            let rid = (1000 + i) as i64;
            let mut addresses = Vec::new();

            addresses.push(RecordAddress {
                page: page_range.base_container.rid_page(),
                offset: (i % 100) as i64,
            });

            let record = Record {
                rid,
                addresses: Arc::new(Mutex::new(addresses)),
                lock: Arc::new(RwLock::new(RecordLock::default())),
            };

            pd.directory.insert(rid, record);
        }
    }

    {
        let a = page_range.tail_container.rid_page();
        let mut rid_guard = a.lock().unwrap();
        rid_guard.data.resize(num_records, 0);
        for i in 0..num_records {
            rid_guard.data[i] = (1000 + i) as i64;
        }
    }

    (page_range, page_directory)
}

fn benchmark_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("PageRange_Merge");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::new("Original", size), size, |b, &size| {
            b.iter_with_setup(
                || setup_benchmark_data(size as usize, 5),
                |(mut page_range, pd)| {
                    black_box(page_range.merge(pd));
                },
            );
        });

        group.bench_with_input(BenchmarkId::new("Optimized", size), size, |b, &size| {
            b.iter_with_setup(
                || setup_benchmark_data(size as usize, 5),
                |(mut page_range, pd)| {
                    black_box(page_range.optimized_merge(pd));
                },
            );
        });
    }

    group.finish();
}

fn benchmark_column_counts(c: &mut Criterion) {
    let mut group = c.benchmark_group("PageRange_ColumnCounts");

    for cols in [3, 5].iter() {
        group.bench_with_input(BenchmarkId::new("Original", cols), cols, |b, &cols| {
            b.iter_with_setup(
                || setup_benchmark_data(100, cols),
                |(mut page_range, pd)| {
                    black_box(page_range.merge(pd));
                },
            );
        });

        group.bench_with_input(BenchmarkId::new("Optimized", cols), cols, |b, &cols| {
            b.iter_with_setup(
                || setup_benchmark_data(100, cols),
                |(mut page_range, pd)| {
                    black_box(page_range.optimized_merge(pd));
                },
            );
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = benchmark_merge, benchmark_column_counts
}
criterion_main!(benches);
