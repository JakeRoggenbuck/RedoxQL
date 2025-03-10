use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redoxql::record::Record;
use redoxql::table::PageDirectory;

fn bench_single_write(c: &mut Criterion) {
    c.bench_function("write to page dir once", |b| {
        b.iter_with_setup(
            || PageDirectory::new(),
            |mut pagedir| {
                pagedir.directory.insert(black_box(42), Record::default());
            },
        )
    });
}

fn bench_many_writes(c: &mut Criterion) {
    let sizes = [1_000, 10_000, 100_000];

    let mut group = c.benchmark_group("many writes to pagedir");
    for size in sizes.iter() {
        group.bench_with_input(format!("{} writes", size), size, |b, &size| {
            b.iter_with_setup(
                || PageDirectory::new(),
                |mut pagedir| {
                    for i in 0..size {
                        pagedir.directory.insert(black_box(i), Record::default());
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_many_reads(c: &mut Criterion) {
    let sizes = [1_000, 10_000, 100_000];

    let mut group = c.benchmark_group("many reads from pagedir");
    for size in sizes.iter() {
        group.bench_with_input(format!("{} reads", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut pd = PageDirectory::new();
                    for i in 0..size {
                        pd.directory.insert(black_box(i), Record::default());
                    }
                    pd
                },
                |pagedir| {
                    for i in 0..size {
                        black_box(pagedir.directory.get(&i));
                    }
                },
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_single_write,
    bench_many_reads,
    bench_many_writes,
);

criterion_main!(benches);
