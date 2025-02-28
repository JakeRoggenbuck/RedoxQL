use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redoxql::page::PhysicalPage;

fn bench_new_page(c: &mut Criterion) {
    c.bench_function("new page", |b| b.iter(|| PhysicalPage::new(0)));
}

fn bench_single_write(c: &mut Criterion) {
    c.bench_function("single write", |b| {
        b.iter_with_setup(
            || PhysicalPage::new(0),
            |mut page| {
                page.write(black_box(42));
            },
        )
    });
}

fn bench_single_read(c: &mut Criterion) {
    c.bench_function("single read", |b| {
        b.iter_with_setup(
            || {
                let mut page = PhysicalPage::new(0);
                page.write(42);
                page
            },
            |page| {
                black_box(page.read(black_box(0)));
            },
        )
    });
}

fn bench_sequential_writes(c: &mut Criterion) {
    let sizes = [100, 1000, 10000];

    let mut group = c.benchmark_group("sequential writes");
    for size in sizes.iter() {
        group.bench_with_input(format!("{} writes", size), size, |b, &size| {
            b.iter_with_setup(
                || PhysicalPage::new(0),
                |mut page| {
                    for i in 0..size {
                        page.write(black_box(i));
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_sequential_reads(c: &mut Criterion) {
    let sizes = [100, 1000, 10000];

    let mut group = c.benchmark_group("sequential reads");
    for size in sizes.iter() {
        group.bench_with_input(format!("{} reads", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut page = PhysicalPage::new(0);
                    for i in 0..size {
                        page.write(i);
                    }
                    page
                },
                |page| {
                    for i in 0..size {
                        black_box(page.read(black_box(i as usize)));
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let sizes = [100, 1000, 10000];

    let mut group = c.benchmark_group("mixed workload");
    for size in sizes.iter() {
        group.bench_with_input(format!("{} operations", size), size, |b, &size| {
            b.iter_with_setup(
                || PhysicalPage::new(0),
                |mut page| {
                    for i in 0..size {
                        if i % 2 == 0 {
                            page.write(black_box(i));
                        } else {
                            black_box(page.read(black_box((i / 2) as usize)));
                        }
                    }
                },
            )
        });
    }
    group.finish();
}

fn bench_capacity_check(c: &mut Criterion) {
    c.bench_function("capacity check", |b| {
        b.iter_with_setup(
            || PhysicalPage::new(0),
            |page| {
                black_box(page.has_capacity());
            },
        )
    });
}

criterion_group!(
    benches,
    bench_new_page,
    bench_single_write,
    bench_single_read,
    bench_sequential_writes,
    bench_sequential_reads,
    bench_mixed_workload,
    bench_capacity_check
);

criterion_main!(benches);
