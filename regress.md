src (sum-tests) λ cargo bench -- sum
   Compiling either v1.13.0
   Compiling crossbeam-utils v0.8.21
   Compiling itertools v0.10.5
   Compiling crossbeam-epoch v0.9.18
   Compiling crossbeam-deque v0.8.6
   Compiling rayon-core v1.12.1
   Compiling rayon v1.10.0
   Compiling criterion-plot v0.5.0
   Compiling criterion v0.4.0
   Compiling redoxql v0.1.0 (/home/jake/Repos/redoxql)
    Finished `bench` profile [optimized + debuginfo] target(s) in 1m 19s
     Running unittests src/lib.rs (/home/jake/Repos/redoxql/target/release/deps/redoxql-bd4aee3918e78c09)

running 2 tests
test table::tests::large_sum_test ... ignored
test table::tests::sum_test ... ignored

test result: ok. 0 passed; 0 failed; 2 ignored; 0 measured; 43 filtered out; finished in 0.00s

     Running benches/merge_benchmarks.rs (/home/jake/Repos/redoxql/target/release/deps/merge_benchmarks-9a5f75221ac36d28)
Gnuplot not found, using plotters backend
     Running benches/page_benchmarks.rs (/home/jake/Repos/redoxql/target/release/deps/page_benchmarks-23f697d7bf81e8e7)
Gnuplot not found, using plotters backend
     Running benches/page_dir_benchmarks.rs (/home/jake/Repos/redoxql/target/release/deps/page_dir_benchmarks-a5a0b831cdac0359)
Gnuplot not found, using plotters backend
     Running benches/query_benchmarks.rs (/home/jake/Repos/redoxql/target/release/deps/query_benchmarks-2d8c6c38b30cda52)
Gnuplot not found, using plotters backend
sum operation/sum 10 records
                        time:   [34.739 µs 34.908 µs 35.093 µs]
                        change: [+2366.9% +2398.9% +2434.7%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 8 outliers among 100 measurements (8.00%)
  1 (1.00%) low severe
  4 (4.00%) high mild
  3 (3.00%) high severe
sum operation/sum 100 records
                        time:   [97.079 µs 98.910 µs 101.18 µs]
                        change: [+495.20% +502.97% +511.99%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 10 outliers among 100 measurements (10.00%)
  6 (6.00%) high mild
  4 (4.00%) high severe
sum operation/sum 1000 records
                        time:   [479.19 µs 480.36 µs 481.51 µs]
                        change: [+166.07% +166.90% +167.77%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) low mild
sum operation/sum 10000 records
                        time:   [3.5710 ms 3.5928 ms 3.6174 ms]
                        change: [+75.521% +76.708% +78.180%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 10 outliers among 100 measurements (10.00%)
  2 (2.00%) high mild
  8 (8.00%) high severe
Benchmarking sum operation/sum 100000 records: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 8.7s, or reduce sample count to 50.
sum operation/sum 100000 records
                        time:   [53.093 ms 53.273 ms 53.463 ms]
                        change: [+31.530% +32.465% +33.422%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 9 outliers among 100 measurements (9.00%)
  8 (8.00%) high mild
  1 (1.00%) high severe

(arg: 1) ^C
src (sum-tests) λ
