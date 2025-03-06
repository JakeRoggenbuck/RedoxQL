
![RedoxQL-darkmode](https://github.com/user-attachments/assets/ad60c31a-ff64-47b0-b50f-2d161c9e9f96#gh-dark-mode-only)
![RedoxQL-lightmode](https://github.com/user-attachments/assets/8f38e31e-163a-4f49-aa43-34484ad361ed#gh-light-mode-only)

🦀 RedoxQL is an L-Store database written in Rust and Python 🚀

![Rust](https://img.shields.io/badge/Rust-1A5D8A?style=for-the-badge&logo=rust&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

RedoxQL is our implementation of an [L-Store](https://openproceedings.org/2018/conf/edbt/paper-215.pdf) database.

![image](https://github.com/user-attachments/assets/bb1f9c7e-0d9a-4c52-8630-73990e0a7eea)

> [!IMPORTANT]
> Read the Structure section — We use both Rust and Python and they go in different places

## Setup
Create a virtual envirement
```
python3 -m venv venv
```

Source the virtual envirement
```
source venv/bin/activate
```

Install maturin
```
pip install -r requirements.txt
```

## Running
Build the Rust code
```
maturin build --release
```

Install the module (Note: the version will change so check the exact filename in `target/wheels/`)
```
pip install target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl --force-reinstall
```

Run the database benchmark
```
python3 __main__.py
```

You should see this for milestone 1.

```
(venv) ecs-165a-database (main) λ p __main__.py
Inserting 10k records took:  			 0.0077650810000000035
Updating 10k records took:  			 0.020893269
Selecting 10k records took:  			 0.016048745000000003
Aggregate 10k of 100 record batch took:	 0.0039221569999999956
Deleting 10k records took:  			 0.002314741000000009
(venv) ecs-165a-database (main) λ
```

## Attribution
- Keanu - Secondary indexes, page.rs and all the page stuff, index.rs and all of the index stuff
- Lucas & Andrew - update
- Lucas - Merge, select_version, sum_version, matching 
- Abdulrasol - Merge, BaseContainer, TailContainer, PageDirectory, insert into containers, RecordAddress and Record
- Jake - Persistence, RTable, RDatabase, RQuery, new RIndex, python bindings, inserts and reads for all props

## Structure

![RedoxQL system diagram](https://github.com/user-attachments/assets/9b170cbd-1bbc-4966-870d-47a331bf2515)

### File Structure

```
.
├── benches
│   └── db_benchmark.rs
├── Cargo.lock
├── Cargo.toml
├── docs
│   └── old-system-diagrams.md
├── LICENSE
├── main_checking.py
├── __main__.py
├── Makefile
├── pyproject.toml
├── python
│   ├── benchmarks
│   │   ├── graph_scale.py
│   │   ├── scaling_tester.py
│   │   ├── simple_example.py
│   │   ├── simple_tester.py
│   │   └── speedtests.py
│   ├── lstore
│   │   ├── db.py
│   │   ├── __init__.py
│   │   ├── query.py
│   │   ├── transaction.py
│   │   └── transaction_worker.py
│   └── tests
│       ├── __init__.py
│       └── test_main.py
├── README.md
├── requirements.txt
└── src
    ├── container.rs
    ├── database.rs
    ├── index.rs
    ├── lib.rs
    ├── pagerange.rs
    ├── page.rs
    ├── query.rs
    ├── record.rs
    ├── system.rs
    └── table.rs
```

#### lstore
The lstore (`./lstore`) directory is where the Python code goes. This is what gets called by the tests in `__main__.py`. The lstore directory can be referred to as a Python module.

#### src
The src (`./src`) directory is where the Rust code goes. This gets called by the code in the lstore module.

`system.rs` - all the functions and structs related to getting information from the system machine
`database.rs` - the database struct and the related functions

## Testing

### Rust testing

```
cargo test
```

Here is what the correct output should look like. You should see multiple tests passing.

![image](https://github.com/user-attachments/assets/b6aee0b5-571f-4450-9381-296efc5e2f73)

#### Unit Tests

Rust unit tests are located in each Rust file and can be found in `./src`

#### Integration Tests

The integration tests are located at `./tests` and are also run with `cargo test`

### Python testing
```
pytest
```

Python tests are located in a separate directory called `tests` located in `./python`

## Rust Docs

Rust has a way of making docs from the source code. Run `cargo doc` and view the produced HTML page in your browser. Adding comments to yor code starting with `///` will be put into these docs.

## Speed Analysis

### Using flamegraph to benchmark

You may need to install flamegraph with `cargo install flamegraph`

```sh
cargo flamegraph --test update_tests
```

```sh
# Open the svg (It's nice to view in a browser)
firefox flamegraph.svg
```

Preview:

![image](https://github.com/user-attachments/assets/ac866062-79f2-45e0-84ae-c81dceef68cc)

### Running cargo benchmarks

This will take a long time but you can run benchmarks separately.

```sh
cargo bench
```

You can use `cargo bench` to see if your changes significantly affect performance.

![image](https://github.com/user-attachments/assets/367e1f7a-dd85-46ed-998a-939f95a1b561)

Often small changes can happen randomly. Like this has no change in the code.
Try to run the bench as the only thing running on the system.

### Using maturin in release mode

![perf_chart](https://github.com/user-attachments/assets/31b18374-11b6-42fd-8405-5f32a751804f)
![tests_chart](https://github.com/user-attachments/assets/8e638ec0-12f7-461f-b1e6-7823d98004cf)

Data for both graphs can be found [here](./python/benchmarks/speedtests.py)

### Scaling of Insert Operation

![scaling](https://github.com/user-attachments/assets/22cff07d-d7b0-4502-b559-635a22e38c77)

![update_scale](https://github.com/user-attachments/assets/e65ee6c3-7256-4cf1-8432-369cd6658eaf)

### Using release build settings

With compiler options 
```
(venv) redoxql (main) λ p __main__.py
Inserting 10k records took:                      0.006803145
Updating 10k records took:                       0.018702902999999996
Selecting 10k records took:                      0.016315803000000004
Aggregate 10k of 100 record batch took:  0.005981531999999998
Deleting 10k records took:                       0.002332115999999995
(venv) redoxql (main) λ time p m1_tester.py
Insert finished

real    0m0.117s
user    0m0.106s
sys     0m0.010s
user    0m0.106s
sys     0m0.010s
(venv) redoxql (main) λ time p exam_tester_m1.py
Insert finished

real    0m0.282s
user    0m0.272s
sys     0m0.010s
(venv) redoxql (main) λ
```

Without compiler options
```
(venv) redoxql (main) λ p __main__.py
Inserting 10k records took:                      0.007401888000000002
Updating 10k records took:                       0.018957811999999997
Selecting 10k records took:                      0.015054888999999995
Aggregate 10k of 100 record batch took:  0.003300163000000002
Deleting 10k records took:                       0.002181812999999991
(venv) redoxql (main) λ time p m1_tester.py
Insert finished

real    0m0.112s
user    0m0.108s
sys     0m0.004s
(venv) redoxql (main) λ time p exam_tester_m1.py
Insert finished

real    0m0.268s
user    0m0.254s
sys     0m0.014s
```

### Running with debug or info logging

To use the logger, import the debug, error, or info macro from the log crate.
Then you can add the macros to code like `debug!("Start database!");`.
When you go to run the code, you can set the env var `RUST_LOG=debug`.
Docs: https://docs.rs/env_logger/latest/env_logger/.

![image](https://github.com/user-attachments/assets/1fb2f55f-f21b-4b2d-8301-88e44e0a9260)

### We tried Pypy

We started to try using Pypy which is a runtime for Python that is supposedly faster. Because of [Amdahl's law](https://en.wikipedia.org/wiki/Amdahl%27s_law), we actually can't get all that much performance out of it. We also found that the newest version of Pypy cannot use the newest version of Pyo3, so future work is needed to get them to run together.

Future questions:
- How much time does the Python part take up?
- How do we measure the improvement from Python to Pypy
- How do we downgrade Pypy to work with Py03
