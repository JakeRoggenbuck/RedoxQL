# RedoxQL - ecs-165a-database
🦀 RedoxQL is an L-Store database written in Rust and Python 🚀

![Rust](https://img.shields.io/badge/Rust-1A5D8A?style=for-the-badge&logo=rust&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

![image](https://github.com/user-attachments/assets/2ac1e769-afdf-4905-8d99-d18df26cc7ff)

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
- Keanu - page.rs and all the page stuff, index.rs and all of the index stuff
- Lucas & Andrew - update
- Lucas - select_version, sum_version, matching 
- Abdulrasol - BaseContainer, TailContainer, PageDirectory, insert into containers, RecordAddress and Record
- Jake - RTable, RDatabase, RQuery, new RIndex, python bindings, inserts and reads for all props

## Structure

![RedoxQL system diagram](https://github.com/user-attachments/assets/9b170cbd-1bbc-4966-870d-47a331bf2515)

### File Structure

```
.
├── Cargo.lock
├── Cargo.toml
├── LICENSE
├── main_checking.py
├── __main__.py
├── pyproject.toml
├── python
│   ├── lstore
│   │   ├── db.py
│   │   ├── __init__.py
│   │   ├── __pycache__
│   │   │   └── __init__.cpython-312.pyc
│   │   ├── query.py
│   │   ├── transaction.py
│   │   └── transaction_worker.py
│   └── tests
│       ├── __init__.py
│       ├── __pycache__
│       │   ├── __init__.cpython-312.pyc
│       │   └── test_main.cpython-312-pytest-8.3.4.pyc
│       └── test_main.py
├── README.md
├── requirements.txt
└── src
    ├── container.rs
    ├── database.rs
    ├── lib.rs
    ├── page.rs
    ├── query.rs
    └── system.rs
```

#### lstore
The lstore (`./lstore`) directory is where the Python code goes. This is what gets called by the tests in `__main__.py`. The lstore directory can be referred to as a Python module.

#### src
The src (`./src`) directory is where the Rust code goes. This gets called by the code in the lstore module.

`system.rs` - all the functions and structs related to getting information from the system machine
`database.rs` - the database struct and the related functions

## Testing

#### Rust testing
```
cargo test
```

Here is what the correct output should look like. You should see multiple tests passing.

![image](https://github.com/user-attachments/assets/b6aee0b5-571f-4450-9381-296efc5e2f73)

Rust tests are located in each Rust file and can be found in `./src`

#### Python testing
```
pytest
```

Python tests are located in a seperate directory called `tests` located in `./python`

## Rust Docs

Rust has a way of making docs from the source code. Run `cargo doc` and view the produced HTML page in your browser. Adding comments to yor code starting with `///` will be put into these docs. 
