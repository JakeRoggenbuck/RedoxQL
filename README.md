# RedoxQL - ecs-165a-database
🦀 RedoxQL written in Rust and Python 🚀

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
maturin build
```

Install the module (Note: the version will change so check the exact filename in `target/wheels/`)
```
pip install target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl --force-reinstall
```

Run the database benchmark
```
python3 __main__.py
```

You should see this ...
```
(venv) ecs-165a-database (main) λ p __main__.py
Inserting 10k records took:  			 0.0017988820000000016
Updating 10k records took:  			 0.008435604000000003
Selecting 10k records took:  			 0.003455875000000004
Aggregate 10k of 100 record batch took:	 4.326100000000277e-05
Deleting 10k records took:  			 0.0006921710000000053
(venv) ecs-165a-database (main) λ
```

## Structure

![image](https://github.com/user-attachments/assets/4bc1e607-9b01-4992-a853-4a7636ba6196)

![image](https://github.com/user-attachments/assets/d39b6500-13aa-4568-9d90-57811c5359f8)

![image](https://github.com/user-attachments/assets/d16e1c24-ef6a-4bb6-98cb-30b9c071142e)

![image](https://github.com/user-attachments/assets/dba599a7-347b-4466-bcdf-cca5b44c4c5b)

![image](https://github.com/user-attachments/assets/4d501d6d-7a3d-44d0-9854-f60c0c43b7f9)

![image](https://github.com/user-attachments/assets/3079e4a1-5c60-4244-9cbb-57d877d35061)

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

Rust tests are located in each Rust file and can be found in `./src`

#### Python testing
```
pytest
```

Python tests are located in a seperate directory called `tests` located in `./python`

## Rust Docs

Rust has a way of making docs from the source code. Run `cargo doc` and view the produced HTML page in your browser. Adding comments to yor code starting with `///` will be put into these docs. 
