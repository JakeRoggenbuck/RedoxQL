# ecs-165a-database
:crab: Yet to be named database written in Rust and Python 🚀

![Rust](https://img.shields.io/badge/Rust-1A5D8A?style=for-the-badge&logo=rust&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

> [!IMPORTANT]
> Read the Structure section — We use both Rust and Python and they go in different places

## Setup
Create a virtual envirement
```
python3 -m venv venv
```

Install maturin
```
pip install maturin
```

## Running
Build the Rust code
```
maturin build
```

Install the module (Note: the version will change so check the exact filename in `target/wheels/`)
```
pip install target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
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

#### lstore
The lstore (`./lstore`) directory is where the Python code goes. This is what gets called by the tests in `__main__.py`. The lstore directory can be referred to as a Python module.

#### src
The src (`./src`) directory is where the Rust code goes. This gets called by the code in the lstore module.
