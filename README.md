# ecs-165a-database
:crab: Yet to be named database written in Rust and Python 🚀

![Rust](https://img.shields.io/badge/Rust-1A5D8A?style=for-the-badge&logo=rust&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

> [!IMPORTANT]
> Read the Structure section — We use both Rust and Python and they go in different places

## Structure

![image](https://github.com/user-attachments/assets/4bc1e607-9b01-4992-a853-4a7636ba6196)

#### lstore
The lstore (`./lstore`) directory is where the Python code goes. This is what gets called by the tests in `__main__.py`. The lstore directory can be referred to as a Python module.

#### src
The src (`./src`) directory is where the Rust code goes. This gets called by the code in the lstore module.
