source venv/bin/activate

pytest

cargo build --release

cargo test

maturin build --release
