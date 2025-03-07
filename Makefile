all:
	make release

release:
	maturin build --release
	pip install --force-reinstall target/wheels/lstore-0.1.0-c*

test:
	cargo test

pyenv:
	python3 -m venv venv
	source venv/bin/activate
	pip install -r requirements.txt

format:
	cargo fmt
