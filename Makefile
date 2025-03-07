all:
	make release

release:
	maturin build --release
	# C Python version
	pip install --force-reinstall target/wheels/lstore-0.1.0-cp312*

	# Pypy version
	# pip install --force-reinstall target/wheels/lstore-0.1.0-pp311*

test:
	cargo test

pyenv:
	python3 -m venv venv
	source venv/bin/activate
	pip install -r requirements.txt

format:
	cargo fmt
