all:
	maturin build --release
	pip install --force-reinstall target/wheels/lstore*
