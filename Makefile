all:
	make release

release:
	maturin build --release
	pip install --force-reinstall target/wheels/lstore*

dev:
	maturin build
	pip install --force-reinstall target/wheels/lstore*

test:
	cargo test
