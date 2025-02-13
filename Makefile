all:
	maturin build
	pip install --force-reinstall target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
