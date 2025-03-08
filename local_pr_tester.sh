source venv/bin/activate

pytest

cargo build --release

cargo test

maturin build --release

python3 testM2.py > ./test-outputs/M2.out
cat ./test-outputs/M2.out | grep -b100 --color=always "Wrong" ./test-outputs/M2.out
