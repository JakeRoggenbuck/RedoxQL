(venv) redoxql (main) λ p testM2.py
==========correctness tester===============
DEBUG: Result of select(1, 2, [1,1,1,1,1]) = [[0, 1, 1, 2, 1], [1, 1, 1, 1, 2], [5, 1, 1, 1, 1], [7, 1, 1, 1, 1]]
PASS[0]
DEBUG: Result of select(3, 2, [1,1,1,1,1]) = [[2, 0, 3, 5, 1]]
PASS[1]
PASS[2]
[]
PASS[3]
Wrong[4]
Wrong[5]
Wrong[6]
PASS[7]
5
PASS[8]
==========durability tester================
Checking exam M2 durability
Insert finished
Select finished
Update finished
Aggregate finished
DB is closed
Select finished
Aggregate finished
==========merging tester===================

Total time Taken:  1.20 seconds
(venv) redoxql (main) λ ls
__main__.py        exam_tester_m2_part1.py  m1_tester.py         main_checking.py  pypyvenv               requirements.txt             test-outputs                venv
benches            exam_tester_m2_part2.py  M2                   Makefile          python                 run_all_tests.sh             testM1.py
Cargo.lock         exam_tester_m3_part1.py  m2_tester_part1.py   perf.data         python_problematic.py  simple_durability.py         testM2.py
Cargo.toml         exam_tester_m3_part2.py  m2_tester_part2.py   perf.data.old     README.md              simple_update_durability.py  tests
docs               LICENSE                  m3_tester_part_1.py  profile.svg       redoxdata              src                          update-flamegraph.svg
exam_tester_m1.py  local_pr_tester.sh       m3_tester_part_2.py  pyproject.toml    redoxql-m1.zip         target                       update-proj-flamegraph.svg
(venv) redoxql (main) λ git bisect start
status: waiting for both good and bad commits
(venv) redoxql (main|BISECTING) λ git bisect bad
status: waiting for good commit(s), bad commit known
(venv) redoxql (main|BISECTING) λ git checkout milestone2
Note: switching to 'milestone2'.

You are in 'detached HEAD' state. You can look around, make experimental
changes and commit them, and you can discard any commits you make in this
state without impacting any branches by switching back to a branch.

If you want to create a new branch to retain commits you create, you may
do so (now or later) by using -c with the switch command. Example:

  git switch -c <new-branch-name>

Or undo this operation with:

  git switch -

Turn off this advice by setting config variable advice.detachedHead to false

HEAD is now at 32651ba Merge pull request #137 from JakeRoggenbuck/faster
(venv) redoxql ((milestone2)|BISECTING) λ git bisect good
Bisecting: 43 revisions left to test after this (roughly 6 steps)
[8c3f79113b6e400a794f52d2c61c69022617d6bc] Add py profile info
(venv) redoxql ((8c3f791...)|BISECTING) λ make
make release
make[1]: Entering directory '/home/jake/Repos/redoxql'
maturin build --release
📦 Including license file "/home/jake/Repos/redoxql/LICENSE"
🍹 Building a mixed python/rust project
🔗 Found pyo3 bindings
🐍 Found CPython 3.12 at /home/jake/Repos/redoxql/venv/bin/python
📡 Using build options features from pyproject.toml
   Compiling redoxql v0.1.0 (/home/jake/Repos/redoxql)
    Finished `release` profile [optimized] target(s) in 12.43s
📦 Built wheel for CPython 3.12 to /home/jake/Repos/redoxql/target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
pip install --force-reinstall target/wheels/lstore*
Processing ./target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
Installing collected packages: lstore
  Attempting uninstall: lstore
    Found existing installation: lstore 0.1.0
    Uninstalling lstore-0.1.0:
      Successfully uninstalled lstore-0.1.0
Successfully installed lstore-0.1.0

[notice] A new release of pip is available: 24.2 -> 25.0.1
[notice] To update, run: pip install --upgrade pip
make[1]: Leaving directory '/home/jake/Repos/redoxql'
(venv) redoxql ((8c3f791...)|BISECTING) λ p testM2.py
==========correctness tester===============
DEBUG: Result of select(1, 2, [1,1,1,1,1]) = [[0, 1, 1, 2, 1], [1, 1, 1, 1, 2], [5, 1, 1, 1, 1], [7, 1, 1, 1, 1]]
PASS[0]
DEBUG: Result of select(3, 2, [1,1,1,1,1]) = [[2, 0, 3, 5, 1]]
PASS[1]
PASS[2]
[]
PASS[3]
PASS[4]
PASS[5]
PASS[6]
PASS[7]
5
PASS[8]
==========durability tester================
Checking exam M2 durability
Insert finished
Select finished
Update finished
Aggregate finished
DB is closed
Select finished
Aggregate finished
==========merging tester===================

Total time Taken:  1.26 seconds
(venv) redoxql ((8c3f791...)|BISECTING) λ git bisect good
Bisecting: 21 revisions left to test after this (roughly 5 steps)
[658b1ff5ac645a3ced681f61248a32f6dcfefe85] Try out Pypy
(venv) redoxql ((658b1ff...)|BISECTING) λ make
make release
make[1]: Entering directory '/home/jake/Repos/redoxql'
maturin build --release
📦 Including license file "/home/jake/Repos/redoxql/LICENSE"
🍹 Building a mixed python/rust project
🔗 Found pyo3 bindings
🐍 Found CPython 3.12 at /home/jake/Repos/redoxql/venv/bin/python
📡 Using build options features from pyproject.toml
   Compiling redoxql v0.1.0 (/home/jake/Repos/redoxql)
    Finished `release` profile [optimized] target(s) in 12.91s
📦 Built wheel for CPython 3.12 to /home/jake/Repos/redoxql/target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
# C Python version
pip install --force-reinstall target/wheels/lstore-0.1.0-cp312*
Processing ./target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
Installing collected packages: lstore
  Attempting uninstall: lstore
    Found existing installation: lstore 0.1.0
    Uninstalling lstore-0.1.0:
      Successfully uninstalled lstore-0.1.0
Successfully installed lstore-0.1.0

[notice] A new release of pip is available: 24.2 -> 25.0.1
[notice] To update, run: pip install --upgrade pip
# Pypy version
# pip install --force-reinstall target/wheels/lstore-0.1.0-pp311*
make[1]: Leaving directory '/home/jake/Repos/redoxql'
(venv) redoxql ((658b1ff...)|BISECTING) λ p testM2.py
==========correctness tester===============
DEBUG: Result of select(1, 2, [1,1,1,1,1]) = [[0, 1, 1, 2, 1], [1, 1, 1, 1, 2], [5, 1, 1, 1, 1], [7, 1, 1, 1, 1]]
PASS[0]
DEBUG: Result of select(3, 2, [1,1,1,1,1]) = [[2, 0, 3, 5, 1]]
PASS[1]
PASS[2]
[]
PASS[3]
Wrong[4]
Wrong[5]
Wrong[6]
PASS[7]
5
PASS[8]
==========durability tester================
Checking exam M2 durability
Insert finished
Select finished
Update finished
Aggregate finished
DB is closed
Select finished
Aggregate finished
==========merging tester===================

Total time Taken:  1.17 seconds
(venv) redoxql ((658b1ff...)|BISECTING) λ git bisect bad
Bisecting: 12 revisions left to test after this (roughly 4 steps)
[909c64e2eb7702555aa3e17d0429ae0ee986f6ae] Merge pull request #155 from JakeRoggenbuck/profile-python
(venv) redoxql ((909c64e...)|BISECTING) λ p testM2.py
==========correctness tester===============
DEBUG: Result of select(1, 2, [1,1,1,1,1]) = [[0, 1, 1, 2, 1], [1, 1, 1, 1, 2], [5, 1, 1, 1, 1], [7, 1, 1, 1, 1]]
PASS[0]
DEBUG: Result of select(3, 2, [1,1,1,1,1]) = [[2, 0, 3, 5, 1]]
PASS[1]
PASS[2]
[]
PASS[3]
Wrong[4]
Wrong[5]
Wrong[6]
PASS[7]
5
PASS[8]
==========durability tester================
Checking exam M2 durability
Insert finished
Select finished
Update finished
Aggregate finished
DB is closed
Select finished
Aggregate finished
==========merging tester===================

Total time Taken:  1.16 seconds
(venv) redoxql ((909c64e...)|BISECTING) λ make
make release
make[1]: Entering directory '/home/jake/Repos/redoxql'
maturin build --release
📦 Including license file "/home/jake/Repos/redoxql/LICENSE"
🍹 Building a mixed python/rust project
🔗 Found pyo3 bindings
🐍 Found CPython 3.12 at /home/jake/Repos/redoxql/venv/bin/python
📡 Using build options features from pyproject.toml
   Compiling redoxql v0.1.0 (/home/jake/Repos/redoxql)
    Finished `release` profile [optimized] target(s) in 12.43s
📦 Built wheel for CPython 3.12 to /home/jake/Repos/redoxql/target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
pip install --force-reinstall target/wheels/lstore*
Processing ./target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
Installing collected packages: lstore
  Attempting uninstall: lstore
    Found existing installation: lstore 0.1.0
    Uninstalling lstore-0.1.0:
      Successfully uninstalled lstore-0.1.0
Successfully installed lstore-0.1.0

[notice] A new release of pip is available: 24.2 -> 25.0.1
[notice] To update, run: pip install --upgrade pip
make[1]: Leaving directory '/home/jake/Repos/redoxql'
(venv) redoxql ((909c64e...)|BISECTING) λ p testM2.py
==========correctness tester===============
DEBUG: Result of select(1, 2, [1,1,1,1,1]) = [[0, 1, 1, 2, 1], [1, 1, 1, 1, 2], [5, 1, 1, 1, 1], [7, 1, 1, 1, 1]]
PASS[0]
DEBUG: Result of select(3, 2, [1,1,1,1,1]) = [[2, 0, 3, 5, 1]]
PASS[1]
PASS[2]
[]
PASS[3]
PASS[4]
PASS[5]
PASS[6]
PASS[7]
5
PASS[8]
==========durability tester================
Checking exam M2 durability
Insert finished
Select finished
Update finished
Aggregate finished
DB is closed
Select finished
Aggregate finished
==========merging tester===================

Total time Taken:  1.22 seconds
(venv) redoxql ((909c64e...)|BISECTING) λ git bisect good
Bisecting: 6 revisions left to test after this (roughly 3 steps)
[d48ae4bdb1aa9318ff1b3df78b9c98daacceb95f] Merge pull request #163 from JakeRoggenbuck/more-docs
(venv) redoxql ((d48ae4b...)|BISECTING) λ make
make release
make[1]: Entering directory '/home/jake/Repos/redoxql'
maturin build --release
📦 Including license file "/home/jake/Repos/redoxql/LICENSE"
🍹 Building a mixed python/rust project
🔗 Found pyo3 bindings
🐍 Found CPython 3.12 at /home/jake/Repos/redoxql/venv/bin/python
📡 Using build options features from pyproject.toml
   Compiling redoxql v0.1.0 (/home/jake/Repos/redoxql)
    Finished `release` profile [optimized] target(s) in 12.59s
📦 Built wheel for CPython 3.12 to /home/jake/Repos/redoxql/target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
pip install --force-reinstall target/wheels/lstore*
Processing ./target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
Installing collected packages: lstore
  Attempting uninstall: lstore
    Found existing installation: lstore 0.1.0
    Uninstalling lstore-0.1.0:
      Successfully uninstalled lstore-0.1.0
Successfully installed lstore-0.1.0

[notice] A new release of pip is available: 24.2 -> 25.0.1
[notice] To update, run: pip install --upgrade pip
make[1]: Leaving directory '/home/jake/Repos/redoxql'
(venv) redoxql ((d48ae4b...)|BISECTING) λ p testM2.py
==========correctness tester===============
DEBUG: Result of select(1, 2, [1,1,1,1,1]) = [[0, 1, 1, 2, 1], [1, 1, 1, 1, 2], [5, 1, 1, 1, 1], [7, 1, 1, 1, 1]]
PASS[0]
DEBUG: Result of select(3, 2, [1,1,1,1,1]) = [[2, 0, 3, 5, 1]]
PASS[1]
PASS[2]
[]
PASS[3]
Wrong[4]
Wrong[5]
Wrong[6]
PASS[7]
5
PASS[8]
==========durability tester================
Checking exam M2 durability
Insert finished
Select finished
Update finished
Aggregate finished
DB is closed
Select finished
Aggregate finished
==========merging tester===================

Total time Taken:  1.13 seconds
(venv) redoxql ((d48ae4b...)|BISECTING) λ git bisect bad
Bisecting: 2 revisions left to test after this (roughly 2 steps)
[91e7fa8be3dab0b412393b8d1a352a12d4eef37d] Add comments
(venv) redoxql ((91e7fa8...)|BISECTING) λ make
make release
make[1]: Entering directory '/home/jake/Repos/redoxql'
maturin build --release
📦 Including license file "/home/jake/Repos/redoxql/LICENSE"
🍹 Building a mixed python/rust project
🔗 Found pyo3 bindings
🐍 Found CPython 3.12 at /home/jake/Repos/redoxql/venv/bin/python
📡 Using build options features from pyproject.toml
   Compiling redoxql v0.1.0 (/home/jake/Repos/redoxql)
    Finished `release` profile [optimized] target(s) in 12.58s
📦 Built wheel for CPython 3.12 to /home/jake/Repos/redoxql/target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
pip install --force-reinstall target/wheels/lstore*
Processing ./target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
Installing collected packages: lstore
  Attempting uninstall: lstore
    Found existing installation: lstore 0.1.0
    Uninstalling lstore-0.1.0:
      Successfully uninstalled lstore-0.1.0
Successfully installed lstore-0.1.0

[notice] A new release of pip is available: 24.2 -> 25.0.1
[notice] To update, run: pip install --upgrade pip
make[1]: Leaving directory '/home/jake/Repos/redoxql'
(venv) redoxql ((91e7fa8...)|BISECTING) λ p testM2.py
==========correctness tester===============
DEBUG: Result of select(1, 2, [1,1,1,1,1]) = [[0, 1, 1, 2, 1], [1, 1, 1, 1, 2], [5, 1, 1, 1, 1], [7, 1, 1, 1, 1]]
PASS[0]
DEBUG: Result of select(3, 2, [1,1,1,1,1]) = [[2, 0, 3, 5, 1]]
PASS[1]
PASS[2]
[]
PASS[3]
Wrong[4]
Wrong[5]
Wrong[6]
PASS[7]
5
PASS[8]
==========durability tester================
Checking exam M2 durability
Insert finished
Select finished
Update finished
Aggregate finished
DB is closed
Select finished
Aggregate finished
==========merging tester===================

Total time Taken:  1.15 seconds
(venv) redoxql ((91e7fa8...)|BISECTING) λ git bisect bad
Bisecting: 0 revisions left to test after this (roughly 1 step)
[322e2e63e7775c6babf68b305cb04680fe066786] Remove py ReturnRecord stuff and move it to Rust
(venv) redoxql ((322e2e6...)|BISECTING) λ make
make release
make[1]: Entering directory '/home/jake/Repos/redoxql'
maturin build --release
📦 Including license file "/home/jake/Repos/redoxql/LICENSE"
🍹 Building a mixed python/rust project
🔗 Found pyo3 bindings
🐍 Found CPython 3.12 at /home/jake/Repos/redoxql/venv/bin/python
📡 Using build options features from pyproject.toml
   Compiling redoxql v0.1.0 (/home/jake/Repos/redoxql)
    Finished `release` profile [optimized] target(s) in 12.48s
📦 Built wheel for CPython 3.12 to /home/jake/Repos/redoxql/target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
pip install --force-reinstall target/wheels/lstore*
Processing ./target/wheels/lstore-0.1.0-cp312-cp312-manylinux_2_34_x86_64.whl
Installing collected packages: lstore
  Attempting uninstall: lstore
    Found existing installation: lstore 0.1.0
    Uninstalling lstore-0.1.0:
      Successfully uninstalled lstore-0.1.0
Successfully installed lstore-0.1.0

[notice] A new release of pip is available: 24.2 -> 25.0.1
[notice] To update, run: pip install --upgrade pip
make[1]: Leaving directory '/home/jake/Repos/redoxql'
(venv) redoxql ((322e2e6...)|BISECTING) λ p testM2.py
==========correctness tester===============
DEBUG: Result of select(1, 2, [1,1,1,1,1]) = [[0, 1, 1, 2, 1], [1, 1, 1, 1, 2], [5, 1, 1, 1, 1], [7, 1, 1, 1, 1]]
PASS[0]
DEBUG: Result of select(3, 2, [1,1,1,1,1]) = [[2, 0, 3, 5, 1]]
PASS[1]
PASS[2]
[]
PASS[3]
Wrong[4]
Wrong[5]
Wrong[6]
PASS[7]
5
PASS[8]
==========durability tester================
Checking exam M2 durability
Insert finished
Select finished
Update finished
Aggregate finished
DB is closed
Select finished
Aggregate finished
==========merging tester===================

Total time Taken:  1.16 seconds
(venv) redoxql ((322e2e6...)|BISECTING) λ git bisect bad
Bisecting: 0 revisions left to test after this (roughly 0 steps)
[234a84102b3230c725e2fb9bd5dd5f23fd2c0a0f] Make an internal select
(venv) redoxql ((234a841...)|BISECTING) λ git bisect ^C
(venv) redoxql ((234a841...)|BISECTING) λ make
make release
make[1]: Entering directory '/home/jake/Repos/redoxql'
maturin build --release
📦 Including license file "/home/jake/Repos/redoxql/LICENSE"
🍹 Building a mixed python/rust project
🔗 Found pyo3 bindings
🐍 Found CPython 3.12 at /home/jake/Repos/redoxql/venv/bin/python
📡 Using build options features from pyproject.toml
   Compiling redoxql v0.1.0 (/home/jake/Repos/redoxql)
error[E0308]: mismatched types
  --> src/query.rs:95:16
   |
78 |     ) -> Option<Vec<Vec<Option<i64>>>> {
   |          ----------------------------- expected `std::option::Option<Vec<Vec<std::option::Option<i64>>>>` because of return type
...
95 |         return out;
   |                ^^^ expected `Option<Vec<Vec<Option<i64>>>>`, found `Vec<Option<RReturnRecord>>`
   |
   = note: expected enum `std::option::Option<Vec<Vec<std::option::Option<i64>>>>`
            found struct `Vec<std::option::Option<RReturnRecord>>`

For more information about this error, try `rustc --explain E0308`.
error: could not compile `redoxql` (lib) due to 1 previous error
💥 maturin failed
  Caused by: Failed to build a native library through cargo
  Caused by: Cargo build finished with "exit status: 101": `env -u CARGO PYO3_ENVIRONMENT_SIGNATURE="cpython-3.12-64bit" PYO3_PYTHON="/home/jake/Repos/redoxql/venv/bin/python" PYTHON_SYS_EXECUTABLE="/home/jake/Repos/redoxql/venv/bin/python" "cargo" "rustc" "--features" "pyo3/extension-module" "--message-format" "json-render-diagnostics" "--manifest-path" "/home/jake/Repos/redoxql/Cargo.toml" "--release" "--lib" "--crate-type" "cdylib"`
make[1]: *** [Makefile:5: release] Error 1
make[1]: Leaving directory '/home/jake/Repos/redoxql'
make: *** [Makefile:2: all] Error 2
(venv) redoxql ((234a841...)|BISECTING) λ ls
__main__.py  exam_tester_m1.py        LICENSE             m3_tester_part_1.py  perf.data.old   python_problematic.py  run_all_tests.sh             test-outputs
benches      exam_tester_m2_part1.py  m1_tester.py        m3_tester_part_2.py  profile.svg     README.md              simple_durability.py         testM1.py
Cargo.lock   exam_tester_m2_part2.py  M2                  main_checking.py     pyproject.toml  redoxdata              simple_update_durability.py  testM2.py
Cargo.toml   exam_tester_m3_part1.py  m2_tester_part1.py  Makefile             pypyvenv        redoxql-m1.zip         src                          tests
docs         exam_tester_m3_part2.py  m2_tester_part2.py  perf.data            python          requirements.txt       target                       venv
(venv) redoxql ((234a841...)|BISECTING) λ git bisect bad
234a84102b3230c725e2fb9bd5dd5f23fd2c0a0f is the first bad commit
commit 234a84102b3230c725e2fb9bd5dd5f23fd2c0a0f (HEAD)
Author: Jake Roggenbuck <jakeroggenbuck2@gmail.com>
Date:   Thu Mar 6 16:21:09 2025 -0800

    Make an internal select

 python/lstore/query.py | 11 +----------
 src/query.rs           | 54 ++++++++++++++++++++++++++++++++++++++++++------------
 2 files changed, 43 insertions(+), 22 deletions(-)
