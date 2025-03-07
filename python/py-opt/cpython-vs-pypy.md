# C Python vs Pypy

Here we are testing the difference between C Python and Pypy

## Pypy

```
(pypyvenv) redoxql (opt) λ p testM2.py
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

Total time Taken:  1.65 seconds
(pypyvenv) redoxql (opt) λ p testM2.py
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

Total time Taken:  1.66 seconds
(pypyvenv) redoxql (opt) λ
```

```
(pypyvenv) benchmarks (opt) λ p one_hunderd_thousand_ops.py Time taken:  1.0947623252868652
Time taken:  1.2844014167785645
Time taken:  1.4248299598693848
Time taken:  1.4113421440124512
Time taken:  1.4108021259307861
Time taken:  1.4684548377990723
Time taken:  1.4111592769622803
Time taken:  1.4579296112060547
Time taken:  1.3900971412658691
Time taken:  1.437528133392334
Time taken:  1.3875908851623535
Time taken:  1.4148144721984863
Time taken:  1.3881783485412598
Time taken:  1.4330570697784424
Time taken:  1.3624420166015625
Time taken:  1.3890328407287598
Time taken:  1.3693325519561768
Time taken:  1.4034101963043213
Time taken:  1.3789188861846924
Time taken:  1.4398105144500732
Time taken:  1.3823513984680176
Time taken:  1.4009954929351807
Time taken:  1.3668043613433838
Time taken:  1.4006311893463135
Time taken:  1.3749990463256836
Time taken:  1.3933594226837158
Time taken:  1.3697967529296875
Time taken:  1.3945369720458984
Time taken:  1.3686223030090332
Time taken:  1.4087376594543457
Mean:  1.3872909784317016
Stand. Dev:  0.06497411327364458
(pypyvenv) benchmarks (opt) λ
```

## C Python (default)

```
(venv) redoxql (opt) λ p testM2.py
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

Total time Taken:  0.90 seconds
(venv) redoxql (opt) λ p testM2.py
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

Total time Taken:  0.91 seconds
```

```
(venv) benchmarks (opt) λ p one_hunderd_thousand_ops.py
Time taken:  0.3328847885131836
Time taken:  0.38111376762390137
Time taken:  0.38730382919311523
Time taken:  0.3802917003631592
Time taken:  0.38680338859558105
Time taken:  0.38395190238952637
Time taken:  0.38918185234069824
Time taken:  0.3849310874938965
Time taken:  0.40045881271362305
Time taken:  0.3932836055755615
Time taken:  0.41007041931152344
Time taken:  0.4063296318054199
Time taken:  0.3800044059753418
Time taken:  0.37378382682800293
Time taken:  0.3861417770385742
Time taken:  0.39180660247802734
Time taken:  0.38900232315063477
Time taken:  0.3806729316711426
Time taken:  0.38063955307006836
Time taken:  0.3836021423339844
Time taken:  0.3839859962463379
Time taken:  0.3850264549255371
Time taken:  0.3855717182159424
Time taken:  0.38179993629455566
Time taken:  0.391416072845459
Time taken:  0.38553810119628906
Time taken:  0.3814077377319336
Time taken:  0.3887214660644531
Time taken:  0.41253137588500977
Time taken:  0.38439369201660156
Mean:  0.3860883633295695
Stand. Dev:  0.013426947271149016
(venv) benchmarks (opt) λ
```

## Results

#### Pypy results:

- 1.65 seconds for `testM2.py`
- 1.66 seconds for `testM2.py` (Second run)
- Mean:  1.38729, Stand. Dev:  0.06497 for `one_hunderd_thousand_ops.py`

#### C Python results:

- 0.90 seconds for `testM2.py`
- 0.91 seconds for `testM2.py` (Second run)
- Mean:  0.38609 Stand. Dev:  0.01343 for `one_hunderd_thousand_ops.py`

The standard C Python is much faster. My hypothesis for why is because the JIT starts a more startup time but is better in the long term whereas C Python doesn't have a high startup time.

For this reason, we will stick with C Python.

This was actually somewhat surprising.
