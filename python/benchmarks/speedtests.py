import matplotlib.pyplot as plt
import numpy as np

"""
Release mode:
===============================================================================

(venv) redoxql (main) λ p __main__.py
Inserting 10k records took:  			 0.007401888000000002
Updating 10k records took:  			 0.018957811999999997
Selecting 10k records took:  			 0.015054888999999995
Aggregate 10k of 100 record batch took:	 0.003300163000000002
Deleting 10k records took:  			 0.002181812999999991
(venv) redoxql (main) λ time p m1_tester.py
Insert finished

real	0m0.112s
user	0m0.108s
sys	0m0.004s
(venv) redoxql (main) λ time p exam_tester_m1.py
Insert finished

real	0m0.268s
user	0m0.254s
sys	0m0.014s


Debug mode:
===============================================================================

(venv) redoxql (main) λ p __main__.py
Inserting 10k records took:  			 0.045038948
Updating 10k records took:  			 0.07168388099999999
Selecting 10k records took:  			 0.059004240999999985
Aggregate 10k of 100 record batch took:	 0.021751132999999978
Deleting 10k records took:  			 0.015860246999999994
(venv) redoxql (main) λ time p m1_tester.py
Insert finished

real	0m0.508s
user	0m0.500s
sys	0m0.004s
(venv) redoxql (main) λ time p exam_tester_m1.py
Insert finished

real	0m1.649s
user	0m1.640s
sys	0m0.004s
"""

operations = [
    "Insert 10k",
    "Update 10k",
    "Select 10k",
    "Aggregate 10k",
    "Delete 10k",
]

tests = ["m1_tester.py (real)", "exam_tester_m1.py (real)"]

release_times = [
    0.007401888000000002,
    0.018957811999999997,
    0.015054888999999995,
    0.003300163000000002,
    0.002181812999999991,
]

debug_times = [
    0.045038948,
    0.07168388099999999,
    0.059004240999999985,
    0.021751132999999978,
    0.015860246999999994,
]

debug_tests = [0.508, 1.649]
release_tests = [0.112, 0.268]

x = np.arange(len(operations))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 5))

bars1 = ax.bar(
    x - width / 2,
    release_times,
    width,
    label='Release Mode',
    color='royalblue',
)

bars2 = ax.bar(
    x + width / 2,
    debug_times,
    width,
    label='Debug Mode',
    color='tomato',
)

ax.set_xlabel("Operation")
ax.set_ylabel("Time (seconds)")
ax.set_title("Performance Comparison: Release Mode vs Debug Mode")
ax.set_xticks(x)
ax.set_xticklabels(operations, rotation=30, ha="right")
ax.legend()

plt.tight_layout()
plt.show()

x = np.arange(len(tests))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 5))

bars1 = ax.bar(
    x - width / 2,
    release_tests,
    width,
    label='Release Mode',
    color='royalblue',
)

bars2 = ax.bar(
    x + width / 2,
    debug_tests,
    width,
    label='Debug Mode',
    color='tomato',
)

ax.set_xlabel("Tests")
ax.set_ylabel("Time (seconds)")
ax.set_title("Performance Comparison: Release Mode vs Debug Mode")
ax.set_xticks(x)
ax.set_xticklabels(tests, rotation=30, ha="right")
ax.legend()

plt.tight_layout()
plt.show()
