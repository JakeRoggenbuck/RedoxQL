import matplotlib.pyplot as plt
import numpy as np

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
