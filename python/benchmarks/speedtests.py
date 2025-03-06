import matplotlib.pyplot as plt
from matplotlib import colors
import seaborn as sns
import pandas as pd

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

df_operations = pd.DataFrame(
    {
        "Operation": operations * 2,
        "Time (s)": release_times + debug_times,
        "Mode": ["Release Mode"] * len(release_times) + ["Debug Mode"] * len(debug_times),
    }
)

df_tests = pd.DataFrame(
    {
        "Test": tests * 2,
        "Time (s)": release_tests + debug_tests,
        "Mode": ["Release Mode"] * len(release_tests) + ["Debug Mode"] * len(debug_tests),
    }
)

sns.set_theme(style="darkgrid")

bar_colors = {"Release Mode": "mediumseagreen", "Debug Mode": "deepskyblue"}
edge_colors = {"mediumseagreen": "lightgreen", "deepskyblue": "lightskyblue"}

# Operations Graph
fig, ax = plt.subplots(figsize=(10, 5))
bars = sns.barplot(
    data=df_operations,
    x="Operation",
    y="Time (s)",
    hue="Mode",
    palette=bar_colors,
)

for bar in bars.patches:
    face_color = bar.get_facecolor()[:3]
    for fill, edge in edge_colors.items():
        if tuple(colors.to_rgb(fill)) == face_color:
            bar.set_edgecolor(edge)
            break
    bar.set_linewidth(1.5)

ax.set_xlabel("Operation", color="#DDD")
ax.set_ylabel("Time (seconds)", color="#DDD")
ax.set_title(
    "Performance Comparison: Release Mode vs Debug Mode",
    color="#DDD",
)
plt.xticks(
    range(len(operations)),
    operations,
    rotation=30,
    ha="right",
    color="#DDD",
)
ax.tick_params(axis='y', colors='#DDD')
ax.legend(title="Mode")

ax.set_facecolor("#222222")
fig.patch.set_facecolor("#222222")

legend = ax.get_legend()
legend.get_title().set_color("#222")
for text in legend.get_texts():
    text.set_color("#222")

plt.tight_layout()
plt.show()

# Tests Graph
fig, ax = plt.subplots(figsize=(10, 5))
bars = sns.barplot(
    data=df_tests,
    x="Test",
    y="Time (s)",
    hue="Mode",
    palette=bar_colors,
)

for bar in bars.patches:
    face_color = bar.get_facecolor()[:3]
    for fill, edge in edge_colors.items():
        if tuple(colors.to_rgb(fill)) == face_color:
            bar.set_edgecolor(edge)
            break
    bar.set_linewidth(1.5)

ax.set_xlabel("Tests", color="#DDD")
ax.set_ylabel("Time (seconds)", color="#DDD")
ax.set_title(
    "Performance Comparison: Release Mode vs Debug Mode",
    color="#DDD",
)
plt.xticks(range(len(tests)), tests, rotation=30, ha="right", color="#DDD")
ax.tick_params(axis='y', colors='#DDD')
ax.legend(title="Mode")

ax.set_facecolor("#222222")
fig.patch.set_facecolor("#222222")

legend = ax.get_legend()
legend.get_title().set_color("#222")
for text in legend.get_texts():
    text.set_color("#222")

plt.tight_layout()
plt.show()
