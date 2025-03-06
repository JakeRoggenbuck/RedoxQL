import matplotlib.pyplot as plt

counts = [10, 100, 1000, 10000, 100000, 1000000, 10000000]
insert_execution_times = [0.0000, 0.0001, 0.0007, 0.0101, 0.0880, 1.1142, 9.8154]
update_execution_times = [0.0000, 0.0002, 0.0017, 0.0207, 0.2312, 2.6942, 26.4918]

plt.figure(figsize=(10, 6))

plt.style.use("dark_background")
fig = plt.gcf()
fig.patch.set_facecolor("#222222")
ax = plt.gca()
ax.set_facecolor("#222222")

plt.plot(
    counts,
    insert_execution_times,
    marker="o",
    color="mediumseagreen",
    linewidth=2,
    markersize=8,
    markeredgecolor="lightgreen",
    markerfacecolor="mediumseagreen",
)

plt.xscale("log")
plt.yscale("log")

plt.title("Time Taken to Insert Records vs. Number of Records", color="#DDD", pad=20)
plt.xlabel("Number of Records (log scale)", color="#DDD")
plt.ylabel("Time Taken (seconds, log scale)", color="#DDD")

plt.grid(True, which="both", linestyle="--", linewidth=0.5, color="#444444", alpha=0.7)

plt.xticks(counts, color="#DDD")
plt.yticks(color="#DDD")

plt.tight_layout()
plt.show()

plt.style.use("dark_background")
fig = plt.gcf()
fig.patch.set_facecolor("#222222")
ax = plt.gca()
ax.set_facecolor("#222222")

plt.plot(
    counts,
    update_execution_times,
    marker="o",
    color="mediumseagreen",
    linewidth=2,
    markersize=8,
    markeredgecolor="lightgreen",
    markerfacecolor="mediumseagreen",
)

plt.xscale("log")
plt.yscale("log")

plt.title("Time Taken to Update Records vs. Number of Records", color="#DDD", pad=20)
plt.xlabel("Number of Records (log scale)", color="#DDD")
plt.ylabel("Time Taken (seconds, log scale)", color="#DDD")

plt.grid(True, which="both", linestyle="--", linewidth=0.5, color="#444444", alpha=0.7)

plt.xticks(counts, color="#DDD")
plt.yticks(color="#DDD")

plt.tight_layout()
plt.show()
