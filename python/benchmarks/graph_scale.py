import matplotlib.pyplot as plt


counts = [10, 100, 1000, 10000, 100000, 1000000, 10000000]
execution_times = [0.0000, 0.0001, 0.0007, 0.0101, 0.0880, 1.1142, 9.8154]

plt.figure(figsize=(10, 6))
plt.plot(counts, execution_times, marker='o')
plt.xscale('log')
plt.yscale('log')
plt.title('Time Taken to Insert Records vs. Number of Records')
plt.xlabel('Number of Records (log scale)')
plt.ylabel('Time Taken (seconds, log scale)')
plt.grid(True, which="both", linestyle='--', linewidth=0.5)
plt.xticks(counts)
plt.show()
