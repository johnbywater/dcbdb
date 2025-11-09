import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Keep this in sync with benches/grpc_append_with_readers_bench.rs
EVENTS_PER_ITER = 1  # number of events appended per iteration by a single writer client
READER_COUNT = 4     # number of background readers running during the append bench

# Thread variants you ran (match the bench). Edit if you change the bench.
threads = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]

x = []
throughputs = []  # events per second (total across threads)

for t in threads:
    est_path = Path(f"target/criterion/grpc_append_4readers/{t}/new/estimates.json")
    if not est_path.exists():
        # Skip missing variants gracefully
        continue
    est = pd.read_json(est_path)
    mean_ns = est.loc['point_estimate', 'mean']
    mean_sec = mean_ns / 1e9

    events_total = EVENTS_PER_ITER * t  # total across all writer threads for this variant
    eps = events_total / mean_sec  # events per second

    x.append(t)
    throughputs.append(eps)

plt.figure(figsize=(8, 5))
plt.plot(x, throughputs, marker='o')
plt.xscale('log')
plt.yscale('log')
plt.xlabel('Writer clients')
plt.ylabel('Total ops/sec')
plt.title(f'UmaDB Append with {READER_COUNT} Concurrent Readers')
# Show y-axis grid lines and x-axis grid lines only at major ticks (the labeled x ticks)
plt.grid(True, which='both', axis='y')
plt.grid(True, which='major', axis='x')
plt.xticks(x, [str(t) for t in x])

# Optional: annotate points
for t, eps in zip(x, throughputs):
    plt.annotate(f"{eps:,.0f}", (t, eps), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)

plt.tight_layout()
plt.show()
