import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Keep this in sync with benches/grpc_append
EVENTS_PER_REQUEST = 1  # number of events appended per client request

# Thread variants you ran (match the bench). Edit if you change the bench.
threads = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]

x = []
throughputs = []  # events per second (total across threads)
lower_errors = []  # lower error bars
upper_errors = []  # upper error bars

for t in threads:
    est_path = Path(f"target/criterion/grpc_append/{t}/new/estimates.json")
    if not est_path.exists():
        # Skip missing variants gracefully
        continue
    est = pd.read_json(est_path)
    mean_ns = est.loc['point_estimate', 'mean']
    mean_sec = mean_ns / 1e9
    
    # Extract confidence interval bounds for variance visualization
    mean_ci = est.loc['confidence_interval', 'mean']
    ci_lower_ns = mean_ci['lower_bound']
    ci_upper_ns = mean_ci['upper_bound']
    ci_lower_sec = ci_lower_ns / 1e9
    ci_upper_sec = ci_upper_ns / 1e9

    events_total = EVENTS_PER_REQUEST * t  # total across all threads for this variant
    eps = events_total / mean_sec  # events per second
    
    # Calculate throughput bounds (note: inverse relationship with time)
    eps_upper = events_total / ci_lower_sec  # lower time → higher throughput
    eps_lower = events_total / ci_upper_sec  # upper time → lower throughput

    x.append(t)
    throughputs.append(eps)
    lower_errors.append(eps - eps_lower)
    upper_errors.append(eps_upper - eps)

plt.figure(figsize=(8, 5))
plt.errorbar(x, throughputs, yerr=[lower_errors, upper_errors], marker='o', capsize=5, capthick=2, label='Mean ± 95% CI')
plt.xscale('log')
plt.yscale('log')
plt.xlabel('Clients')
plt.ylabel('Total events/sec')
plt.title(f'UmaDB: Append Operations ({EVENTS_PER_REQUEST} event{"s" if EVENTS_PER_REQUEST > 1 else ""} per request)')
# Show y-axis grid lines and x-axis grid lines only at major ticks (the labeled x ticks)
plt.grid(True, which='both', axis='y')
plt.grid(True, which='major', axis='x')
plt.xticks(x, [str(t) for t in x])
plt.legend()

# Optional: annotate points
for t, eps in zip(x, throughputs):
    plt.annotate(f"{eps:,.0f}", (t, eps), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)

plt.tight_layout()
plt.savefig(f"UmaDB-append-bench-{EVENTS_PER_REQUEST}-per-request.png", format="png", dpi=300)
plt.show()
