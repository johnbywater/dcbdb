import pandas as pd
import matplotlib.pyplot as plt

threads = [1,2,4,8,16,32,64,128,256,512,1024]
means = []
for t in threads:
    est = pd.read_json(f"target/criterion/grpc_stream_read/{t}/new/estimates.json")
    means.append(est.loc['point_estimate','mean'])

plt.plot(threads, means, marker='o')
plt.xscale('log')
plt.xlabel('Tokio worker threads')
plt.ylabel('Mean time per iter (ns)')
plt.title('gRPC stream read benchmark')
plt.grid(True, which='both')
plt.show()