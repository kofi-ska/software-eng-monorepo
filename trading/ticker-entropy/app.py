import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import entropy

# -------------------------------
# 1. Generate Synthetic Trade Batches
# -------------------------------
# Each batch = group of trades over a short interval.
# We'll simulate buys (+1) and sells (-1) with random volumes.
np.random.seed(42)


def simulate_batch(n_trades=200, bias=0.5):
    # bias > 0.5 => more buys, bias < 0.5 => more sells
    directions = np.random.choice([1, -1], size=n_trades, p=[bias, 1 - bias])
    volumes = np.random.exponential(scale=1.0, size=n_trades)
    return pd.DataFrame({"direction": directions, "volume": volumes})


batches = [simulate_batch(bias=np.random.uniform(0.4, 0.6)) for _ in range(50)]


# -------------------------------
# 2. Entropy Computation per Batch
# -------------------------------
def batch_entropy(df: pd.DataFrame):
    # Directional entropy: how mixed are buys vs sells
    direction_counts = df["direction"].value_counts(normalize=True)
    dir_entropy = entropy(direction_counts, base=2)

    # Volume entropy: normalize volumes into bins
    vol_bins = np.histogram(df["volume"], bins=10, density=True)[0]
    vol_entropy = entropy(vol_bins + 1e-9, base=2)  # +eps to avoid log(0)

    # Combined entropy (weighted)
    total_entropy = 0.6 * dir_entropy + 0.4 * vol_entropy
    return dir_entropy, vol_entropy, total_entropy


results = [batch_entropy(b) for b in batches]
dir_e, vol_e, total_e = map(np.array, zip(*results))

# -------------------------------
# 3. Plot the Entropy Graph
# -------------------------------
plt.figure(figsize=(10, 5))
plt.plot(dir_e, label="Directional Entropy", linewidth=1.8)
plt.plot(vol_e, label="Volume Entropy", linewidth=1.8)
plt.plot(total_e, label="Total Entropy", color="black", linewidth=2.2)
plt.title("Market Structure Entropy Across Transaction Batches")
plt.xlabel("Batch Index")
plt.ylabel("Entropy (bits)")
plt.legend()
plt.grid(True)
plt.show()
