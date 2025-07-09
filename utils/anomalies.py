# ---------------------- utils/anomalies.py

import random

def random_anomaly():
    anomaly_types = [
        "none", "overload", "silence", "routing_loop", "fibre_cut",
        "drift", "intermittent_loss", "latency_spike", "throughput_drop",
        "offline", "spike"
    ]
    weights = [0.6, 0.05, 0.02, 0.03, 0.01, 0.05, 0.06, 0.06, 0.06, 0.03, 0.03]
    return random.choices(anomaly_types, weights=weights, k=1)[0]