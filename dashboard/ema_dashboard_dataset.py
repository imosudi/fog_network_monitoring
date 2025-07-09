# ---------------------- dashboard/ema_dashboard.py ----------------------
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import pandas as pd

from core.node_dispatcher import get_node_sample


class EMALiveMultiNodeDashboard:
    def __init__(self, node_ids, max_display=16, ema_beta=0.3):
        self.node_ids = node_ids[:max_display]
        self.base_time = datetime.now(timezone.utc)
        self.time_step = 0
        self.beta = ema_beta
        self.maxlen = 100

        # Initialise metric histories
        self.health_histories = defaultdict(list)
        self.cpu_histories = defaultdict(list)
        self.plr_histories = defaultdict(list)
        self.rtt_histories = defaultdict(list)
        self.ema_health = {nid: None for nid in self.node_ids}
        
        # Initialise dataset storage
        self.dataset = {
            'timestamp': [],
            'node_id': [],
            'plr': [],
            'cpu': [],
            'rtt': [],
            'weighted_health_score': [],
            'health_threshold': [],
            'health_difference': [],
            'health_status': [],
            'anomaly_type': []
        }

        # Set up the figure and axes
        n = len(self.node_ids)
        rows = (n + 3) // 4
        cols = min(4, n)
        self.fig, self.axs = plt.subplots(rows, cols, figsize=(16, 4 * rows))
        self.fig.suptitle('Real-Time Network Node Health Dashboard (EMA)', fontsize=16)

        if rows == 1:
            self.axs = [self.axs] if n == 1 else self.axs
        else:
            self.axs = self.axs.flatten()

        self.lines = {}
        self.cpu_lines = {}
        self.plr_lines = {}
        self.rtt_lines = {}

        for i, node_id in enumerate(self.node_ids):
            if i < len(self.axs):
                ax = self.axs[i]
                ax.set_title(f"{node_id} - Health Metrics", fontsize=10)
                ax.set_xlim(0, self.maxlen)
                ax.set_ylim(0, 1)
                ax.grid(True, alpha=0.3)

                self.lines[node_id], = ax.plot([], [], 'b-', linewidth=2, label='Health Score')
                self.cpu_lines[node_id], = ax.plot([], [], 'r--', alpha=0.7, label='CPU (norm)')
                self.plr_lines[node_id], = ax.plot([], [], 'g:', alpha=0.7, label='PLR (norm)')
                self.rtt_lines[node_id], = ax.plot([], [], 'm-.', alpha=0.7, label='RTT (norm)')

                ax.legend(loc="upper right", fontsize=8)
                ax.set_xlabel('Time Steps', fontsize=8)
                ax.set_ylabel('Normalised Values', fontsize=8)

        plt.tight_layout()

    def update_plot(self, frame):
        self.time_step += 1
        current_time = datetime.now(timezone.utc)

        for i, node_id in enumerate(self.node_ids):
            if i >= len(self.axs):
                continue

            sample = get_node_sample(node_id, self.base_time, self.time_step)
            if sample is None:
                continue

            cpu = sample["cpu"]
            plr = sample["plr"]
            rtt = sample["rtt"]
            anomaly_type = sample.get("anomaly_type", "normal")  # Default to "normal" if not specified

            cpu_score = 1 - (cpu / 100)
            plr_score = 1 - min(plr / 0.2, 1.0)
            rtt_score = 1 - min(rtt / 400, 1.0)

            # Weighted health score
            weighted_health_score = 0.3 * cpu_score + 0.3 * plr_score + 0.4 * rtt_score

            # Update EMA health threshold
            if self.ema_health[node_id] is None:
                self.ema_health[node_id] = weighted_health_score
            else:
                self.ema_health[node_id] = (
                    self.beta * weighted_health_score + (1 - self.beta) * self.ema_health[node_id]
                )
            
            health_threshold = self.ema_health[node_id]
            health_difference = weighted_health_score - health_threshold

            # Determine health status based on difference and absolute score
            if health_difference > 0:
                if weighted_health_score > 0.8:
                    health_status = "GOOD"
                elif weighted_health_score > 0.6:
                    health_status = "FAIR"
                else:
                    health_status = "POOR"
            else:
                health_status = "FAULTY"

            # Store metrics in dataset
            self.dataset['timestamp'].append(current_time)
            self.dataset['node_id'].append(node_id)
            self.dataset['plr'].append(plr)
            self.dataset['cpu'].append(cpu)
            self.dataset['rtt'].append(rtt)
            self.dataset['weighted_health_score'].append(weighted_health_score)
            self.dataset['health_threshold'].append(health_threshold)
            self.dataset['health_difference'].append(health_difference)
            self.dataset['health_status'].append(health_status)
            self.dataset['anomaly_type'].append(anomaly_type)

            # Update plot histories
            self.health_histories[node_id].append(weighted_health_score)
            self.cpu_histories[node_id].append(cpu_score)
            self.plr_histories[node_id].append(plr_score)
            self.rtt_histories[node_id].append(rtt_score)

            for hist in [self.health_histories, self.cpu_histories, self.plr_histories, self.rtt_histories]:
                hist[node_id] = hist[node_id][-self.maxlen:]

            xs = list(range(len(self.health_histories[node_id])))

            self.lines[node_id].set_data(xs, self.health_histories[node_id])
            self.cpu_lines[node_id].set_data(xs, self.cpu_histories[node_id])
            self.plr_lines[node_id].set_data(xs, self.plr_histories[node_id])
            self.rtt_lines[node_id].set_data(xs, self.rtt_histories[node_id])

            ax = self.axs[i]
            ax.set_xlim(0, max(self.maxlen, len(xs)))
            
            # Update title with health status and color
            if health_status == "GOOD":
                ax.set_title(f"{node_id} - Health (GOOD)", fontsize=10, color='green')
            elif health_status == "FAIR":
                ax.set_title(f"{node_id} - Health (FAIR)", fontsize=10, color='#FFC107')
            elif health_status == "POOR":
                ax.set_title(f"{node_id} - Health (POOR)", fontsize=10, color='#FF5722')
            else:
                ax.set_title(f"{node_id} - Faulty", fontsize=10, color='#607D8B')

        return [
            line for lines in [
                self.lines, self.cpu_lines, self.plr_lines, self.rtt_lines
            ] for line in lines.values()
        ]

    def get_metrics_dataset(self):
        """Return the collected metrics as a pandas DataFrame"""
        return pd.DataFrame(self.dataset)
    
    def save_metrics_to_csv(self, filename="node_metrics.csv"):
        """Save the collected metrics to a CSV file"""
        df = self.get_metrics_dataset()
        df.to_csv(filename, index=False)
        return df

    def run(self):
        ani = FuncAnimation(self.fig, self.update_plot, interval=300, blit=False, cache_frame_data=False)
        plt.show()
        return ani