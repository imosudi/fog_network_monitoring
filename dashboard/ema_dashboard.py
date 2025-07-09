# ---------------------- dashboard/ema_dashboard.py ----------------------
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from core.node_dispatcher import get_node_sample
from core.health_calculator import NodeHealthCalculator # Added import

 # Enhanced EMA-based live health dashboard
class EMALiveMultiNodeDashboard:
    def __init__(self, node_ids, max_display=16, ema_beta=0.3):
        self.node_ids = node_ids[:max_display]
        self.base_time = datetime.now(timezone.utc)
        self.time_step = 0
        self.beta = ema_beta
        self.maxlen = 100

        self.health_histories = defaultdict(list)
        self.cpu_histories = defaultdict(list)
        self.plr_histories = defaultdict(list)
        self.rtt_histories = defaultdict(list)
        # self.ema_health = {nid: None for nid in self.node_ids} # Replaced by NodeHealthCalculator

        self.health_calculator = NodeHealthCalculator(ema_beta=self.beta) # Instantiate NodeHealthCalculator

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
        #self.status = {}
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

                #self.lines[node_id], = ax.plot([], [], 'b-', linewidth=2, label='EMA Health')
                self.lines[node_id], = ax.plot([], [], 'b-', linewidth=2, label='Health Score')
                #self.status[node_id], = ax.plot([], [], 'y-', linewidth=2, label='H. Status')
                self.cpu_lines[node_id], = ax.plot([], [], 'r--', alpha=0.7, label='CPU (norm)')
                self.plr_lines[node_id], = ax.plot([], [], 'g:', alpha=0.7, label='PLR (norm)')
                self.rtt_lines[node_id], = ax.plot([], [], 'm-.', alpha=0.7, label='RTT (norm)')

                ax.legend(loc="upper right", fontsize=8)
                ax.set_xlabel('Time Steps', fontsize=8)
                ax.set_ylabel('Normalised Values', fontsize=8)

        plt.tight_layout()

    def update_plot(self, frame):
        self.time_step += 1

        for i, node_id in enumerate(self.node_ids):
            if i >= len(self.axs):
                continue

            sample = get_node_sample(node_id, self.base_time, self.time_step)
            if sample is None:
                continue

            # Prepare node_data for NodeHealthCalculator
            node_data = {
                'node_id': node_id,
                'cpu': sample["cpu"],
                'plr': sample["plr"],
                'rtt': sample["rtt"],
                'anomaly': sample.get('anomaly', 'none') # Ensure anomaly is passed
            }

            # Calculate health metrics using NodeHealthCalculator
            health_metrics = self.health_calculator.calculate_health_metrics(node_data)
            
            weighted_health_score = health_metrics['weighted_health_score'] # 0-100 scale
            ema_health_score = health_metrics['ema_health_score'] # 0-100 scale
            ema_health_threshold = health_metrics['ema_health_threshold'] # 0-100 scale

            # For plotting, we might need to normalize scores to 0-1 if axes limits are fixed at 0-1
            # Or, adjust axes limits to 0-100. For now, let's assume we'll normalize for plotting.
            # The health_histories will store the 0-100 scale EMA score.
            
            # Keep original CPU, PLR, RTT local score calculations for individual metric lines (0-1 scale)
            cpu_plot_score = 1 - (sample["cpu"] / 100)
            plr_plot_score = 1 - min(sample["plr"] / 0.2, 1.0) # Assuming 0.2 is max PLR for normalization
            rtt_plot_score = 1 - min(sample["rtt"] / 400, 1.0) # Assuming 400ms is max RTT for normalization

            self.health_histories[node_id].append(ema_health_score / 100.0) # Store normalized EMA for plot
            self.cpu_histories[node_id].append(cpu_plot_score)
            self.plr_histories[node_id].append(plr_plot_score)
            self.rtt_histories[node_id].append(rtt_plot_score)

            for hist_dict in [self.health_histories, self.cpu_histories, self.plr_histories, self.rtt_histories]:
                hist_dict[node_id] = hist_dict[node_id][-self.maxlen:]

            xs = list(range(len(self.health_histories[node_id])))

            self.lines[node_id].set_data(xs, self.health_histories[node_id]) # Plotting EMA health score (normalized)
            self.cpu_lines[node_id].set_data(xs, self.cpu_histories[node_id])
            self.plr_lines[node_id].set_data(xs, self.plr_histories[node_id])
            self.rtt_lines[node_id].set_data(xs, self.rtt_histories[node_id])

            ax = self.axs[i]
            ax.set_xlim(0, max(self.maxlen, len(xs)))

            # Health status determination will be updated in the next step
            # For now, let's put a placeholder title or use the old logic to avoid errors
            # This print will be removed/updated later.
            print(f"Node: {node_id}, Raw Score: {weighted_health_score:.2f}, EMA Score: {ema_health_score:.2f}, EMA Threshold: {ema_health_threshold:.2f}")

            # Determine health status based on NodeHealthCalculator's metrics
            if ema_health_score < ema_health_threshold:
                ax.set_title(f"{node_id} - FAULTY", fontsize=10, color='#607D8B') # Grey
            # Statuses based on raw weighted_health_score (0-100 scale)
            elif weighted_health_score > 80: # Corresponds to > 0.8 on a 0-1 scale
                ax.set_title(f"{node_id} - Health (GOOD)", fontsize=10, color='green')
            elif weighted_health_score > 60: # Corresponds to > 0.6 on a 0-1 scale
                ax.set_title(f"{node_id} - Health (FAIR)", fontsize=10, color='#FFC107') # Amber
            else: # weighted_health_score <= 60
                ax.set_title(f"{node_id} - Health (POOR)", fontsize=10, color='#FF5722') # Orange-Red


        return [
            line for lines in [
                self.lines, self.cpu_lines, self.plr_lines, self.rtt_lines
            ] for line in lines.values()
        ]

    def run(self):
        ani = FuncAnimation(self.fig, self.update_plot, interval=300, blit=False, cache_frame_data=False)
        plt.show()
        return ani


