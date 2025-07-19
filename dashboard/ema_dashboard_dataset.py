
# ---------------------- dashboard/ema_dashboard_dataset.py ----------------------
import json
import os
import time
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import pandas as pd

from core.node_dispatcher import generate_layer_aware_degradation, get_node_sample, generate_gradual_degradation
from utils.node_utils import fogNodeCharacterisation

OUTPUT_FILE = 'data/node_health_metric.json'

class EMALiveMultiNodeDashboard:
    def __init__(self, all_node_ids, sample_node_ids, max_display=16, ema_beta=0.3):
        self.all_node_ids = all_node_ids
        self.sample_node_ids = sample_node_ids[:max_display]
        self.base_time = datetime.now(timezone.utc)
        self.time_step = 0
        self.beta = ema_beta
        self.maxlen = 100

        # Initialise metric histories (only for displayed nodes)
        self.health_histories = defaultdict(list)
        self.cpu_histories = defaultdict(list)
        self.plr_histories = defaultdict(list)
        self.rtt_histories = defaultdict(list)
        self.threshold_histories = defaultdict(list)
        self.ema_health = {nid: None for nid in self.all_node_ids}
        self.WEIGHTS            = {"PLR": 0.4, "TTL": 0.4, "CPU": 0.3} #, "Accuracy": 0.2}
        self.STATIC_THRESHOLDS  = {"PLR": 10, "TTL": 200, "CPU": 80}#, "Accuracy": 80}
        self.ALPHA=0.2
        self.iteration_counter = 0

        # Initialise dataset storage for all nodes
        self.dataset = {
            'timestamp': [], 'node_id': [], 'plr': [], 'cpu': [], 'rtt': [],
            'health_metric': [], 'health_threshold': [],
            'health_difference': [], 'health_status': [], 'anomaly_type': [],
            'cpu_score': [], 'plr_score': [], 'rtt_score': []
        }

        # Create a grid of subplots with proper spacing
        n = len(self.sample_node_ids)
        rows = (n + 3) // 4  # Maximum 4 columns
        cols = min(4, n)
        
        # Calculate figure size based on number of nodes
        fig_width = min(20, 5 * cols)
        fig_height = min(17, 4 * rows)
        
        self.fig = plt.figure(figsize=(fig_width, fig_height), constrained_layout=True)
        self.fig.suptitle('Real-Time Network Node Health Dashboard (EMA)', fontsize=16)
        
        # Create a grid specification with proper spacing
        gs = self.fig.add_gridspec(rows, cols, hspace=0.15, wspace=0.05)
        
        self.axs = []
        self.lines = {}
        self.cpu_lines = {}
        self.plr_lines = {}
        self.rtt_lines = {}
        self.threshold_lines = {}

        for i, node_id in enumerate(self.sample_node_ids):
            row = i // cols
            col = i % cols
            ax = self.fig.add_subplot(gs[row, col])
            self.axs.append(ax)
            
            # Set up the plot with borders to make it look like a separate box
            for spine in ax.spines.values():
                spine.set_visible(True)
                spine.set_color('#dddddd')
                spine.set_linewidth(1.5)
            
            # Set background color to make each plot distinct
            ax.set_facecolor('#f8f9fa')
            ax.grid(True, color='white', linestyle='-', linewidth=1)
            
            ax.set_title(f"{node_id}", fontsize=12, pad=10)
            ax.set_xlim(0, self.maxlen)
            ax.set_ylim(0, 1)
            
            # Create plot lines
            self.lines[node_id], = ax.plot([], [], 'b-', linewidth=2, label='Health')
            self.cpu_lines[node_id], = ax.plot([], [], 'r--', alpha=0.7, label='CPU')
            self.plr_lines[node_id], = ax.plot([], [], 'g:', alpha=0.7, label='PLR')
            self.rtt_lines[node_id], = ax.plot([], [], 'm-.', alpha=0.7, label='RTT')
            self.threshold_lines[node_id], = ax.plot([], [], 'k--', alpha=0.5, label='Threshold')
            
            # Add legend inside the plot
            ax.legend(loc="upper right", fontsize=8, framealpha=0.9)
            
            # Add labels only to bottom and leftmost plots
            if col == 0:
                ax.set_ylabel('Score', fontsize=9)
            if row == rows - 1 or i == n - 1:
                ax.set_xlabel('Time Steps', fontsize=9)
                
            # Rotate x-axis labels for better readability
            ax.tick_params(axis='x', labelsize=8)
            ax.tick_params(axis='y', labelsize=8)

    def update_plot(self, frame):
        self.time_step += 1
        current_time = datetime.now(timezone.utc)
        node_metrics = {}
        # Process all nodes but only update plots for sample nodes
        for node_id in self.all_node_ids:
            node_metric = {}
            
            # By default i.e running simulation, the node metrics be loaded from data/initialisation_node_metric.json
            # If data/initialisation_node_metric.json not available bcos not yet generated implying the begining of simulation
            # generate using get_node_sample()
            
            #  Loading data from the persistent storage for node statistics
            try:
                if os.path.exists('data/initialisation_node_metric.json'):
                    with open('data/initialisation_node_metric.json', 'r') as f:
                        node_stats = json.load(f)
                        if node_id in node_stats:
                            sample = node_stats[node_id]
                            #print("sample: ", sample); time.sleep(30)
                        else:
                            # Node not found in JSON, get sample from function
                            sample = get_node_sample(node_id, self.base_time, self.time_step)
                else:
                    # File doesn't exist, get sample from function
                    sample = get_node_sample(node_id, self.base_time, self.time_step)
                    
            except (json.JSONDecodeError, IOError, OSError) as e:
                print(f"Warning: Could not load node statistics from data/initialisation_node_metric.json: {e}")
                sample = get_node_sample(node_id, self.base_time, self.time_step)
            
            
                
            #sample = get_node_sample(node_id, self.base_time, self.time_step)
            if sample is None:
                continue
            #print("sample: ", sample); time.sleep(30)
            try:
                cycle = sample["cycle"]
            except :
                cycle = 0
            #node_id
            cpu = sample["cpu"]
            plr = sample["plr"]
            rtt = sample["rtt"]
            degraded_data = generate_layer_aware_degradation(cycle, node_id, cpu, plr, rtt)
            cpu = degraded_data["cpu"]
            plr = degraded_data["plr"]
            rtt = degraded_data["rtt"]
            anomaly_type = degraded_data["anomaly"]
            #anomaly_type = sample.get("anomaly_type", "normal")

            cpu_score = 1 - (cpu / 100)
            plr_score = 1 - min(plr / 0.2, 1.0)
            rtt_score = 1 - min(rtt / 400, 1.0)

            fognodecharacterisation = fogNodeCharacterisation(self.WEIGHTS, self.STATIC_THRESHOLDS, self.ALPHA)
            
            # Update stats, compute health
            updated_stats = fognodecharacterisation.muAndsigma_per_node(
                node_id, 
                plr, 
                rtt, 
                cpu, 
                #metrics["Accuracy"]
            )
            node_metric['cycle']        = self.time_step 
            print("node_id: ", node_id)
            node_metric["timestamp"] = current_time.isoformat()
            node_metric["node_id"] = node_id
            node_metric["plr"] = plr
            node_metric["cpu"] = cpu
            node_metric["rtt"] = rtt
            node_metric['PLR'] = {}
            node_metric['TTL'] = {}
            node_metric['CPU'] = {}
            node_metric['PLR']['n'] = self.iteration_counter
            node_metric['TTL']['n'] = self.iteration_counter
            node_metric['CPU']['n'] = self.iteration_counter
            
            
            node_metrics[node_id] = node_metric
            node_metric['PLR']['mu']        = updated_stats["PLR"]["mu"] 
            node_metric['PLR']['sigma']     = updated_stats["PLR"]["sigma"]
            node_metric['PLR']["sum_sq"]    = updated_stats["PLR"]["sum_sq"]
            node_metric['TTL']['mu']        = updated_stats["TTL"]["mu"] 
            node_metric['TTL']['sigma']     = updated_stats["TTL"]["sigma"]
            node_metric['TTL']["sum_sq"]    = updated_stats["TTL"]["sum_sq"]
            node_metric['CPU']['mu']        = updated_stats["CPU"]["mu"]
            node_metric['CPU']['sigma']     = updated_stats["CPU"]["sigma"]
            node_metric['CPU']["sum_sq"]    = updated_stats["CPU"]["sum_sq"]
            
            #print("node_id, plr, rtt, cpu: ", node_id, plr, rtt,    cpu)
            #print("updated_stats: ", updated_stats); #time.sleep(300)
            health_metric = fognodecharacterisation.healthMetric(node_id,
                plr, 
                rtt, 
                cpu, 
                #metrics["Accuracy"],
                updated_stats["PLR"]["mu"], updated_stats["PLR"]["sigma"],
                updated_stats["TTL"]["mu"], updated_stats["TTL"]["sigma"],
                updated_stats["CPU"]["mu"], updated_stats["CPU"]["sigma"] #,
                #updated_stats["Accuracy"]["mu"], updated_stats["Accuracy"]["sigma"]
            )
            # Weighted health score
            weighted_health_score = 0.3 * cpu_score + 0.3 * plr_score + 0.4 * rtt_score

            # Update EMA health threshold
            # health_threshold(t+1) = β * weighted_health_score(t) + (1 - β) * health_threshold(t)
            # health_threshold = self.beta * weighted_health_score + (1 - self.beta) * self.ema_health[node_id]
            if self.ema_health[node_id] is None:
                self.ema_health[node_id] = weighted_health_score
            else:
                self.ema_health[node_id] = (
                    self.beta * weighted_health_score + (1 - self.beta) * self.ema_health[node_id]
                )
            
            health_threshold = self.ema_health[node_id]
            print("health_threshold: ", health_threshold, "health_metric: ", health_metric)
            try:
                health_difference = health_metric - health_threshold
            except:
                health_difference = 0
                
            # Determine health status
            if health_difference > 0 or health_difference == 0:
                health_status = "GOOD"
                """if weighted_health_score > 0.8:
                    health_status = "GOOD"
                elif weighted_health_score > 0.6:
                    health_status = "FAIR"
                else:
                    health_status = "POOR" """
            else:
                health_status = "FAULTY"

            
            
            node_metric["health_threshold"] = health_threshold
            node_metric["health_metric"] = health_metric
            node_metric["health_status"] = health_status
        
            #("node_metric: ", node_metric)
            # Store metrics
            self.dataset['timestamp'].append(current_time.isoformat())
            self.dataset['node_id'].append(node_id)
            self.dataset['plr'].append(plr)
            self.dataset['cpu'].append(cpu)
            self.dataset['rtt'].append(rtt)
            self.dataset['health_metric'].append(health_metric)
            self.dataset['health_threshold'].append(health_threshold)
            self.dataset['health_difference'].append(health_difference)
            self.dataset['health_status'].append(health_status)
            self.dataset['anomaly_type'].append(anomaly_type)
            self.dataset['cpu_score'].append(cpu_score)
            self.dataset['plr_score'].append(plr_score)
            self.dataset['rtt_score'].append(rtt_score)

            # Only update plot histories for sample nodes
            if node_id in self.sample_node_ids:
                self.health_histories[node_id].append(health_metric)
                self.cpu_histories[node_id].append(cpu_score)
                self.plr_histories[node_id].append(plr_score)
                self.rtt_histories[node_id].append(rtt_score)
                self.threshold_histories[node_id].append(health_threshold)

                for hist in [self.health_histories, self.cpu_histories, 
                           self.plr_histories, self.rtt_histories,
                           self.threshold_histories]:
                    hist[node_id] = hist[node_id][-self.maxlen:]
        self.iteration_counter+=1
            
        #print("node_metrics: ", node_metrics); time.sleep(3)
        with open('data/initialisation_node_metric.json', 'w') as fp:
            json.dump(node_metrics, fp, indent=4) 
        # Update plots only for sample nodes
        for i, node_id in enumerate(self.sample_node_ids):
            if i >= len(self.axs) or node_id not in self.health_histories:
                continue

            xs = list(range(len(self.health_histories[node_id])))

            self.lines[node_id].set_data(xs, self.health_histories[node_id])
            self.cpu_lines[node_id].set_data(xs, self.cpu_histories[node_id])
            self.plr_lines[node_id].set_data(xs, self.plr_histories[node_id])
            self.rtt_lines[node_id].set_data(xs, self.rtt_histories[node_id])
            self.threshold_lines[node_id].set_data(xs, self.threshold_histories[node_id])

            ax = self.axs[i]
            ax.set_xlim(0, max(self.maxlen, len(xs)))
            
            # Get latest health status
            latest_status = "UNKNOWN"
            for idx in reversed(range(len(self.dataset['node_id']))):
                if self.dataset['node_id'][idx] == node_id:
                    latest_status = self.dataset['health_status'][idx]
                    break

            # Update title with status and color
            status_colors = {
                "GOOD": 'green',
                "FAIR": '#FFC107',  # Amber
                "POOR": '#FF5722',  # Orange
                "FAULTY": '#F44336'  # Red
            }
            ax.set_title(f"{node_id} - {latest_status}", 
                        fontsize=12, 
                        color=status_colors.get(latest_status, 'black'),
                        fontweight='bold')
            
            # Change border color based on status
            for spine in ax.spines.values():
                spine.set_color(status_colors.get(latest_status, '#dddddd'))

        return [
            line for lines in [
                self.lines, self.cpu_lines, self.plr_lines, 
                self.rtt_lines, self.threshold_lines
            ] for line in lines.values()
        ]

    def get_metrics_dataset(self):
        """Return the collected metrics as a pandas DataFrame"""
        return pd.DataFrame(self.dataset)
    
    def save_metrics_to_csv(self,  filename="data/node_metrics_data.csv"):
        """Save the collected metrics to a CSV file"""
        df = self.get_metrics_dataset()
        df.to_csv(filename, index=False)
        return df

    def save_node_metrics_json(self, node_metrics, output_file):
        """Save processed metrics to JSON file"""
        try:
            with open(output_file, 'w') as fp:
                json.dump(node_metrics, fp, indent=4)
            print(f"Results saved to {output_file}")
        except Exception as e:
            print(f"Error saving results: {e}")
            
    def run(self):
        ani = FuncAnimation(self.fig, self.update_plot, interval=300, blit=False, cache_frame_data=False)
        plt.show()
        return ani
    
    
        