# ---------------------- core/node_dispatcher.py ----------------------        
# Enhanced dispatcher with time-based variation and error handling
from datetime import datetime, timedelta, timezone
import json
import os
import time
import numpy as np
import pandas as pd
from core.health_calculator import NodeHealthCalculator
from utils.node_utils import create_node_list


# ---- Enhanced Layered Node Generators with Proper Variability ----
def generate_l1_node_samples(num_nodes=1, base_time=None, fault_rate=0.0005, anomaly_types=None, seed=99, iterations=50):
    base_time = base_time or datetime.now(timezone.utc)
    anomaly_types = anomaly_types or ["overload", "silence", "routing_loop", "fibre_cut"]
    data = []

    for i in range(1, num_nodes + 1):
        node_id = f"L1N_{i:02d}"
        # Create unique random state for each node
        node_rng = np.random.RandomState(seed + i)
        
        # Initialise trend variables for realistic variation
        cpu_trend = node_rng.uniform(-0.1, 0.1)
        plr_trend = node_rng.uniform(-0.0001, 0.0001)
        rtt_trend = node_rng.uniform(-0.2, 0.2)
        
        for t in range(iterations):
            timestamp = base_time + timedelta(seconds=30 * t)
            
            # Add time-based variation and trends
            cpu_base = 30 + cpu_trend * t + node_rng.normal(0, 7)
            plr_base = 0.001 + plr_trend * t + node_rng.beta(1.5, 98) * 0.03
            rtt_base = 70 + rtt_trend * t + node_rng.normal(0, 8)
            
            # Add random walk component for more realistic variation
            cpu_base += node_rng.normal(0, 2)
            plr_base += node_rng.normal(0, 0.0002)
            rtt_base += node_rng.normal(0, 3)
            
            cpu = np.clip(cpu_base, 5, 85)
            plr = np.clip(plr_base, 0.0, 0.03)
            rtt = np.clip(rtt_base, 25, 120)
            anomaly = "none"

            if node_rng.random() < fault_rate:
                anomaly = node_rng.choice(anomaly_types)
                if anomaly == "overload":
                    cpu = 100
                    plr = 0.2
                    rtt = 400
                elif anomaly == "routing_loop":
                    plr = 0.5
                    rtt = 999
                elif anomaly in ["fibre_cut", "silence"]:
                    # Instead of skipping, generate degraded data
                    cpu = 0
                    plr = 1.0
                    rtt = 999

            data.append({
                "timestamp": timestamp.isoformat(),
                "node_id": node_id,
                "cpu": round(cpu, 2),
                "plr": round(plr, 4),
                "rtt": round(rtt, 2),
                "anomaly": anomaly
            })

    return pd.DataFrame(data)

def generate_l2_node_samples(num_nodes=4, base_time=None, fault_rate=0.08, anomaly_types=None, seed=77, iterations=50):
    base_time = base_time or datetime.now(timezone.utc)
    anomaly_types = anomaly_types or ["drift", "overload", "intermittent_loss", "silence"]
    data = []

    for i in range(1, num_nodes + 1):
        node_id = f"L2N_{i:02d}"
        node_rng = np.random.RandomState(seed + i * 10)
        
        # Enhanced variability parameters
        cpu_trend = node_rng.uniform(-0.15, 0.15)
        plr_trend = node_rng.uniform(-0.0002, 0.0002)
        rtt_trend = node_rng.uniform(-0.3, 0.3)
        
        for t in range(iterations):
            timestamp = base_time + timedelta(seconds=30 * t)
            
            # More dynamic base values
            cpu_base = 45 + cpu_trend * t + node_rng.normal(0, 10)
            plr_base = 0.002 + plr_trend * t + node_rng.beta(1.5, 98) * 0.06
            rtt_base = 85 + rtt_trend * t + node_rng.normal(0, 12)
            
            # Random walk for realistic fluctuation
            cpu_base += node_rng.normal(0, 3)
            plr_base += node_rng.normal(0, 0.0003)
            rtt_base += node_rng.normal(0, 4)
            
            cpu = np.clip(cpu_base, 10, 95)
            plr = np.clip(plr_base, 0.0, 0.06)
            rtt = np.clip(rtt_base, 30, 160)
            anomaly = "none"

            if node_rng.random() < fault_rate:
                anomaly = node_rng.choice(anomaly_types)
                if anomaly == "drift":
                    cpu = min(cpu + t * 0.25, 100)
                    plr = min(plr + t * 0.001, 1.0)
                elif anomaly == "overload":
                    cpu = 100
                    plr = 0.15
                    rtt = 300
                elif anomaly == "intermittent_loss":
                    if node_rng.choice([True, False]):
                        plr = 0.8
                        rtt = 500
                elif anomaly == "silence":
                    cpu = 0
                    plr = 1.0
                    rtt = 999

            data.append({
                "timestamp": timestamp.isoformat(),
                "node_id": node_id,
                "cpu": round(cpu, 2),
                "plr": round(plr, 4),
                "rtt": round(rtt, 2),
                "anomaly": anomaly
            })

    return pd.DataFrame(data)

def generate_l3_node_samples(num_nodes=12, base_time=None, fault_rate=0.01, anomaly_types=None, seed=123, iterations=50):
    base_time = base_time or datetime.now(timezone.utc)
    anomaly_types = anomaly_types or ["latency_spike", "throughput_drop", "drift", "offline"]
    data = []

    for i in range(1, num_nodes + 1):
        node_id = f"L3N_{i:02d}"
        node_rng = np.random.RandomState(seed + i * 20)
        
        # Enhanced variability
        cpu_trend = node_rng.uniform(-0.2, 0.2)
        plr_trend = node_rng.uniform(-0.0003, 0.0003)
        rtt_trend = node_rng.uniform(-0.5, 0.5)
        
        for t in range(iterations):
            timestamp = base_time + timedelta(seconds=30 * t)
            
            # Dynamic base values with cycles
            cpu_base = 60 + cpu_trend * t + 5 * np.sin(t * 0.1) + node_rng.normal(0, 12)
            plr_base = 0.003 + plr_trend * t + node_rng.beta(1.8, 97) * 0.08
            rtt_base = 90 + rtt_trend * t + 10 * np.sin(t * 0.15) + node_rng.normal(0, 18)
            
            # Random walk
            cpu_base += node_rng.normal(0, 4)
            plr_base += node_rng.normal(0, 0.0004)
            rtt_base += node_rng.normal(0, 6)
            
            cpu = np.clip(cpu_base, 10, 100)
            plr = np.clip(plr_base, 0.0, 0.08)
            rtt = np.clip(rtt_base, 30, 220)
            anomaly = "none"

            if node_rng.random() < fault_rate:
                anomaly = node_rng.choice(anomaly_types)
                if anomaly == "latency_spike":
                    rtt = min(rtt + node_rng.uniform(100, 150), 300)
                elif anomaly == "throughput_drop":
                    cpu = max(cpu - node_rng.uniform(10, 25), 0)
                    plr = min(plr + node_rng.uniform(0.04, 0.15), 1.0)
                elif anomaly == "drift":
                    cpu = min(cpu + t * 0.3, 100)
                    plr = min(plr + t * 0.001, 1.0)
                elif anomaly == "offline":
                    cpu = 0
                    plr = 1.0
                    rtt = 999

            data.append({
                "timestamp": timestamp.isoformat(),
                "node_id": node_id,
                "cpu": round(cpu, 2),
                "plr": round(plr, 4),
                "rtt": round(rtt, 2),
                "anomaly": anomaly
            })

    return pd.DataFrame(data)

def generate_l4_node_samples(num_nodes=36, base_time=None, fault_rate=0.23, anomaly_types=None, seed=42, iterations=50):
    base_time = base_time or datetime.now(timezone.utc)
    anomaly_types = anomaly_types or ["spike", "drift", "silence", "overload"]
    data = []

    for i in range(1, num_nodes + 1):
        node_id = f"L4N_{i:02d}"
        node_rng = np.random.RandomState(seed + i * 30)
        
        # Maximum variability for L4 nodes
        cpu_trend = node_rng.uniform(-0.3, 0.3)
        plr_trend = node_rng.uniform(-0.0005, 0.0005)
        rtt_trend = node_rng.uniform(-0.8, 0.8)
        
        for t in range(iterations):
            timestamp = base_time + timedelta(seconds=30 * t)
            
            # High variability with multiple cycles
            cpu_base = 50 + cpu_trend * t + 8 * np.sin(t * 0.2) + 3 * np.cos(t * 0.3) + node_rng.normal(0, 15)
            plr_base = 0.005 + plr_trend * t + node_rng.beta(2, 98) * 0.15
            rtt_base = 120 + rtt_trend * t + 20 * np.sin(t * 0.1) + node_rng.normal(0, 30)
            
            # Strong random walk
            cpu_base += node_rng.normal(0, 5)
            plr_base += node_rng.normal(0, 0.0008)
            rtt_base += node_rng.normal(0, 10)
            
            cpu = np.clip(cpu_base, 5, 100)
            plr = np.clip(plr_base, 0.0, 0.15)
            rtt = np.clip(rtt_base, 50, 300)
            anomaly = "none"

            if node_rng.random() < fault_rate:
                anomaly = node_rng.choice(anomaly_types)
                if anomaly == "spike":
                    cpu = min(cpu + node_rng.uniform(30, 50), 100)
                    rtt = min(rtt + node_rng.uniform(80, 140), 300)
                elif anomaly == "drift":
                    cpu = min(cpu + t * 0.3, 100)
                    plr = min(plr + t * 0.001, 1.0)
                elif anomaly == "silence":
                    cpu = 0
                    plr = 1.0
                    rtt = 999
                elif anomaly == "overload":
                    cpu = 100
                    plr = 0.2
                    rtt = 400

            data.append({
                "timestamp": timestamp.isoformat(),
                "node_id": node_id,
                "cpu": round(cpu, 2),
                "plr": round(plr, 4),
                "rtt": round(rtt, 2),
                "anomaly": anomaly
            })

    return pd.DataFrame(data)

# Enhanced dispatcher with time-based variation and error handling
def get_node_sample(node_id, base_time, time_step=0, node_list=None):
    """
    Enhanced dispatcher function with integrated health metrics calculation
    
    Parameters:
    node_id: str - Node identifier
    base_time: datetime - Base timestamp
    time_step: int - Time step for temporal variation
    node_list: DataFrame - Node metadata (optional)
    
    Returns:
    pandas.Series: Complete node sample with health metrics
    """
    # Initialise health calculator
    health_calc = NodeHealthCalculator()
    
    # Load node list if not provided
    if node_list is None:
        node_list_path = "data/node_list.csv"
        if os.path.exists(node_list_path):
            node_list = pd.read_csv(node_list_path)
        else:
            node_list = create_node_list()
    
    # Get node metadata
    node_meta = node_list[node_list['node_id'] == node_id].iloc[0] if not node_list.empty else None
    
    prefix = node_id[:3]
    #print("prefix: ", prefix), time.sleep(3)
    try:
        index = int(node_id.split("_")[1])
    except:
        index = 0
    #print("index: ", index), time.sleep(3)
    dynamic_seed = index * 13 + time_step
    
    try:
        if prefix == "L1N":
            df = generate_l1_node_samples(1, base_time=base_time, seed=dynamic_seed, iterations=1)
        elif prefix == "L2N":
            df = generate_l2_node_samples(1, base_time=base_time, seed=dynamic_seed, iterations=1)
        elif prefix == "L3N":
            df = generate_l3_node_samples(1, base_time=base_time, seed=dynamic_seed, iterations=1)
        elif prefix == "L4N":
            df = generate_l4_node_samples(1, base_time=base_time, seed=dynamic_seed, iterations=1)
        else:
            # Return default values for unknown node types
            sample_data = {
                "timestamp": base_time.isoformat(),
                "node_id": node_id,
                "cpu": 50.0,
                "plr": 0.01,
                "rtt": 100.0,
                "anomaly": "none"
            }
            health_metrics = health_calc.calculate_health_metrics(sample_data)
            sample_data.update(health_metrics)
            return pd.Series(sample_data)
            
        if df.empty:
            # Return default values if no data generated
            sample_data = {
                "timestamp": base_time.isoformat(),
                "node_id": node_id,
                "cpu": 50.0,
                "plr": 0.01,
                "rtt": 100.0,
                "anomaly": "none"
            }
            health_metrics = health_calc.calculate_health_metrics(sample_data)
            sample_data.update(health_metrics)
            return pd.Series(sample_data)
        
        # Get the generated sample
        sample = df.iloc[0].to_dict()
        
        # Calculate health metrics
        health_metrics = health_calc.calculate_health_metrics(sample)
        
        # Merge health metrics with sample data
        sample.update(health_metrics)
        
        # Merge with node metadata if available
        if node_meta is not None:
            for col in node_meta.index:
                if col not in sample:
                    sample[col] = node_meta[col]
        
        return pd.Series(sample)
        
    except Exception as e:
        print(f"Error generating sample for {node_id}: {e}")
        # Return default values with health metrics on error
        sample_data = {
            "timestamp": base_time.isoformat(),
            "node_id": node_id,
            "cpu": 50.0,
            "plr": 0.01,
            "rtt": 100.0,
            "anomaly": "none"
        }
        health_metrics = health_calc.calculate_health_metrics(sample_data)
        sample_data.update(health_metrics)
        return pd.Series(sample_data)
