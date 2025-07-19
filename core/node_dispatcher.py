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
def generate_l1_node_samples(num_nodes=1, base_time=None, fault_rate=0.0, anomaly_types=None, seed=99, iterations=50): # MODIFIED fault_rate from 0.0005 to 0.0
    base_time = base_time or datetime.now(timezone.utc)
    anomaly_types = anomaly_types or ["overload", "silence", "routing_loop", "fibre_cut"]
    data = []

    for i in range(1, num_nodes + 1):
        node_id = f"L1N_{i:02d}"
        # Create unique random state for each node
        node_rng = np.random.RandomState(seed + i)
        
        # Initialise trend variables for realistic variation (REFINED for L1)
        cpu_trend = node_rng.uniform(-0.05, 0.05) # Was (-0.1, 0.1)
        plr_trend = node_rng.uniform(-0.00005, 0.00005) # Was (-0.0001, 0.0001)
        rtt_trend = node_rng.uniform(-0.1, 0.1) # Was (-0.2, 0.2)
        
        for t in range(iterations):
            timestamp = base_time + timedelta(seconds=30 * t)
            
            # Add time-based variation and trends (REFINED for L1)
            cpu_base = 30 + cpu_trend * t + node_rng.normal(0, 2) # Std dev was 7
            plr_base = 0.001 + plr_trend * t + node_rng.beta(1.5, 98) * 0.01 # Multiplier was 0.03
            rtt_base = 70 + rtt_trend * t + node_rng.normal(0, 3) # Std dev was 8
            
            # Add random walk component for more realistic variation (REFINED for L1)
            cpu_base += node_rng.normal(0, 0.5) # Std dev was 2
            plr_base += node_rng.normal(0, 0.00005) # Std dev was 0.0002
            rtt_base += node_rng.normal(0, 1) # Std dev was 3
            
            cpu = np.clip(cpu_base, 10, 25) # MODIFIED clip from (5, 40) to (10, 25)
            plr = np.clip(plr_base, 0.0, 0.01) # MODIFIED upper clip from 0.03 to 0.01
            rtt = np.clip(rtt_base, 25, 70) # MODIFIED upper clip from 120 to 70
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

def generate_l2_node_samples(num_nodes=4, base_time=None, fault_rate=0.009, anomaly_types=None, seed=77, iterations=50): # MODIFIED fault_rate from 0.08 to 0.009
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
            
            # More dynamic base values (REFINED for L2)
            cpu_base = 45 + cpu_trend * t + node_rng.normal(0, 8) # Std dev was 10
            plr_base = 0.002 + plr_trend * t + node_rng.beta(1.5, 98) * 0.06
            rtt_base = 85 + rtt_trend * t + node_rng.normal(0, 12)
            
            # Random walk for realistic fluctuation
            cpu_base += node_rng.normal(0, 3)
            plr_base += node_rng.normal(0, 0.0003)
            rtt_base += node_rng.normal(0, 4)
            
            cpu = np.clip(cpu_base, 10, 70) # MODIFIED clip from (10, 95)
            plr = np.clip(plr_base, 0.0, 0.05) # MODIFIED upper clip from 0.06
            rtt = np.clip(rtt_base, 30, 150) # MODIFIED upper clip from 160
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

def generate_l3_node_samples(num_nodes=12, base_time=None, fault_rate=0.019, anomaly_types=None, seed=123, iterations=50): # MODIFIED fault_rate from 0.01 to 0.019
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
            
            # Dynamic base values with cycles (REFINED for L3)
            cpu_base = 60 + cpu_trend * t + 5 * np.sin(t * 0.1) + node_rng.normal(0, 10) # Std dev was 12
            plr_base = 0.003 + plr_trend * t + node_rng.beta(1.8, 97) * 0.08
            rtt_base = 90 + rtt_trend * t + 10 * np.sin(t * 0.15) + node_rng.normal(0, 18)
            
            # Random walk
            cpu_base += node_rng.normal(0, 4)
            plr_base += node_rng.normal(0, 0.0004)
            rtt_base += node_rng.normal(0, 6)
            
            cpu = np.clip(cpu_base, 15, 70) # MODIFIED clip from (10, 100)
            plr = np.clip(plr_base, 0.0, 0.07) # MODIFIED upper clip from 0.08
            rtt = np.clip(rtt_base, 30, 180) # MODIFIED upper clip from 220
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

def generate_l4_node_samples(num_nodes=36, base_time=None, fault_rate=0.029, anomaly_types=None, seed=42, iterations=50): # MODIFIED fault_rate from 0.23 to 0.029
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
            
            # High variability with multiple cycles (REFINED for L4)
            cpu_base = 50 + cpu_trend * t + 8 * np.sin(t * 0.2) + 3 * np.cos(t * 0.3) + node_rng.normal(0, 12) # Std dev was 15
            plr_base = 0.005 + plr_trend * t + node_rng.beta(2, 98) * 0.15
            rtt_base = 120 + rtt_trend * t + 20 * np.sin(t * 0.1) + node_rng.normal(0, 30)
            
            # Strong random walk
            cpu_base += node_rng.normal(0, 5)
            plr_base += node_rng.normal(0, 0.0008)
            rtt_base += node_rng.normal(0, 10)
            
            cpu = np.clip(cpu_base, 20, 80) # MODIFIED clip from (5, 100)
            plr = np.clip(plr_base, 0.0, 0.10) # MODIFIED upper clip from 0.15
            rtt = np.clip(rtt_base, 40, 220) # MODIFIED clip from (50, 300)
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
def get_node_sample(node_id, base_time, time_step, node_list=None):
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
    #health_calc = NodeHealthCalculator()
    
    # Load node list if not provided
    if node_list is None:
        node_list_path = "data/node_list.csv"
        if os.path.exists(node_list_path):
            node_list = pd.read_csv(node_list_path)
            #print("node_list: ", node_list)
        else:
            node_list = create_node_list()
    
    # Get node metadata
    node_meta = node_list[node_list['node_id'] == node_id].iloc[0] if not node_list.empty else None
    
    #print("node_meta: ", node_meta)
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
            #print("sample_data: ", sample_data)
            #sample_data.update(health_metrics)
            #print("sample_data: ", sample_data); time.sleep(300)
            #health_metrics = health_calc.calculate_health_metrics(sample_data)
            #sample_data.update(health_metrics)
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
            #health_metrics = health_calc.calculate_health_metrics(sample_data)
            #sample_data.update(health_metrics)
            return pd.Series(sample_data)
        
        # Get the generated sample
        sample = df.iloc[0].to_dict()
        
        # Calculate health metrics
        #health_metrics = health_calc.calculate_health_metrics(sample)
        
        # Merge health metrics with sample data
        #sample.update(health_metrics)
        
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
        #health_metrics = health_calc.calculate_health_metrics(sample_data)
        #sample_data.update(health_metrics)
        return pd.Series(sample_data)

#import numpy as np
def generate_gradual_degradation(cycle, node_id, cpu, plr, rtt):
    """
    Generate gradual degradation values using Monte Carlo simulation.
    Degradation is visible but indirect, with continuous decline punctuated by
    temporary improvements that don't prevent overall deterioration.
    
    Args:
        cycle (int): Current iteration count (used for seeding randomness)
        node_id (str): Node identifier (used for seeding)
        cpu (float): Current CPU utilization percentage (0-100)
        plr (float): Current packet loss rate percentage (0-100)
        rtt (float): Current round-trip time in ms
        
    Returns:
        dict: Dictionary containing degraded metrics and anomaly classification
    """
    import numpy as np
    
    # Create node-specific random generator for reproducibility
    rng = np.random.RandomState(seed=hash(f"{node_id}_{cycle}") % (2**32))
    
    # Progressive degradation model - accelerates over time
    cycle_factor = 1 + (cycle * 0.001)  # Degradation accelerates over time
    
    # Enhanced base degradation factors with visible but indirect changes
    cpu_degrade_base = 0.012 * cycle_factor  # More noticeable CPU degradation
    plr_degrade_base = 0.015 * cycle_factor  # More visible packet loss increase
    rtt_degrade_base = 0.008 * cycle_factor  # More apparent latency growth
    
    # State-dependent acceleration (worse state = faster degradation)
    cpu_acceleration = 1 + (cpu / 200)  # Higher CPU usage accelerates degradation
    plr_acceleration = 1 + (plr / 50)   # Higher PLR creates cascade effect
    rtt_acceleration = 1 + (rtt / 500)  # Higher RTT compounds delays
    
    # Calculate stochastic degradation with higher variance for visibility
    cpu_degradation = max(0.001, rng.normal(
        loc=cpu_degrade_base * cpu_acceleration,
        scale=0.008  # Increased variance for more visible changes
    ))
    
    plr_degradation = max(0.001, rng.normal(
        loc=plr_degrade_base * plr_acceleration,
        scale=0.012  # Higher variance for noticeable PLR changes
    ))
    
    rtt_degradation = max(0.001, rng.normal(
        loc=rtt_degrade_base * rtt_acceleration,
        scale=0.006  # Increased RTT variance
    ))
    
    # Apply multiplicative degradation for compound effect
    base_cpu = cpu * (1 + cpu_degradation)
    base_plr = plr * (1 + plr_degradation)
    base_rtt = rtt * (1 + rtt_degradation)
    
    # Occasional temporary improvements (15% chance, but limited scope)
    improvement_chance = 0.15
    improvement_applied = False
    
    if rng.random() < improvement_chance:
        # Temporary improvement that doesn't fully reverse degradation
        improvement_factor = rng.uniform(0.85, 0.95)  # 5-15% improvement
        
        # Apply improvement but ensure it doesn't go below 90% of original values
        base_cpu = max(cpu * 0.9, base_cpu * improvement_factor)
        base_plr = max(plr * 0.9, base_plr * improvement_factor)
        base_rtt = max(rtt * 0.9, base_rtt * improvement_factor)
        improvement_applied = True
    
    # Add periodic stress spikes for more visible degradation patterns
    if cycle % 20 == 0:  # Every 20 cycles, add stress
        stress_multiplier = rng.uniform(1.05, 1.25)
        base_cpu = min(100, base_cpu * stress_multiplier)
        base_plr = min(100, base_plr * stress_multiplier)
        base_rtt = base_rtt * stress_multiplier
    
    # Memory effect - track cumulative degradation for visible trend
    cumulative_degradation = 1 + (cycle * 0.0005)  # Subtle but persistent decline
    
    # Final values with bounds checking
    degraded_cpu = min(100, base_cpu * cumulative_degradation)
    degraded_plr = min(100, base_plr * cumulative_degradation)
    degraded_rtt = base_rtt * cumulative_degradation
    
    # Enhanced anomaly classification with more granular detection
    anomaly_type = "normal"
    severity_score = 0
    
    # Calculate severity with adjusted thresholds for visibility
    cpu_severity = max(0, min(1, (degraded_cpu - 50) / 50))      # Starts at 50%
    plr_severity = max(0, min(1, (degraded_plr - 3) / 20))       # Starts at 3%
    rtt_severity = max(0, min(1, (degraded_rtt - 80) / 150))     # Starts at 80ms
    
    # Weighted severity calculation
    severity_score = (0.4 * cpu_severity + 0.35 * plr_severity + 0.25 * rtt_severity)
    
    # Multi-tier classification for better visibility
    if severity_score > 0.8:
        anomaly_type = "critical_degradation"
    elif severity_score > 0.6:
        anomaly_type = "severe_degradation"
    elif severity_score > 0.4:
        anomaly_type = "moderate_degradation"
    elif severity_score > 0.2:
        anomaly_type = "mild_degradation"
    elif severity_score > 0.05:
        anomaly_type = "early_degradation"
    
    # Override for improvement periods (temporary classification)
    if improvement_applied and severity_score < 0.6:
        anomaly_type = "temporary_recovery"
    
    # Calculate degradation trend (for visibility)
    if cycle > 0:
        cpu_trend = (degraded_cpu - cpu) / max(cpu, 1) * 100
        plr_trend = (degraded_plr - plr) / max(plr, 0.1) * 100
        rtt_trend = (degraded_rtt - rtt) / max(rtt, 1) * 100
        avg_trend = (cpu_trend + plr_trend + rtt_trend) / 3
    else:
        avg_trend = 0
    
    return {
        "cycle": cycle,
        "node_id": node_id,
        "cpu": round(degraded_cpu, 2),
        "plr": round(degraded_plr, 3),
        "rtt": round(degraded_rtt, 1),
        "anomaly_type": anomaly_type,
        "severity_score": round(severity_score, 3),
        "degradation_trend": round(avg_trend, 2),  # Percentage change this cycle
        "cumulative_factor": round(cumulative_degradation, 4),  # Overall degradation
        "improvement_applied": improvement_applied
    }
    
def generate_layer_aware_degradation(cycle, node_id, cpu, plr, rtt, layer=None):
    """
    Generates layer-aware degradation with Monte Carlo simulation.
    Special handling for:
    - Layer 0 (CloudDBServer): No degradation, perfect metrics
    - L1N_01: Core backbone node with near-zero degradation
    - Other nodes: Normal layer-specific degradation patterns
    
    Args:
        cycle: Current simulation cycle
        node_id: Node identifier (used to determine layer if not specified)
        cpu/plr/rtt: Current metric values
        layer: Explicit layer specification (L0-L4), otherwise inferred from node_id
        
    Returns:
        Dictionary with degraded metrics and anomaly classification
    """
    import numpy as np
    
    # Handle special cases first
    if node_id == "CloudDBServer" or layer == 0:
        return {
            "cycle": cycle,
            "node_id": node_id,
            "layer": 0,
            "cpu": 5.0,  # Constant low CPU for cloud DB
            "plr": 0.0,  # Perfect packet delivery
            "rtt": 2.0,  # Minimal latency
            "anomaly": "normal",
            "severity": 0.0,
            "degrade_factor": 0.0
        }
    
    # Determine layer from node_id if not specified
    if layer is None:
        if node_id == "L1N_01":  # Special backbone node
            layer = 0.5  # Special case between L0 and L1
        elif node_id.startswith('L1'): layer = 1
        elif node_id.startswith('L2'): layer = 2
        elif node_id.startswith('L3'): layer = 3
        elif node_id.startswith('L4'): layer = 4
        else: layer = 4  # Default to most volatile
    
    # Create reproducible random state
    rng = np.random.RandomState(seed=hash(f"{node_id}_{cycle}") % (2**32))
    
    # Layer-specific configuration
    layer_config = {
        0.5: {  # Special case for L1N_01 backbone node
            'degrade_rate': 0.0001,  # Near-zero degradation
            'variance': 0.1,
            'recovery_chance': 0.5,  # High chance of self-correction
            'stress_interval': (1000, 2000),  # Extremely rare stress
            'max_cpu': 15,
            'max_plr': 0.001,
            'max_rtt': 30,
            'baseline': {  # Ideal operating parameters
                'cpu': 8.0,
                'plr': 0.0001,
                'rtt': 5.0
            }
        },
        1: {  # Core nodes
            'degrade_rate': 0.003,
            'variance': 0.5,
            'recovery_chance': 0.2,
            'stress_interval': (40, 60),
            'max_cpu': 25,
            'max_plr': 0.01,
            'max_rtt': 70
        },
        2: {  # Aggregation nodes
            'degrade_rate': 0.006,
            'variance': 0.7,
            'recovery_chance': 0.15,
            'stress_interval': (30, 50),
            'max_cpu': 40,
            'max_plr': 0.03,
            'max_rtt': 120
        },
        3: {  # Edge nodes
            'degrade_rate': 0.01,
            'variance': 0.9,
            'recovery_chance': 0.1,
            'stress_interval': (20, 40),
            'max_cpu': 60,
            'max_plr': 0.06,
            'max_rtt': 180
        },
        4: {  # Endpoint devices
            'degrade_rate': 0.015,
            'variance': 1.2,
            'recovery_chance': 0.05,
            'stress_interval': (10, 30),
            'max_cpu': 80,
            'max_plr': 0.1,
            'max_rtt': 250
        }
    }
    cfg = layer_config[layer]
    
    # Special handling for L1N_01 - tends to return to baseline
    if layer == 0.5:
        # Strong regression toward baseline values
        degraded_cpu = cpu * 0.9 + cfg['baseline']['cpu'] * 0.1
        degraded_plr = plr * 0.8 + cfg['baseline']['plr'] * 0.2
        degraded_rtt = rtt * 0.85 + cfg['baseline']['rtt'] * 0.15
    else:
        # Normal degradation process for other nodes
        time_factor = 1 + (cycle * 0.0005 * layer)
        state_factor = 1 + (cpu/100 + plr/0.1 + rtt/200)/3
        
        cpu_degrade = max(0.001, rng.normal(
            loc=cfg['degrade_rate'] * time_factor * state_factor,
            scale=0.005 * cfg['variance']
        ))
        plr_degrade = max(0.001, rng.normal(
            loc=cfg['degrade_rate'] * 1.5 * time_factor * state_factor,
            scale=0.008 * cfg['variance']
        ))
        rtt_degrade = max(0.001, rng.normal(
            loc=cfg['degrade_rate'] * 0.8 * time_factor * state_factor,
            scale=0.003 * cfg['variance']
        ))
        
        degraded_cpu = cpu * (1 + cpu_degrade)
        degraded_plr = plr * (1 + plr_degrade)
        degraded_rtt = rtt * (1 + rtt_degrade)
        
        # Apply temporary improvements if applicable
        if rng.random() < cfg['recovery_chance']:
            recovery_factor = rng.uniform(0.85, 0.98 - (layer * 0.03))
            degraded_cpu = max(cpu * 0.9, degraded_cpu * recovery_factor)
            degraded_plr = max(plr * 0.9, degraded_plr * recovery_factor)
            degraded_rtt = max(rtt * 0.9, degraded_rtt * recovery_factor)
            recovery_applied = True
        else:
            recovery_applied = False
        
        # Apply periodic stress (disabled for L1N_01)
        if cycle % rng.randint(*cfg['stress_interval']) == 0:
            stress_severity = rng.uniform(1.1, 1.3 + (layer * 0.1))
            degraded_cpu = min(100, degraded_cpu * stress_severity)
            degraded_plr = min(1.0, degraded_plr * stress_severity)
            degraded_rtt = degraded_rtt * stress_severity
    
    # Apply layer-specific bounds
    degraded_cpu = min(cfg['max_cpu'], degraded_cpu)
    degraded_plr = min(cfg['max_plr'], degraded_plr)
    degraded_rtt = min(cfg['max_rtt'], degraded_rtt)
    
    # Anomaly classification (simplified for special nodes)
    if layer == 0:
        severity = 0.0
        anomaly = "normal"
    elif layer == 0.5:  # L1N_01
        severity = max(0, min(1, (
            0.4 * (degraded_cpu - 5)/10 +
            0.3 * (degraded_plr - 0.00005)/0.00095 +
            0.3 * (degraded_rtt - 2)/28
        )))
        if severity > 0.3:
            anomaly = "L1_backbone_warning"
        else:
            anomaly = "normal"
    else:
        severity = (
            0.4 * min(1, max(0, (degraded_cpu - (40 - layer*5))/(60 + layer*10))) +
            0.3 * min(1, max(0, (degraded_plr - (0.01 - layer*0.002))/(0.1 + layer*0.02))) +
            0.3 * min(1, max(0, (degraded_rtt - (80 - layer*10))/(150 + layer*30)))
        )
        
        if severity > 0.8:
            anomaly = f"L{layer}_critical"
        elif severity > 0.6:
            anomaly = f"L{layer}_severe"
        elif severity > 0.4:
            anomaly = f"L{layer}_moderate"
        elif severity > 0.2:
            anomaly = f"L{layer}_mild"
        elif recovery_applied:
            anomaly = f"L{layer}_recovery"
        else:
            anomaly = "normal"
    
    return {
        "cycle": cycle,
        "node_id": node_id,
        "layer": int(layer) if layer != 0.5 else "0.5 (L1N_01)",
        "cpu": round(max(0, degraded_cpu), 2),
        "plr": round(max(0, degraded_plr), 4),
        "rtt": round(max(1, degraded_rtt), 1),
        "anomaly": anomaly,
        "severity": round(severity, 3),
        "degrade_factor": 0.0 if layer in [0, 0.5] else round(time_factor * state_factor, 4)
    }  