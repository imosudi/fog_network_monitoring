        
# Enhanced dispatcher with time-based variation and error handling
from datetime import datetime, timedelta, timezone
import json
import os
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

def create_node_list():
    """Create node list for Nigerian crop farming fog network"""
    node_data = []
    
    # Nigerian crop zones based on agro-ecological regions
    crop_zones = [
        "Cereal_Belt",      # Northern Nigeria (Grains)
        "Rootcrop_Zone",     # Middle Belt
        "Vegetable_Belt",    # Urban peri-areas
        "Treecrop_Zone"      # Southern Nigeria
    ]
    
    # Nigerian crop sub-zones
    sub_zones = {
        "Cereal_Belt": ["Sorghum_Field", "Millet_Field", "Maize_Field"],
        "Rootcrop_Zone": ["Cassava_Plot", "Yam_Mound", "SweetPotato_Bed"],
        "Vegetable_Belt": ["Tomato_Plot", "Pepper_Field", "Onion_Beds"],
        "Treecrop_Zone": ["OilPalm_Grove", "Cocoa_Plot", "Plantation"]
    }
    
    # Common Nigerian sensor placements
    sensor_placements = [
        "Topsoil_Monitor", 
        "Crop_Canopy",
        "Fertilizer_Node",
        "Irrigation_Point",
        "Weather_Station",
        "Pest_Monitor"
    ]
    
    # --- Node Definitions ---
    # Layer 0: Farm Central (Local Server)
    node_data.append({
        "node_id": "FarmHQ_Server",
        "tier": "L0",
        "location": "Farm_Office",
        "node_type": "Farm_Management_System",
        "management_zone": "Entire_Farm",
        "crop_focus": "All_Crops",
        "criticality": "Mission_Critical",
        "power_source": "Grid_Backup_Solar"
    })
    
    # Layer 1: Field Coordinator
    node_data.append({
        "node_id": "L1N_01",
        "tier": "L1",
        "location": "Main_Storage_Barn",
        "node_type": "Field_Coordinator",
        "management_zone": "Entire_Farm",
        "crop_focus": "All_Crops",
        "criticality": "Ultra_High",
        "power_source": "Solar_Diesel_Hybrid"
    })
    
    # Layer 2: Zone Managers (4 nodes)
    for i in range(1, 5):
        zone = crop_zones[i-1]
        node_data.append({
            "node_id": f"L2N_{i:02d}",
            "tier": "L2",
            "location": f"{zone}_Control",
            "node_type": "Zone_Supervisor",
            "management_zone": zone,
            "crop_focus": zone.replace('_Zone','').replace('_Belt',''),
            "criticality": "High",
            "power_source": "Solar_Battery"
        })
    
    # Layer 3: Field Units (12 nodes)
    for i in range(1, 13):
        zone = crop_zones[(i-1)//3]
        subzone = sub_zones[zone][(i-1)%3]
        node_data.append({
            "node_id": f"L3N_{i:02d}",
            "tier": "L3",
            "location": subzone,
            "node_type": "Field_Unit",
            "management_zone": zone,
            "crop_focus": subzone.split('_')[0],
            "criticality": "Medium",
            "power_source": "Solar_Only"
        })
    
    # Layer 4: Crop Sensors (36 nodes)
    nigerian_sensors = [
        "Soil_Moisture", 
        "Nutrient_Level",
        "Growth_Monitor",
        "Disease_Alert",
        "Rain_Gauge",
        "Temperature_Probe"
    ]
    
    for i in range(1, 37):
        parent_l3 = (i-1)//3 + 1
        zone = crop_zones[(parent_l3-1)//3]
        crop_type = sub_zones[zone][(parent_l3-1)%3].split('_')[0]
        
        node_data.append({
            "node_id": f"L4N_{i:02d}",
            "tier": "L4",
            "location": f"{crop_type}_{sensor_placements[i%6]}",
            "node_type": "Field_Sensor",
            "management_zone": zone,
            "crop_focus": crop_type,
            "sensor_type": nigerian_sensors[i%6],
            "criticality": "Standard",
            "power_source": "Battery_Solar"
        })
    
    df = pd.DataFrame(node_data)
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/node_list.csv", index=False)
    
    # Nigerian farming metadata
    with open("data/nafmis_metadata.json", "w") as f:  # NAFMIS = Nigerian Agricultural Farm Management Information System
        json.dump({
            "nigerian_crop_varieties": {
                "Cereal": ["Sorghum (Farafara)", "Millet (Gero)", "Maize (Masara)"],
                "Rootcrop": ["Cassava (Rodo)", "Yam (Doya)", "Sweet Potato (Dankali)"],
                "Vegetable": ["Tomato (Tumatir)", "Pepper (Tattasai)", "Onion (Albasa)"],
                "Treecrop": ["Oil Palm (Kwakwa)", "Cocoa (Koko)", "Rubber (Roba)"]
            },
            "local_sensor_specs": {
                "Soil_Moisture": "0-100% (Suitable for sandy loam soils)",
                "Nutrient_Level": "NPK+ for tropical soils",
                "Growth_Monitor": "Adapted for West African crop varieties",
                "Disease_Alert": "Common Nigerian crop pathogens",
                "Rain_Gauge": "Tropical rainfall intensity measurement",
                "Temperature_Probe": "25-45°C range"
            },
            "power_notes": "Adapted for frequent power fluctuations"
        }, f, indent=2)
    
    return df


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
    index = int(node_id.split("_")[1])
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
