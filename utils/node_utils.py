
# ---------------------- utils/node_utils.py ----------------------
from collections import defaultdict
import json
import math
import os
import random
import time
from typing import Dict
import pandas as pd

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
    # Layer 0: Cloud Database Server
    node_data.append({
        "node_id": "CloudDB_Server",
        "tier": "L0",
        "location": "Cloud_Provider",
        "node_type": "Cloud_Database",
        "management_zone": "Entire_Network",
        "crop_focus": "All_Crops",
        "criticality": "Mission_Critical",
        "power_source": "Grid_Backup"
    })
    
    # Layer 1: Farm Central (Local Server)
    node_data.append({
        "node_id": "L1N_01", #"FarmHQ_Server"
        "tier": "L1",
        "location": "Farm_Office", #"location": "Main_Storage_Barn",
        "node_type": "Farm_Management_System", #"node_type": "Field_Coordinator",
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


def select_any_random_nodes(node_ids, n):
    """
    Randomly select N items from a list of node_ids.
    
    Args:
        node_ids: List of strings (node identifiers)
        n: Number of items to select
    
    Returns:
        List of randomly selected node_ids
    """
    if n > len(node_ids):
        raise ValueError(f"Cannot select {n} items from a list of {len(node_ids)} items")
    
    return random.sample(node_ids, n)

def select_random_nodes(node_ids, n):
    """
    Randomly select N items from a list of node_ids, always including:
    - Layer 0 node ('CloudDB_Server')
    - Layer 1 node ('L1N_01')
    - All Layer 2 nodes ('L2N_01', 'L2N_02', 'L2N_03', 'L2N_04')
    The remaining nodes are randomly selected from layers 3 and 4.
    
    Args:
        node_ids: List of strings (node identifiers)
        n: Number of items to select
    
    Returns:
        List of selected node_ids
    """
    # Define the mandatory nodes
    mandatory_nodes = ['CloudDB_Server', 'L1N_01', 'L2N_01', 'L2N_02', 'L2N_03', 'L2N_04']
    
    # Calculate how many additional nodes we need to select
    remaining_n = n - len(mandatory_nodes)
    
    if remaining_n < 0:
        raise ValueError(f"n must be at least {len(mandatory_nodes)} to include all mandatory nodes")
    if n > len(node_ids):
        raise ValueError(f"Cannot select {n} items from a list of {len(node_ids)} items")
    
    # Get all layer 3 and 4 nodes (all nodes that aren't in mandatory_nodes)
    remaining_nodes = [node for node in node_ids if node not in mandatory_nodes]
    
    # Select random nodes from remaining nodes
    selected_random_nodes = random.sample(remaining_nodes, remaining_n)
    
    # Combine mandatory nodes with randomly selected nodes
    return mandatory_nodes + selected_random_nodes


class fogNodeCharacterisation(object):
    """Enhanced node characterization with proper statistical tracking"""
    
    def __init__(self, WEIGHTS, STATIC_THRESHOLDS, ALPHA, *args):
        self.G = {}
        self.WEIGHTS = WEIGHTS
        self.STATIC_THRESHOLDS = STATIC_THRESHOLDS
        if not (0 < ALPHA < 1):
            raise ValueError("alpha must be between 0 and 1.")
        self.ALPHA = ALPHA
        
        # Persistent storage for node statistics
        try:
            if os.path.exists('data/initialisation_node_metric.json'):
                with open('data/initialisation_node_metric.json', 'r') as f:
                    self.node_stats = json.load(f)
            else:
                self.node_stats = {}
        except (json.JSONDecodeError, IOError, OSError) as e:
            print(f"Warning: Could not load node statistics from data/initialisation_node_metric.json: {e}")
            self.node_stats = {}
    
        self.current_thresholds = {}
        self.M2 = 0.0
        
        # Minimum sigma values to prevent division by zero
        self.MIN_SIGMA = {
            "PLR": 0.001,
            "TTL": 0.1,
            "CPU": 0.1,
            "Accuracy": 0.1
        }
        
        # Initial default values for when n=1
        self.INITIAL_SIGMA = {
            "PLR": 0.005,
            "TTL": 10.0,
            "CPU": 5.0,
            "Accuracy": 2.0
        }
        # Add warmup period tracking
        self.warmup_samples = 5  # Number of samples needed before calculating z-scores
        self.node_sample_counts = defaultdict(int)  # Track samples per node
        
        super(fogNodeCharacterisation, self).__init__(*args)
        
        
        
    def muAndsigma_per_node(self, node_id: int, new_plr: float, new_ttl: float, new_cpu: float) -> Dict:
        """Calculate running statistics with warmup period handling"""
        if node_id not in self.node_stats:
            self._initialise_node_stats(node_id)
            
        self.node_sample_counts[node_id] += 1
        
        updated_metrics = {}
        metrics = [
            ("PLR", new_plr),
            ("TTL", new_ttl), 
            ("CPU", new_cpu)
        ]
        
        for metric, value in metrics:
            #print("self.node_stats[node_id]: ", self.node_stats[node_id]); time.sleep(2)    
            stats = self.node_stats[node_id][metric]
            stats["n"] += 1
            delta = value - stats["mu"]
            stats["mu"] += delta / stats["n"]
            delta2 = value - stats["mu"]
            stats["sum_sq"] += delta * delta2
            
            # Only calculate sigma after warmup period
            if stats["n"] > 1:
                variance = stats["sum_sq"] / (stats["n"] - 1)
                stats["sigma"] = max(math.sqrt(abs(variance)), self.MIN_SIGMA[metric])
            else:
                stats["sigma"] = self.INITIAL_SIGMA[metric]

            updated_metrics[metric] = {
                "mu": stats["mu"],
                "sigma": stats["sigma"],
                "sum_sq": stats["sum_sq"]
            }

        return updated_metrics

    def healthMetric(self, node_id, plr: float, ttl: float, cpu: float,
                    plr_mean: float, plr_std: float, 
                    ttl_mean: float, ttl_std: float,
                    cpu_mean: float, cpu_std: float) -> float:
        """Enhanced health metric calculation with warmup handling"""
        # Apply sigma flooring
        plr_std = max(plr_std, self.MIN_SIGMA["PLR"])
        ttl_std = max(ttl_std, self.MIN_SIGMA["TTL"])
        cpu_std = max(cpu_std, self.MIN_SIGMA["CPU"])

        # Calculate z-scores with warmup protection
        z_plr = (plr - plr_mean) / plr_std if plr_std > 0 else 0
        z_ttl = (ttl - ttl_mean) / ttl_std if ttl_std > 0 else 0 
        z_cpu = (cpu - cpu_mean) / cpu_std if cpu_std > 0 else 0
        
        # Standardise each metric (Z-score)
        z_plr       = (plr - plr_mean) / plr_std if plr_std != 0 else 0.0
        z_ttl       = (ttl - ttl_mean) / ttl_std if ttl_std != 0 else 0.0
        z_cpu       = (cpu - cpu_mean) / cpu_std if cpu_std != 0 else 0.0
        

        # Weighted sum
        h = (self.WEIGHTS["PLR"] * z_plr +
             self.WEIGHTS["TTL"] * z_ttl +
             self.WEIGHTS["CPU"] * z_cpu)
        
        # Apply EMA smoothing after warmup
        if self.node_sample_counts.get(node_id, 0) > self.warmup_samples:
            if self.ema_health.get(node_id) is None:
                self.ema_health[node_id] = h
            else:
                self.ema_health[node_id] = self.ALPHA * h + (1 - self.ALPHA) * self.ema_health[node_id]
            return self.ema_health[node_id]
        
        return h

    
    def healthMetric2(self, plr, response, cpu, accuracy, 
                    plr_mean, plr_std, response_mean, response_std, 
                 cpu_mean, cpu_std) -> float:
        """
        Computes the health metric h_i(t) for a node at time t.
        
        Args:
            plr, response, cpu, accuracy: Current observed values.
            *_mean, *_std: Historical mean and standard deviation for each metric.
        
        Returns:
            float: Health score h_i(t).
        """
        WEIGHTS = self.WEIGHTS # {"PLR": 0.3, "Response": 0.2, "CPU": 0.3, "Accuracy": 0.2}
        
        # Standardise each metric (Z-score)
        z_plr       = (plr - plr_mean) / plr_std if plr_std != 0 else 0.0
        z_response  = (response - response_mean) / response_std if response_std != 0 else 0.0
        z_cpu       = (cpu - cpu_mean) / cpu_std if cpu_std != 0 else 0.0
        
        #print("z_plr: ", z_plr, "\n z_response: ", z_response, "\n z_cpu", z_cpu, "\n z_accuracy: ", z_accuracy)
        
        # Apply weights and sum
        h = (WEIGHTS["PLR"] * z_plr + 
            WEIGHTS["Response"] * z_response + 
            WEIGHTS["CPU"] * z_cpu  
            )
        
        return h


    def _initialise_node_stats(self, node_id: str):
        """Initialise statistics for a new node"""
        self.node_stats[node_id] = {
            "PLR": {"n": 0, "mu": 0, "sum_sq": 0, "sigma": self.INITIAL_SIGMA["PLR"]},
            "TTL": {"n": 0, "mu": 0, "sum_sq": 0, "sigma": self.INITIAL_SIGMA["TTL"]},
            "CPU": {"n": 0, "mu": 0, "sum_sq": 0, "sigma": self.INITIAL_SIGMA["CPU"]}
        }
        self.node_sample_counts[node_id] = 0

