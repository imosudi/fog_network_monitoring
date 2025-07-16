# ---------------------- core/sample_generators.py ----------------------
import json
import os
import random
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
