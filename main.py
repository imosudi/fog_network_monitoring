# --- Launch Enhanced Real-Time Simulation ---
import os
from core.sample_generators import create_node_list
from dashboard.ema_dashboard_dataset import EMALiveMultiNodeDashboard
from utils.node_id_extractor import extract_node_ids, extract_node_ids_from_string

import pandas as pd

from utils.node_utils import select_random_nodes
try:
    all_node_ids = extract_node_ids('data/node_list.csv')    
except :
    node_list_path = "data/node_list.csv"
    if os.path.exists(node_list_path):
        node_list = pd.read_csv(node_list_path)
    else:
        node_list = create_node_list()
    all_node_ids = extract_node_ids('data/node_list.csv') 
    
print("All node_ids: ", all_node_ids)
n = 16  # Size of sample nodes

sample_node_ids = select_random_nodes(all_node_ids, n)

if __name__ == "__main__":
    # Create a diverse set of nodes for testing
    node_ids = extract_node_ids('data/node_list.csv')
    print("node_ids: ", node_ids)
    sample_node_ids_static = [
        "L1N_01",
        "L2N_01", "L2N_02", "L2N_03", "L2N_04",
        "L3N_01", "L3N_02", "L3N_03", "L3N_04","L3N_05", "L3N_06", "L3N_07", "L3N_08",
        "L4N_01", "L4N_02", "L4N_03", "L4N_04", "L4N_05", "L4N_06", "L4N_07", "L4N_08", "L4N_09", "L4N_10", "L4N_11", "L4N_12"
    ]
    
    
    print("Starting Enhanced Multi-Node Network Dashboard...")
    print(f"Monitoring {len(sample_node_ids)} nodes with enhanced variability")
    print(f"Total nodes available: {len(sample_node_ids)}")  
    print("- Speed: Adjust animation speed")
    print("- EMA β: Adjust smoothing parameter")

    
    
    #dashboard = EMALiveMultiNodeDashboard(sample_node_ids, max_display=len(sample_node_ids))
    dashboard = EMALiveMultiNodeDashboard(node_ids, max_display=len(node_ids))
    dashboard.run()
    dashboard.save_metrics_to_csv("data/node_metrics_data.csv")
    #ani = dashboard.run()