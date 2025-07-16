# ---------------------- main.py ----------------------
import os
from dashboard.ema_dashboard_dataset import EMALiveMultiNodeDashboard
from utils.node_id_extractor import extract_node_ids, extract_node_ids_from_string
from utils.node_utils import create_node_list, select_random_nodes


if __name__ == "__main__":
    # Create a diverse set of nodes for testing
    # Load node list if not provided
    #if node_list is None:
    import pandas as pd
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
    
    # Predefined sample nodes or random selection
    sample_node_ids = [
        "L1N_01",
        "L2N_01", "L2N_02", "L2N_03", "L2N_04",
        "L3N_01", "L3N_02", "L3N_03", "L3N_04","L3N_05", "L3N_06", "L3N_07", "L3N_08",
        "L4N_01", "L4N_02", "L4N_03", "L4N_04", "L4N_05", "L4N_06", "L4N_07", "L4N_08", "L4N_09", "L4N_10", "L4N_11", "L4N_12"
    ]
    
    # If you want to use random selection instead, uncomment:
    sample_node_ids = select_random_nodes(all_node_ids, n)
    
    print("Starting Enhanced Multi-Node Network Dashboard...")
    print(f"Monitoring {len(sample_node_ids)} sample nodes with enhanced variability")
    print(f"Collecting data for {len(all_node_ids)} total nodes")  
    print("- Speed: Adjust animation speed")
    print("- EMA β: Adjust smoothing parameter")

    dashboard = EMALiveMultiNodeDashboard(all_node_ids, sample_node_ids, max_display=len(sample_node_ids))
    dashboard.run()
    dashboard.save_metrics_to_csv("data/node_metrics_data.csv")