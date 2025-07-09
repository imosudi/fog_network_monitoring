# --- Launch Enhanced Real-Time Simulation ---
from dashboard.ema_dashboard import EMALiveMultiNodeDashboard
from simulation.data_generator import generate_comprehensive_dataset


if __name__ == "__main__":
    
    # Generate comprehensive dataset
    dataset = generate_comprehensive_dataset(num_iterations=50)
    
    # Create a diverse set of nodes for testing
    sample_node_ids = [
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

    
    
    dashboard = EMALiveMultiNodeDashboard(sample_node_ids, max_display=len(sample_node_ids))
    ani = dashboard.run()