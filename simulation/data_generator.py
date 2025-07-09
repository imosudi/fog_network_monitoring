# ---------------------- simulation/data_generator.py ----------------------

import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from core.node_dispatcher import get_node_sample
from core.sample_generators import create_node_list

def generate_comprehensive_dataset(num_iterations=50, output_file="data/node_health_dataset.csv"):
    """
    Generate comprehensive dataset with node metrics and health calculations

    Parameters:
    - num_iterations (int): Number of time steps to generate
    - output_file (str): Output CSV file path

    Returns:
    - pd.DataFrame: Dataset with health metrics
    """
    node_list = create_node_list()
    all_samples = []
    base_time = datetime.now(timezone.utc)

    print(f"Generating dataset with {num_iterations} iterations...")

    for t in range(num_iterations):
        current_time = base_time + timedelta(seconds=30 * t)

        for _, node_info in node_list.iterrows():
            node_id = node_info['node_id']
            if node_id.startswith('FarmHQ'):
                continue  # Skip invalid node types

            sample = get_node_sample(node_id, current_time, t, node_list)
            sample['iteration'] = t
            all_samples.append(sample)

        if (t + 1) % 10 == 0:
            print(f"  Completed {t + 1}/{num_iterations} iterations")

    dataset = pd.DataFrame(all_samples)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    dataset.to_csv(output_file, index=False)
    print(f"Dataset saved to {output_file}")

    summary_stats = {
        "total_samples": len(dataset),
        "unique_nodes": dataset['node_id'].nunique(),
        "time_range": f"{dataset['timestamp'].min()} to {dataset['timestamp'].max()}",
        "health_score_stats": {
            "mean": float(dataset['weighted_health_score'].mean()),
            "std": float(dataset['weighted_health_score'].std()),
            "min": float(dataset['weighted_health_score'].min()),
            "max": float(dataset['weighted_health_score'].max())
        },
        "anomaly_distribution": dataset['anomaly'].value_counts().to_dict(),
        "tier_distribution": dataset['tier'].value_counts().to_dict()
    }

    summary_path = output_file.replace(".csv", "_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary_stats, f, indent=2, default=str)
    print(f"Dataset summary saved to {summary_path}")

    return dataset

"""if __name__ == "__main__":
    generate_comprehensive_dataset(num_iterations=50)"""