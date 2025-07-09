# fog_network_monitoring
# Brief project overview

## Fog Network Monitoring
This project simulates health monitoring across a multi-tier fog network using real-time health metrics, anomaly detection, and visual dashboards.

### Modules
- `core/`: Core logic including health calculations and dispatching
- `dashboard/`: Real-time dashboards
- `simulation/`: Dataset generation and execution scripts
- `utils/`: Anomaly configurations
- `data/`: CSV files (node list and generated datasets)

### Usage
```bash
python simulation/run_simulation.py
```

Ensure `data/node_list.csv` exists or is generated on first run.
