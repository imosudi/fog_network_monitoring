
from collections import defaultdict
from datetime import datetime, timezone
import os
import pandas as pd


class NodeHealthCalculator:
    """
    Advanced node health assessment system for multi-tier network infrastructure
    with adaptive thresholds, weighted scoring mechanisms, and EMA-based health tracking.
    """
    
    def __init__(self, ema_beta=0.3):
        # Define tier-specific baseline thresholds and weights
        self.tier_config = {
            "L1": {
                "cpu_threshold": 70.0,
                "plr_threshold": 0.01,
                "rtt_threshold": 100.0,
                "weights": {"cpu": 0.4, "plr": 0.35, "rtt": 0.25},
                "criticality_multiplier": 1.5
            },
            "L2": {
                "cpu_threshold": 75.0,
                "plr_threshold": 0.02,
                "rtt_threshold": 120.0,
                "weights": {"cpu": 0.35, "plr": 0.4, "rtt": 0.25},
                "criticality_multiplier": 1.3
            },
            "L3": {
                "cpu_threshold": 80.0,
                "plr_threshold": 0.03,
                "rtt_threshold": 150.0,
                "weights": {"cpu": 0.3, "plr": 0.4, "rtt": 0.3},
                "criticality_multiplier": 1.1
            },
            "L4": {
                "cpu_threshold": 85.0,
                "plr_threshold": 0.05,
                "rtt_threshold": 200.0,
                "weights": {"cpu": 0.25, "plr": 0.45, "rtt": 0.3},
                "criticality_multiplier": 1.0
            }
        }
        
        # Anomaly impact factors on health scores
        self.anomaly_impact = {
            "none": 1.0,
            "overload": 0.2,
            "silence": 0.0,
            "routing_loop": 0.1,
            "fibre_cut": 0.0,
            "drift": 0.6,
            "intermittent_loss": 0.4,
            "latency_spike": 0.7,
            "throughput_drop": 0.5,
            "offline": 0.0,
            "spike": 0.3
        }
        
        # EMA configuration
        self.ema_beta = ema_beta
        self.ema_health_scores = {}  # Stores EMA health scores per node
        self.ema_thresholds = {}     # Stores EMA thresholds per node
        self.health_history = defaultdict(list)  # Stores historical health data
        
        # Load node metadata for context-aware health assessment
        self.node_metadata = self._load_node_metadata()
    
    def _load_node_metadata(self):
        """Load node metadata for enhanced health calculations"""
        try:
            if os.path.exists("data/node_list.csv"):
                return pd.read_csv("data/node_list.csv")
            return pd.DataFrame()
        except Exception as e:
            print(f"Warning: Could not load node metadata: {e}")
            return pd.DataFrame()
    
    def _get_node_tier(self, node_id):
        """Extract tier from node ID"""
        if node_id.startswith("L") and "_" in node_id:
            return node_id.split("_")[0]
        return "L4"  # Default to L4 if unknown
    
    def _calculate_metric_scores(self, cpu, plr, rtt, tier):
        """Calculate individual metric health scores (0-100)"""
        config = self.tier_config.get(tier, self.tier_config["L4"])
        
        # CPU Score (inverted - lower is better up to threshold)
        cpu_score = max(0, min(100, 100 - (cpu / config["cpu_threshold"]) * 100))
        
        # PLR Score (inverted - lower is better)
        plr_score = max(0, min(100, 100 - (plr / config["plr_threshold"]) * 100))
        
        # RTT Score (inverted - lower is better)
        rtt_score = max(0, min(100, 100 - (rtt / config["rtt_threshold"]) * 100))
        
        return cpu_score, plr_score, rtt_score
    
    def _calculate_adaptive_threshold(self, node_id, tier, anomaly_type):
        """Calculate adaptive health threshold based on node context"""
        config = self.tier_config.get(tier, self.tier_config["L4"])
        base_threshold = 60.0  # Base health threshold
        
        # Adjust for tier criticality
        tier_adjustment = config["criticality_multiplier"] * 10
        
        # Adjust for anomaly presence
        anomaly_adjustment = 0
        if anomaly_type != "none":
            anomaly_adjustment = -20  # Lower threshold expectation during anomalies
        
        # Node-specific adjustments based on metadata
        node_adjustment = 0
        if not self.node_metadata.empty:
            node_info = self.node_metadata[self.node_metadata['node_id'] == node_id]
            if not node_info.empty:
                criticality = node_info.iloc[0].get('criticality', 'Standard')
                if criticality == 'Mission_Critical':
                    node_adjustment = 15
                elif criticality == 'Ultra_High':
                    node_adjustment = 12
                elif criticality == 'High':
                    node_adjustment = 8
                elif criticality == 'Medium':
                    node_adjustment = 4
        
        # Calculate final adaptive threshold
        threshold = base_threshold + tier_adjustment + anomaly_adjustment + node_adjustment
        return max(20, min(95, threshold))  # Constrain between 20-95
    
    def _calculate_weighted_health_score(self, cpu, plr, rtt, tier, anomaly_type):
        """Calculate weighted health score with anomaly impact"""
        config = self.tier_config.get(tier, self.tier_config["L4"])
        
        # Get individual metric scores
        cpu_score, plr_score, rtt_score = self._calculate_metric_scores(cpu, plr, rtt, tier)
        
        # Calculate weighted score
        weighted_score = (
            cpu_score * config["weights"]["cpu"] +
            plr_score * config["weights"]["plr"] +
            rtt_score * config["weights"]["rtt"]
        )
        
        # Apply anomaly impact
        anomaly_factor = self.anomaly_impact.get(anomaly_type, 0.5)
        final_score = weighted_score * anomaly_factor
        
        return max(0, min(100, final_score))
    
    def _update_ema_values(self, node_id, current_score, current_threshold):
        """Update EMA values for health score and threshold"""
        # Initialise EMA values if they don't exist for this node
        if node_id not in self.ema_health_scores:
            self.ema_health_scores[node_id] = current_score
            self.ema_thresholds[node_id] = current_threshold
        else:
            # Update health score EMA
            self.ema_health_scores[node_id] = (
                self.ema_beta * current_score + 
                (1 - self.ema_beta) * self.ema_health_scores[node_id]
            )
            
            # Update threshold EMA
            self.ema_thresholds[node_id] = (
                self.ema_beta * current_threshold + 
                (1 - self.ema_beta) * self.ema_thresholds[node_id]
            )
        
        return self.ema_health_scores[node_id], self.ema_thresholds[node_id]
    
    def calculate_health_metrics(self, node_data):
        """
        Calculate comprehensive health metrics for a node sample with EMA tracking
        
        Parameters:
        node_data: pandas Series or dict containing node metrics
        
        Returns:
        dict: Health metrics including threshold, score, EMA values, and status
        """
        # Extract basic metrics
        node_id = node_data.get('node_id', 'UNKNOWN')
        cpu = float(node_data.get('cpu', 50.0))
        plr = float(node_data.get('plr', 0.01))
        rtt = float(node_data.get('rtt', 100.0))
        anomaly_type = node_data.get('anomaly', 'none')
        
        # Determine tier
        tier = self._get_node_tier(node_id)
        
        # Calculate adaptive health threshold
        health_threshold = self._calculate_adaptive_threshold(node_id, tier, anomaly_type)
        
        # Calculate weighted health score
        weighted_health_score = self._calculate_weighted_health_score(
            cpu, plr, rtt, tier, anomaly_type
        )
        
        # Update EMA values
        ema_score, ema_threshold = self._update_ema_values(
            node_id, weighted_health_score, health_threshold
        )
        
        # Calculate health status (difference between EMA score and EMA threshold)
        health_status = ema_score - ema_threshold
        
        # Store historical data
        self.health_history[node_id].append({
            'timestamp': datetime.now(timezone.utc),
            'raw_score': weighted_health_score,
            'raw_threshold': health_threshold,
            'ema_score': ema_score,
            'ema_threshold': ema_threshold,
            'status': health_status
        })
        
        return {
            'health_threshold': round(health_threshold, 2),
            'weighted_health_score': round(weighted_health_score, 2),
            'ema_health_score': round(ema_score, 2),
            'ema_health_threshold': round(ema_threshold, 2),
            'health_status': round(health_status, 2),
            'tier': tier,
            'cpu_subscore': round(self._calculate_metric_scores(cpu, plr, rtt, tier)[0], 2),
            'plr_subscore': round(self._calculate_metric_scores(cpu, plr, rtt, tier)[1], 2),
            'rtt_subscore': round(self._calculate_metric_scores(cpu, plr, rtt, tier)[2], 2),
            'anomaly_type': anomaly_type
        }
    
    def get_health_history(self, node_id, max_points=100):
        """
        Get historical health data for a specific node
        
        Parameters:
        node_id: ID of the node to retrieve history for
        max_points: Maximum number of historical points to return
        
        Returns:
        list: List of historical health data points (most recent first)
        """
        history = self.health_history.get(node_id, [])
        return history[-max_points:]
    
    def get_health_status(self, node_id):
        """
        Get current health status for a specific node based on EMA values
        
        Parameters:
        node_id: ID of the node to check
        
        Returns:
        dict: Current health status or None if node not found
        """
        if node_id not in self.ema_health_scores:
            return None
            
        return {
            'ema_health_score': round(self.ema_health_scores[node_id], 2),
            'ema_health_threshold': round(self.ema_thresholds[node_id], 2),
            'status': round(self.ema_health_scores[node_id] - self.ema_thresholds[node_id], 2)
        }
        
