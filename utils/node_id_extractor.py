import csv
import pandas as pd
from typing import List, Optional, Union
from pathlib import Path

def extract_node_ids(file_path: Union[str, Path], 
                    delimiter: str = 'auto', 
                    node_id_column: str = 'node_id',
                    return_count: bool = False) -> Union[List[str], tuple]:
    """
    Extract node IDs from a CSV file containing network node data.
    
    Args:
        file_path (Union[str, Path]): Path to the CSV file
        delimiter (str): CSV delimiter character ('auto' for auto-detection, default: 'auto')
        node_id_column (str): Name of the column containing node IDs (default: 'node_id')
        return_count (bool): If True, return tuple of (node_ids, count) (default: False)
    
    Returns:
        Union[List[str], tuple]: List of node IDs or tuple of (node_ids, count)
    
    Raises:
        FileNotFoundError: If the specified file doesn't exist
        ValueError: If the node_id column is not found in the CSV
        Exception: For other CSV reading errors
    """
    
    try:
        # Convert to Path object for better path handling
        file_path = Path(file_path)
        
        # Check if file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Auto-detect delimiter if needed
        if delimiter == 'auto':
            # Read first line to detect delimiter
            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if '\t' in first_line:
                    delimiter = '\t'
                elif ',' in first_line:
                    delimiter = ','
                elif ';' in first_line:
                    delimiter = ';'
                else:
                    delimiter = ','  # Default fallback
        
        # Method 1: Using pandas (recommended for larger files)
        try:
            df = pd.read_csv(file_path, delimiter=delimiter)
            
            # Check if node_id column exists
            if node_id_column not in df.columns:
                raise ValueError(f"Column '{node_id_column}' not found in CSV. Available columns: {list(df.columns)}")
            
            # Extract node IDs and remove any NaN values
            node_ids = df[node_id_column].dropna().astype(str).tolist()
            
        except ImportError:
            # Fallback method using built-in csv module if pandas not available
            node_ids = []
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                
                # Check if node_id column exists
                if node_id_column not in reader.fieldnames:
                    raise ValueError(f"Column '{node_id_column}' not found in CSV. Available columns: {reader.fieldnames}")
                
                for row in reader:
                    node_id = row.get(node_id_column, '').strip()
                    if node_id:  # Skip empty values
                        node_ids.append(node_id)
        
        # Return based on return_count parameter
        if return_count:
            return node_ids, len(node_ids)
        else:
            return node_ids
            
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        raise

def extract_node_ids_from_string(csv_content: str, 
                                delimiter: str = '\t',
                                node_id_column: str = 'node_id',
                                return_count: bool = False) -> Union[List[str], tuple]:
    """
    Extract node IDs from CSV content provided as a string.
    
    Args:
        csv_content (str): CSV content as a string
        delimiter (str): CSV delimiter character (default: '\t')
        node_id_column (str): Name of the column containing node IDs (default: 'node_id')
        return_count (bool): If True, return tuple of (node_ids, count) (default: False)
    
    Returns:
        Union[List[str], tuple]: List of node IDs or tuple of (node_ids, count)
    """
    
    try:
        # Split content into lines
        lines = csv_content.strip().split('\n')
        
        if not lines:
            return [] if not return_count else ([], 0)
        
        # Get header and find node_id column index
        header = lines[0].split(delimiter)
        
        try:
            node_id_index = header.index(node_id_column)
        except ValueError:
            raise ValueError(f"Column '{node_id_column}' not found in CSV. Available columns: {header}")
        
        # Extract node IDs from data rows
        node_ids = []
        for line in lines[1:]:  # Skip header
            if line.strip():  # Skip empty lines
                columns = line.split(delimiter)
                if len(columns) > node_id_index:
                    node_id = columns[node_id_index].strip()
                    if node_id:  # Skip empty values
                        node_ids.append(node_id)
        
        # Return based on return_count parameter
        if return_count:
            return node_ids, len(node_ids)
        else:
            return node_ids
            
    except Exception as e:
        print(f"Error processing CSV content: {str(e)}")
        raise

def filter_node_ids_by_tier(node_ids: List[str], tier: str) -> List[str]:
    """
    Filter node IDs by tier level (L0, L1, L2, L3, L4).
    
    Args:
        node_ids (List[str]): List of node IDs
        tier (str): Tier level to filter by (e.g., 'L1', 'L2', 'L3', 'L4')
    
    Returns:
        List[str]: Filtered list of node IDs matching the specified tier
    """
    
    if tier.upper() == 'L0':
        # L0 nodes typically don't follow the LXN_XX pattern
        return [node_id for node_id in node_ids if not node_id.startswith(('L1N_', 'L2N_', 'L3N_', 'L4N_'))]
    else:
        tier_prefix = f"{tier.upper()}N_"
        return [node_id for node_id in node_ids if node_id.startswith(tier_prefix)]

def print_node_summary(node_ids: List[str]) -> None:
    """
    Print a summary of node IDs organized by tier.
    
    Args:
        node_ids (List[str]): List of node IDs
    """
    
    print("Node ID Summary:")
    print("=" * 50)
    
    # Group by tier
    tiers = {'L0': [], 'L1': [], 'L2': [], 'L3': [], 'L4': []}
    
    for node_id in node_ids:
        if node_id.startswith('L1N_'):
            tiers['L1'].append(node_id)
        elif node_id.startswith('L2N_'):
            tiers['L2'].append(node_id)
        elif node_id.startswith('L3N_'):
            tiers['L3'].append(node_id)
        elif node_id.startswith('L4N_'):
            tiers['L4'].append(node_id)
        else:
            tiers['L0'].append(node_id)
    
    # Print summary
    for tier, nodes in tiers.items():
        if nodes:
            print(f"\n{tier} Tier ({len(nodes)} nodes):")
            for i, node in enumerate(nodes, 1):
                print(f"  {i:2d}. {node}")
    
    print(f"\nTotal nodes: {len(node_ids)}")

# Example usage and test function
def main():
    """
    Example usage of the node ID extraction functions.
    """
    
    # Example CSV content (your data)
    sample_csv = """node_id	tier	location	node_type	management_zone	crop_focus	criticality	power_source	sensor_type
CloudDB_Server	L0	Cloud_Provider	Cloud_Database	Entire_Network	All_Crops	Mission_Critical	Grid_Backup	
L1N_01	L1	Farm_Office	Farm_Management_System	Entire_Farm	All_Crops	Ultra_High	Solar_Diesel_Hybrid	
L2N_01	L2	Cereal_Belt_Control	Zone_Supervisor	Cereal_Belt	Cereal	High	Solar_Battery	
L2N_02	L2	Rootcrop_Zone_Control	Zone_Supervisor	Rootcrop_Zone	Rootcrop	High	Solar_Battery	
L3N_01	L3	Sorghum_Field	Field_Unit	Cereal_Belt	Sorghum	Medium	Solar_Only	
L4N_01	L4	Sorghum_Crop_Canopy	Field_Sensor	Cereal_Belt	Sorghum	Standard	Battery_Solar	Nutrient_Level"""
    
    # Extract node IDs from string
    node_ids = extract_node_ids_from_string(sample_csv)
    
    # Print results
    print("Extracted Node IDs:")
    for i, node_id in enumerate(node_ids, 1):
        print(f"{i:2d}. {node_id}")
    
    print(f"\nTotal count: {len(node_ids)}")
    
    # Example with count
    node_ids_with_count = extract_node_ids_from_string(sample_csv, return_count=True)
    print(f"\nWith count: {node_ids_with_count}")
    
    # Filter by tier example
    l2_nodes = filter_node_ids_by_tier(node_ids, 'L2')
    print(f"\nL2 tier nodes: {l2_nodes}")
    
    # Print summary
    print_node_summary(node_ids)

if __name__ == "__main__":
    #main()
    pass