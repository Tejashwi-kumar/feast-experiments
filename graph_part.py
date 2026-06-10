import numpy as np
import pandas as pd
import random
from collections import defaultdict

def generate_hcp_alignment(num_territories=84, total_hcps=250000, num_partitions=6, p_internal=0.85):
    """
    Generates synthetic HCP-Territory alignment data optimized for downstream graph partitioning.
    """
    print(f"Initializing data generation for {total_hcps} HCPs across {num_territories} territories...")

    # 1. Base distribution from your uploaded image (k = number of territories)
    # Dictionary format: {k_territories: count_of_hcps}
    shared_distribution = {
        2: 8067, 3: 2459, 4: 920, 5: 425, 6: 224, 7: 128, 8: 61, 9: 33, 10: 20, 
        11: 22, 12: 12, 13: 10, 14: 10, 15: 8, 16: 5, 17: 5, 18: 5, 19: 4, 
        20: 1, 21: 4, 22: 2, 23: 1, 24: 2, 28: 1, 33: 1
    }
    
    # Calculate unshared (k=1) HCPs to reach the 250k total
    total_shared = sum(shared_distribution.values())
    unshared_count = total_hcps - total_shared
    if unshared_count > 0:
        shared_distribution[1] = unshared_count

    # 2. Setup Latent Partitions (Anti-Skew)
    territories = np.arange(1, num_territories + 1)
    # Split territories evenly into latent community buckets
    latent_partitions = np.array_split(territories, num_partitions)
    
    # Track current load per territory to enforce dynamic load balancing
    terr_loads = {t: 0 for t in territories}
    
    alignment_records = []
    hcp_counter = 1

    # Helper function: Select territories using inverse-load probabilities
    def select_territories(pool, k_needed):
        # Weight = 1 / (current_load + 1) to prioritize under-loaded territories
        weights = [1.0 / (terr_loads[t] + 1) for t in pool]
        prob_dist = np.array(weights) / sum(weights)
        
        selected = np.random.choice(pool, size=k_needed, replace=False, p=prob_dist)
        for t in selected:
            terr_loads[t] += 1
        return selected

    # 3. Generate Alignments
    for k_degree, count in shared_distribution.items():
        for _ in range(count):
            hcp_id = f"HCP_{hcp_counter}"
            hcp_counter += 1
            
            # If k=1, just pick from the entire pool of 84 globally to balance load
            if k_degree == 1:
                selected_terrs = select_territories(territories, 1)
            
            else:
                # Decide if this HCP is internal to a community or spans boundaries
                is_internal = random.random() < p_internal
                
                # Pick a primary partition randomly
                p_idx = random.randint(0, num_partitions - 1)
                primary_pool = latent_partitions[p_idx]
                
                # If k is larger than the partition size, it MUST span boundaries (Hub)
                if k_degree > len(primary_pool):
                    is_internal = False
                
                if is_internal:
                    # Intra-partition assignment: pick exclusively from the primary pool
                    selected_terrs = select_territories(primary_pool, k_degree)
                else:
                    # Inter-partition (Boundary/Hub): combine adjacent partitions
                    # Grab the next partition (wrapping around)
                    next_p_idx = (p_idx + 1) % num_partitions
                    boundary_pool = np.concatenate((primary_pool, latent_partitions[next_p_idx]))
                    
                    # If k is massive (like 33), add a 3rd partition to the pool
                    if k_degree > len(boundary_pool):
                        third_p_idx = (p_idx + 2) % num_partitions
                        boundary_pool = np.concatenate((boundary_pool, latent_partitions[third_p_idx]))
                    
                    selected_terrs = select_territories(boundary_pool, k_degree)
            
            # Record the pairs
            for t in selected_terrs:
                alignment_records.append({"HCP_ID": hcp_id, "Territory_ID": t})

    # 4. Export to DataFrame
    df_alignments = pd.DataFrame(alignment_records)
    print("Data generation complete!")
    
    return df_alignments

# --- Execute the Script ---
if __name__ == "__main__":
    # Generate the dataset
    # We are setting 6 latent partitions (84 / 6 = 14 territories per cluster naturally)
    df = generate_hcp_alignment(num_territories=84, total_hcps=250000, num_partitions=6)
    
    # Display sample and distribution statistics
    print("\n--- Sample Output ---")
    print(df.head(10))
    
    print("\n--- Load Balance Check (Top & Bottom 5 Territories) ---")
    load_counts = df['Territory_ID'].value_counts()
    print("Top 5 Loaded Territories:\n", load_counts.head(5))
    print("\nBottom 5 Loaded Territories:\n", load_counts.tail(5))
    
    # Save to CSV for your pipeline
    df.to_csv("territory_hcp_alignment.csv", index=False)
    print("\nFile saved as 'territory_hcp_alignment.csv'")
