import numpy as np
import pandas as pd
import random

def generate_hcp_alignment(num_territories=83, total_hcps=145000, num_partitions=6, p_internal=0.85):
    """
    Generates synthetic HCP-Territory alignment data optimized for downstream graph partitioning.
    """
    print(f"Initializing data generation for {total_hcps} HCPs across {num_territories} territories...")

    # 1. Base distribution from the new uploaded image
    # Dictionary format: {num_territories_shared: num_hcps}
    distribution = {
        1: 117161, 2: 6821, 3: 2097, 4: 756, 5: 359, 6: 173, 7: 92, 8: 49,
        9: 22, 10: 21, 11: 16, 12: 11, 13: 9, 14: 8, 15: 5, 16: 7, 17: 1,
        18: 9, 19: 4, 21: 1, 23: 2, 25: 1, 28: 1
    }
    
    # Calculate the current sum and pad the k=1 (unshared) bucket to reach total_hcps target
    current_total = sum(distribution.values())
    if total_hcps > current_total:
        padding_needed = total_hcps - current_total
        distribution[1] += padding_needed
        print(f"Added {padding_needed} HCPs to the unshared (k=1) bucket to hit {total_hcps} total.")

    # 2. Setup Latent Partitions (Anti-Skew)
    territories = np.arange(1, num_territories + 1)
    
    # Split territories evenly (handles prime numbers like 83 gracefully)
    latent_partitions = np.array_split(territories, num_partitions)
    
    # Track current load per territory to enforce dynamic load balancing
    terr_loads = {t: 0 for t in territories}
    
    alignment_records = []
    hcp_counter = 1

    # Helper function: Select territories using inverse-load probabilities
    def select_territories(pool, k_needed):
        weights = [1.0 / (terr_loads[t] + 1) for t in pool]
        prob_dist = np.array(weights) / sum(weights)
        
        # Ensure we don't try to select more territories than exist in the pool
        actual_k = min(k_needed, len(pool))
        
        selected = np.random.choice(pool, size=actual_k, replace=False, p=prob_dist)
        for t in selected:
            terr_loads[t] += 1
        return selected

    # 3. Generate Alignments
    for k_degree, count in distribution.items():
        for _ in range(count):
            hcp_id = f"HCP_{hcp_counter}"
            hcp_counter += 1
            
            # Unshared HCPs: pick purely to balance global load
            if k_degree == 1:
                selected_terrs = select_territories(territories, 1)
            
            else:
                is_internal = random.random() < p_internal
                p_idx = random.randint(0, num_partitions - 1)
                primary_pool = latent_partitions[p_idx]
                
                # If HCP needs more territories than the partition holds, it must span
                if k_degree > len(primary_pool):
                    is_internal = False
                
                if is_internal:
                    selected_terrs = select_territories(primary_pool, k_degree)
                else:
                    # Boundary/Hub logic: Combine adjacent partitions to handle larger k_degrees
                    next_p_idx = (p_idx + 1) % num_partitions
                    boundary_pool = np.concatenate((primary_pool, latent_partitions[next_p_idx]))
                    
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

if __name__ == "__main__":
    # Generate the dataset
    df = generate_hcp_alignment(num_territories=83, total_hcps=145000, num_partitions=6)
    
    # Display statistics
    print("\n--- Load Balance Check (Top & Bottom 5 Territories) ---")
    load_counts = df['Territory_ID'].value_counts()
    print("Top 5 Loaded Territories:\n", load_counts.head(5))
    print("\nBottom 5 Loaded Territories:\n", load_counts.tail(5))
    
    # Save to CSV
    df.to_csv("territory_hcp_alignment_v2.csv", index=False)
    print("\nFile saved as 'territory_hcp_alignment_v2.csv'")
