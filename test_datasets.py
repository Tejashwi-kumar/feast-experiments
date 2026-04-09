import pandas as pd
import numpy as np
from scipy.stats import ks_2samp

def compare_sequence_matrices(df1, df2, time_col, feature_cols):
    """
    Compares the distribution of features at each timestep across two datasets
    where entities are different.
    """
    results = []
    
    # Get common timesteps to ensure aligned comparison
    timesteps = sorted(set(df1[time_col]).intersection(set(df2[time_col])))
    
    for t in timesteps:
        t_df1 = df1[df1[time_col] == t]
        t_df2 = df2[df2[time_col] == t]
        
        step_metrics = {'Timestep': t}
        
        for col in feature_cols:
            # Kolmogorov-Smirnov test: 
            # Null hypothesis: the two samples are drawn from the same distribution.
            # p-value < 0.05 means the distributions are significantly DIFFERENT.
            stat, p_val = ks_2samp(t_df1[col], t_df2[col])
            
            # Calculate Mean Absolute Error between the means of the features
            mean_diff = abs(t_df1[col].mean() - t_df2[col].mean())
            
            step_metrics[f'{col}_ks_p_val'] = round(p_val, 4)
            step_metrics[f'{col}_mean_diff'] = round(mean_diff, 4)
            
        results.append(step_metrics)
    
    comparison_df = pd.DataFrame(results)
    
    # Summary of "Drift"
    print("--- Distribution Similarity Report (Timestep-wise) ---")
    for col in feature_cols:
        low_p_counts = (comparison_df[f'{col}_ks_p_val'] < 0.05).sum()
        print(f"Feature '{col}': {low_p_counts}/{len(timesteps)} timesteps show significant distribution shift.")
        
    return comparison_df
