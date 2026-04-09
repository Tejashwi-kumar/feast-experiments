import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import euclidean

def compare_lstm_datasets(df1, df2, entity_col, time_col, feature_cols):
    """
    Compares two datasets for similarity across entity groups.
    """
    results = []
    
    # Ensure both dataframes are sorted identically
    df1 = df1.sort_values([entity_col, time_col]).reset_index(drop=True)
    df2 = df2.sort_values([entity_col, time_col]).reset_index(drop=True)
    
    entities = set(df1[entity_col]).intersection(set(df2[entity_col]))
    
    for entity in entities:
        # Extract matrices for the specific entity
        matrix1 = df1[df1[entity_col] == entity][feature_cols].values
        matrix2 = df2[df2[entity_col] == entity][feature_cols].values
        
        # Check if shapes match (essential for LSTM sequence comparison)
        if matrix1.shape != matrix2.shape:
            continue 

        # 1. Cosine Similarity (Average across timesteps)
        # Measures if the 'direction' of features is similar
        cos_sim = np.mean(np.diag(cosine_similarity(matrix1, matrix2)))
        
        # 2. Euclidean Distance (Normalized by matrix size)
        # Measures the absolute difference in values
        dist = np.linalg.norm(matrix1 - matrix2) / matrix1.size
        
        results.append({
            'Entity': entity,
            'Cosine_Similarity': cos_sim,
            'Avg_Abs_Diff': dist,
            'Timesteps': matrix1.shape[0]
        })
    
    comparison_df = pd.DataFrame(results)
    
    # Summary Statistics
    print("--- Dataset Similarity Report ---")
    print(f"Overall Mean Cosine Similarity: {comparison_df['Cosine_Similarity'].mean():.4f}")
    print(f"Overall Mean Deviation: {comparison_df['Avg_Abs_Diff'].mean():.4f}")
    
    return comparison_df

# Example Usage:
# report = compare_lstm_datasets(df_test_1, df_test_2, 'entity_id', 'timestamp', ['feat1', 'feat2', 'feat3'])
