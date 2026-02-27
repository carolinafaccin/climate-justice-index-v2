import pandas as pd
import logging
from . import config as cfg

def calculate_simple_cji(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Calculating CJI (Climate Justice Index) via Simple Average...")
    df = df.copy()

    # 1. Calculate the average for each dimension
    for dimension, columns in cfg.DIMENSIONS.items():
        # Ensure the columns exist; if not, they are ignored
        existing_cols = [c for c in columns if c in df.columns]
        df[f'ind_{dimension}'] = df[existing_cols].mean(axis=1)
        logging.info(f"Dimension '{dimension}' calculated.")

    # 2. Final Index (Average of the 4 dimensions)
    # List of columns created in the previous step
    index_cols = [f'ind_{d}' for d in cfg.DIMENSIONS.keys()]
    
    # Final CJI = Simple average between Climate Exposure, Vulnerability, Priority Groups, and Governance Capacity
    df['cji_final'] = df[index_cols].mean(axis=1)
    
    return df