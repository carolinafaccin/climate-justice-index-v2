import pandas as pd
import numpy as np
import sys
from scipy.stats import mstats
from pathlib import Path
from datetime import datetime

# ==============================================================================
# 1. ENVIRONMENT CONFIGURATION
# ==============================================================================
# Discovers the exact location of THIS .py file and goes up two levels (to the root)
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.append(PROJECT_ROOT)

from src import config as cfg

# ==============================================================================
# 2. PATHS, COLUMNS AND DIAGNOSTIC DEFINITION
# ==============================================================================
input_dir = cfg.RAW_DIR / 'siconfi' / 't3'

# Paths from centralized config.py
h3_path = cfg.FILES_H3["base_metadata"]
output_path = cfg.FILES_H3["g1"]

# Dynamic column names (Pulled straight from indicators.json via config)
# If col_norm is 'g1_inv_norm', the code automatically creates 'g1_inv_abs'
col_norm = cfg.COLUMN_MAP["g1"]
col_abs = col_norm.replace('_norm', '_abs')

# Diagnostic log configuration
now = datetime.now().strftime("%Y%m%d_%H%M%S")
DIAGNOSTIC_TXT = cfg.DIAGNOSE_DIR / f'diagnostic_h3_g1_siconfi_{now}.txt'

# ==============================================================================
# 3. LOADING AND FILTERING (SICONFI)
# ==============================================================================
years = range(2015, 2025)
all_dfs = []

print("Starting ETL Pipeline - SICONFI...")
print("1/4 - Reading Siconfi files...")

for year in years:
    file_path = input_dir / f'finbra_mun_despesas-por-funcao_{year}.csv'
    
    if file_path.exists():
        df_year = pd.read_csv(file_path, sep=';', encoding='utf-8')
        
        # Filter only Gestão Ambiental (Environmental Management)
        # Note: We keep the exact Portuguese strings to match the original dataset values
        df_filtered = df_year[df_year['coluna'].str.contains('Despesas Liquidadas', na=False) & 
                              df_year['conta'].str.contains('Gestão Ambiental', case=False, na=False)].copy()
        
        all_dfs.append(df_filtered[['cd_mun', 'valor_per_capita']])
    else:
        print(f"⚠️ Warning: File not found for the year {year}")

# Consolidate sum by municipality
df_siconfi = pd.concat(all_dfs).groupby('cd_mun')['valor_per_capita'].sum().reset_index()

# Rename dynamically using the col_abs variable
df_siconfi.rename(columns={'valor_per_capita': col_abs}, inplace=True)

# ==============================================================================
# 4. H3 LOADING AND MERGING
# ==============================================================================
print("2/4 - Loading H3 base and merging data...")
df_h3 = pd.read_parquet(h3_path)

# Standardize join key to text (string)
df_siconfi['cd_mun'] = df_siconfi['cd_mun'].astype(str)
df_h3['cd_mun'] = df_h3['cd_mun'].astype(str)

# Join between H3 and Siconfi (filling NA dynamically)
df_final = df_h3.merge(df_siconfi, on='cd_mun', how='left').fillna({col_abs: 0})

# ==============================================================================
# 5. OUTLIER TREATMENT AND NORMALIZATION
# ==============================================================================
print("3/4 - Treating outliers and normalizing...")

# Outlier Treatment (Winsorizing)
df_final[col_abs] = mstats.winsorize(df_final[col_abs], limits=[0.01, 0.01])

# Normalization (0 to 1)
v_min = df_final[col_abs].min()
v_max = df_final[col_abs].max()

if v_max - v_min > 0:
    df_final[col_norm] = (df_final[col_abs] - v_min) / (v_max - v_min)
else:
    df_final[col_norm] = 0

# ==============================================================================
# 6. EXPORT (Parquet)
# ==============================================================================
df_export = df_final[['h3_id', col_abs, col_norm]]
df_export.to_parquet(output_path, index=False)

print(f"  ✓ File successfully saved at: {output_path.name}")

# ==============================================================================
# 7. DIAGNOSTICS AND EXPORT (.txt)
# ==============================================================================
print("4/4 - Generating diagnostic file...")

with open(DIAGNOSTIC_TXT, 'w', encoding='utf-8') as f:
    f.write("=== PARQUET FILES DIAGNOSTIC (H3 Level) ===\n")
    f.write("Note: The counts below reflect the number of hexagons, not municipalities.\n\n")
    
    f.write(f"--- G1 (Siconfi Investment) : {output_path.name} ---\n")
    
    # Uses the dynamic column list for the log
    value_columns = [col_abs, col_norm]
    
    for col in value_columns:
        nulls = df_export[col].isna().sum()
        
        if 'norm' in col:
            ones = (df_export[col] == 1).sum()
            zeros = (df_export[col] == 0).sum()
            f.write(f"Column: {col}\n")
            f.write(f"  > Exact value 1 (Maximum): {ones}\n")
            f.write(f"  > Exact value 0 (Minimum): {zeros}\n")
            f.write(f"  > Distribution mean: {df_export[col].mean():.4f}\n")
            f.write(f"  > Null Values (NaN): {nulls}\n")
        
        if 'abs' in col:
            f.write(f"Column: {col}\n")
            f.write(f"  > Investment mean (BRL per capita): {df_export[col].mean():.2f}\n")
            f.write(f"  > Median: {df_export[col].median():.2f}\n")
            f.write(f"  > Maximum (post-winsorizing): {df_export[col].max():.2f}\n")
            f.write(f"  > Minimum: {df_export[col].min():.2f}\n")
            f.write(f"  > Null Values (NaN): {nulls}\n")
            
    f.write("-" * 50 + "\n")

print(f"\n✅ Pipeline completed successfully! Diagnostic saved at: {DIAGNOSTIC_TXT}")