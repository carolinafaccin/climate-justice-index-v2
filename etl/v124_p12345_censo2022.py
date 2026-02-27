import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime

"""
==============================================================================
CENSUS 2022 - UNIFIED DASYMETRIC ETL PIPELINE
==============================================================================
This script consolidates all IBGE 2022 Census data into H3 Hexagons.
It automatically reads the raw CSVs, extracts the specific variables,
applies the dasymetric weight (peso_dom) to absolute counts, and calculates
the final percentages and normalized indices.

Methodology for Ratios in Dasymetric Interpolation:
1.  Num_Hex = Num_Sector * peso_dom
2.  Den_Hex = Den_Sector * peso_dom
3.  Ratio_Hex = sum(Num_Hex) / sum(Den_Hex)
==============================================================================
"""

# ==============================================================================
# 1. ENVIRONMENT CONFIGURATION
# ==============================================================================
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.append(PROJECT_ROOT)

from src import config as cfg
from src import utils

# ==============================================================================
# 2. BUSINESS RULES & INDICATORS CONFIGURATION (From indicators.json)
# ==============================================================================
# Rules based on the methodological tables:
# 'invert': True means Higher Percentage = 0 (Higher Vulnerability / Priority)
# 'invert': False means Higher Value = 1 (Higher Income = Lower Vulnerability)
CENSUS_LOGIC = {
    "v1": {"num_cols": ["v06004_v06001"], "den_cols": ["v06001"], "invert": False}, # Income (Renda)
    "v2": {"num_cols": ["v00238"], "den_cols": ["v00001"], "invert": True}, # Housing (Banheiro)
    "v4": {"num_cols": ["v00853", "v00855", "v00857"], "den_cols": ["v01006"], "invert": True}, # Education (Analfabetismo)
    "p1": {"num_cols": ["v01063"], "den_cols": ["v01042"], "invert": True}, # Women (Mulheres)
    "p2": {"num_cols": ["v01031"], "den_cols": ["v01006"], "invert": True}, # Children (Crianças)
    "p3": {"num_cols": ["v01040", "v01041"], "den_cols": ["v01006"], "invert": True}, # Elderly (Idosos)
    "p4": {"num_cols": ["v01318", "v01320"], "den_cols": ["v01006"], "invert": True}, # Black/Brown (Pretos e Pardos)
    "p5": {"num_cols": ["v01500", "v03000"], "den_cols": ["v00001"], "invert": True}, # Indigenous/Quilombola
}

# Master list of all raw variables we need to extract from the CSVs
REQUIRED_RAW_VARS = [
    'v06001', 'v06004', 'v00001', 'v00238', 'v00853', 'v00855', 'v00857', 
    'v01006', 'v01031', 'v01040', 'v01041', 'v01042', 'v01063', 'v01318', 
    'v01320', 'v01500', 'v03000'
]

# ==============================================================================
# 3. PATHS AND DIAGNOSTIC DEFINITION
# ==============================================================================
input_dir = cfg.RAW_DIR / 'ibge' / 'censo' / '2022' / 'agregados_por_setores' / 't0'
h3_path = cfg.FILES_H3["base_metadata"]

now = datetime.now().strftime("%Y%m%d_%H%M%S")
DIAGNOSTIC_TXT = cfg.DIAGNOSE_DIR / f'diagnostic_h3_censo2022_{now}.txt'

# ==============================================================================
# 4. DATA EXTRACTION (READING CSVS)
# ==============================================================================
print("Starting Unified ETL Pipeline - CENSUS 2022...")
print("1/4 - Reading and consolidating raw CSVs...")

all_csvs = list(input_dir.glob('*.csv'))
df_censo = None

for f in all_csvs:
    try:
        # Load forcing string to preserve sector codes (cd_setor)
        try:
            df_temp = pd.read_csv(f, sep=';', dtype=str, encoding='utf-8')
        except:
            df_temp = pd.read_csv(f, sep=',', dtype=str, encoding='utf-8')
            
        df_temp.columns = df_temp.columns.str.lower()
        
        if 'cd_setor' not in df_temp.columns:
            continue
            
        # Filter only required columns to save memory
        cols_to_keep = ['cd_setor'] + [c for c in REQUIRED_RAW_VARS if c in df_temp.columns]
        df_temp = df_temp[cols_to_keep]
        
        if len(cols_to_keep) > 1:
            if df_censo is None:
                df_censo = df_temp
            else:
                df_censo = pd.merge(df_censo, df_temp, on='cd_setor', how='outer')
                
    except Exception as e:
        print(f"  ✗ Error reading {f.name}: {e}")

print(f"  ✓ Consolidation completed! Master shape: {df_censo.shape}")

# Convert variables to numeric
for col in REQUIRED_RAW_VARS:
    if col in df_censo.columns:
        df_censo[col] = pd.to_numeric(df_censo[col].str.replace(',', '.'), errors='coerce').fillna(0)

# Create absolute income volume (Average Income * Responsible People)
if 'v06004' in df_censo.columns and 'v06001' in df_censo.columns:
    df_censo['v06004_v06001'] = df_censo['v06004'] * df_censo['v06001']

# ==============================================================================
# 5. MERGE WITH H3 GRID AND DASYMETRIC WEIGHTING
# ==============================================================================
print("2/4 - Merging with H3 grid and applying dasymetric weights...")
df_h3 = pd.read_parquet(h3_path)

df_censo['cd_setor'] = df_censo['cd_setor'].astype(str)
df_h3['cd_setor'] = df_h3['cd_setor'].astype(str)

df_merged = pd.merge(df_h3[['h3_id', 'cd_setor', 'peso_dom']], df_censo, on='cd_setor', how='inner')

# Multiply absolute numerators and denominators by the weight (peso_dom)
columns_to_weight = REQUIRED_RAW_VARS + ['v06004_v06001']
for col in columns_to_weight:
    if col in df_merged.columns:
        df_merged[col] = df_merged[col] * df_merged['peso_dom']

# Aggregate by Hexagon
print("3/4 - Aggregating values per H3 Hexagon...")
df_hex = df_merged.groupby('h3_id')[columns_to_weight].sum().reset_index()

# ==============================================================================
# 6. CALCULATE FINAL INDICATORS AND NORMALIZE
# ==============================================================================
print("4/4 - Calculating absolute ratios, normalizing, and exporting...")
generated_files = {}

for ind_key, logic in CENSUS_LOGIC.items():
    col_norm = cfg.COLUMN_MAP.get(ind_key)
    if not col_norm:
        continue # Skip if it doesn't exist in indicators.json
        
    col_abs = col_norm.replace('_norm', '_abs')
    
    # Sum the weighted numerators and denominators
    num_series = sum(df_hex[c] for c in logic['num_cols'] if c in df_hex.columns)
    den_series = sum(df_hex[c] for c in logic['den_cols'] if c in df_hex.columns)
    
    # Calculate ratio (preventing division by zero)
    abs_series = np.where(den_series > 0, num_series / den_series, 0)
    
    # Normalize with Winsorization (using our utils tool!)
    norm_series = utils.normalize_minmax(pd.Series(abs_series), winsorize=True, limits=(0.01, 0.99))
    
    # Invert scale if required by methodology
    if logic['invert']:
        norm_series = 1.0 - norm_series
        
    # Assemble final DataFrame
    df_export = pd.DataFrame({
        'h3_id': df_hex['h3_id'],
        col_abs: abs_series,
        col_norm: norm_series
    })
    
    out_path = cfg.FILES_H3[ind_key]
    utils.save_parquet(df_export, out_path)
    generated_files[ind_key] = (out_path.name, df_export, col_abs, col_norm)
    print(f"  ✓ Processed and saved: {ind_key.upper()} ({out_path.name})")

# ==============================================================================
# 7. EXPORT DIAGNOSTIC LOG (.txt)
# ==============================================================================
print("Generating diagnostic report...")

with open(DIAGNOSTIC_TXT, 'w', encoding='utf-8') as f:
    f.write("=== CENSUS 2022 PARQUET DIAGNOSTIC (H3 Level) ===\n")
    f.write("Note: Dasymetric interpolation successfully applied.\n\n")
    
    for ind_key, (file_name, df_diag, col_abs, col_norm) in generated_files.items():
        f.write(f"--- {ind_key.upper()} : {file_name} ---\n")
        
        # Absolute stats
        f.write(f"Column (Absolute/Percentage): {col_abs}\n")
        f.write(f"  > Mean: {df_diag[col_abs].mean():.4f}\n")
        f.write(f"  > Median: {df_diag[col_abs].median():.4f}\n")
        f.write(f"  > Maximum: {df_diag[col_abs].max():.4f}\n")
        f.write(f"  > Minimum: {df_diag[col_abs].min():.4f}\n\n")

        # Normalized stats
        f.write(f"Column (Normalized): {col_norm}\n")
        ones = (df_diag[col_norm] >= 0.999).sum()
        zeros = (df_diag[col_norm] <= 0.001).sum()
        f.write(f"  > Extreme Value (~1): {ones} hexagons\n")
        f.write(f"  > Extreme Value (~0): {zeros} hexagons\n")
        f.write(f"  > Mean: {df_diag[col_norm].mean():.4f}\n")
        f.write("-" * 50 + "\n")

print(f"\n✅ Census ETL Pipeline completed! Diagnostic saved at: {DIAGNOSTIC_TXT}")