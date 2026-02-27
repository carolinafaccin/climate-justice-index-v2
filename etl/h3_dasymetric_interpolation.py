import pandas as pd
import sys
from pathlib import Path

"""
==============================================================================
DASYMETRIC INTERPOLATION METHOD
==============================================================================

The column `peso_dom` represents the proportion of households from the census 
tract that are located within that specific hexagon.

To analyze the 2022 Census variables per hexagon, the analysis formula is:
HexagonValue = TractValue * peso_dom

Example:
If `Tract X` has 1000 households, and `Hexagon A` (located inside `Tract X`) 
has a `peso_dom` of 0.15, the calculation will be: 
1000 * 0.15 = 150. 
This means there are an estimated 150 households in Hexagon A.
==============================================================================
"""

# ==============================================================================
# 1. ENVIRONMENT CONFIGURATION
# ==============================================================================
# Discovers the exact location of THIS .py file and goes up two levels (to the root)
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.append(PROJECT_ROOT)

from src import config as cfg

# ==============================================================================
# 2. PATHS DEFINITION
# ==============================================================================
# Pointing to the old/temporary folders inside the RAW_DIR (inputs/raw/)
path_v1 = cfg.RAW_DIR / 'h3_past' / 'br_h3_res9_v1.parquet'
chunks_dir = cfg.RAW_DIR / 'h3_past' / 'chunks_uf_cnefe_domicilios'

# The final output is our global metadata base!
output_path = cfg.FILES_H3["base_metadata"]

# ==============================================================================
# 3. BASE GRID LOADING
# ==============================================================================
print("🚀 Starting consolidation with weight calculation...")

# Load the BASE GRID first (it contains the H3 <-> TRACT link)
cols_base = ['h3_id', 'cd_setor', 'cd_mun', 'nm_mun', 'cd_uf', 'nm_uf']
df_base = pd.read_parquet(path_v1, columns=cols_base)
print(f"✅ Base grid loaded: {len(df_base)} hexagons.")

# ==============================================================================
# 4. HOUSEHOLDS CONSOLIDATION (CNEFE)
# ==============================================================================
list_households = []

# Using pathlib to list only .parquet files in the folder quickly
for file_path in chunks_dir.glob('*.parquet'):
    temp_df = pd.read_parquet(file_path)
    # Renames to the short standard
    temp_df = temp_df.rename(columns={'qtd_domicilios': 'qtd_dom'})
    list_households.append(temp_df)

df_dom = pd.concat(list_households, ignore_index=True)
print(f"✅ Households loaded: {len(df_dom)} records.")

# ==============================================================================
# 5. CROSSING AND WEIGHT CALCULATION (DASYMETRIC)
# ==============================================================================
# Merge: Join the household count to the grid that has the tracts (inner join)
df_final = pd.merge(df_base, df_dom, on='h3_id', how='inner')

print("⚖️ Calculating dasymetric weights...")

# Sum of households per census tract
df_final['total_dom_setor'] = df_final.groupby('cd_setor')['qtd_dom'].transform('sum')

# Weight = fraction of the tract inside the hexagon
df_final['peso_dom'] = df_final['qtd_dom'] / df_final['total_dom_setor']

# ==============================================================================
# 6. FINAL CLEANING AND EXPORT
# ==============================================================================
# If total_dom_setor is 0 (avoids division by zero), we fill weight with 0
df_final['peso_dom'] = df_final['peso_dom'].fillna(0)
df_final = df_final.drop(columns=['total_dom_setor'])

# Remove hexagons with 0 households
df_final = df_final[df_final['qtd_dom'] > 0]

# Ensures the outputs/raw/h3/ folder exists before saving
output_path.parent.mkdir(parents=True, exist_ok=True) 

df_final.to_parquet(output_path, index=False, compression='snappy')

print(f"\n✨ Success! File generated: {output_path}")
print(f"📊 Columns: {df_final.columns.tolist()}")
print(f"📍 Total inhabited hexagons: {len(df_final)}")