import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# ==============================================================================
# 1. ENVIRONMENT CONFIGURATION
# ==============================================================================
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.append(PROJECT_ROOT)

from src import config as cfg

# ==============================================================================
# 2. PATHS, COLUMNS AND DIAGNOSTIC DEFINITION
# ==============================================================================
BASE_T0 = cfg.RAW_DIR / 'ibge' / 'munic' / 't0'

# Timestamp for the log file (Ex: 20260227_143005)
now = datetime.now().strftime("%Y%m%d_%H%M%S")
DIAGNOSTIC_TXT = cfg.DIAGNOSE_DIR / f'diagnostic_munic_h3_{now}.txt'

# Dynamic column names (Pulled straight from your JSON via config)
col_g2_norm = cfg.COLUMN_MAP["g2"]
col_g3_norm = cfg.COLUMN_MAP["g3"]
col_g4_norm = cfg.COLUMN_MAP["g4"]

col_g5_norm = cfg.COLUMN_MAP["g5"]
col_g5_abs = col_g5_norm.replace('_norm', '_abs')

ID_COLS = ['cd_mun', 'sigla_uf', 'cd_uf', 'nm_mun']
G5_COLS = [
    'mdhu571', 'mdhu572', 'mdhu573', 'mdhu574', 'mdhu575', 'mdhu576', 
    'mdhu577', 'mdhu578', 'mdhu579', 'mdhu5710', 'mdhu5711', 'mdhu5712', 
    'mdhu5713', 'mdhu5714', 'mdhu5715', 'mdhu5716', 'mdhu58', 'mdhu61', 
    'mdhu64', 'mdhu67', 'mdhu69'
]

def load_and_select(path, extra_cols):
    """Loads the CSV bypassing encoding errors and filters lowercase columns."""
    # Security check: if the file does not exist, warn and stop the script
    if not path.exists():
        raise FileNotFoundError(f"⚠️ Error: File not found -> {path}")
        
    try:
        df = pd.read_csv(path, sep=',', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=';', encoding='latin1')
    
    final_cols = ID_COLS + [c.lower() for c in extra_cols]
    return df[final_cols]

# ==============================================================================
# 3. EXTRACTION (Extract from t0 folder)
# ==============================================================================
print("Starting ETL Pipeline - IBGE MUNIC...")
print("1/4 - Extracting raw data (t0)...")

df_g2 = load_and_select(BASE_T0 / '2020' / 'munic_2020_gestao-de-riscos.csv', ['mgrd213'])
df_g3_g4 = load_and_select(BASE_T0 / '2023_saneamento' / 'munic_2023_saneamento_drenagem.csv', ['smap126', 'smap122'])
df_g5 = load_and_select(BASE_T0 / '2023' / 'munic_2023_direitos-humanos.csv', G5_COLS)

# Consolidate everything into a single base DataFrame of municipalities
df_munic = df_g2.merge(df_g3_g4, on=ID_COLS, how='left')
df_munic = df_munic.merge(df_g5, on=ID_COLS, how='left')

# ==============================================================================
# 4. TRANSFORMATION AND CLEANING (Transform)
# ==============================================================================
print("2/4 - Cleaning and calculating indicators...")

# Maps Sim/Não to 1 and 0 (Anything else becomes NaN automatically)
# NOTE: Keeping Portuguese text here as it matches the original IBGE CSV content
bool_map = {'Sim': 1, 'Não': 0}
columns_to_map = ['mgrd213', 'smap126', 'smap122'] + G5_COLS

for col in columns_to_map:
    if col in df_munic.columns:
        df_munic[col] = df_munic[col].map(bool_map)

# G2, G3, G4 (Dynamic assignment)
df_munic[col_g2_norm] = df_munic['mgrd213']
df_munic[col_g3_norm] = df_munic['smap126']
df_munic[col_g4_norm] = df_munic['smap122']

# G5: Human Rights (Absolute values and dynamic min-max normalization)
df_munic[col_g5_abs] = df_munic[G5_COLS].sum(axis=1, min_count=1)
min_val = df_munic[col_g5_abs].min()
max_val = df_munic[col_g5_abs].max()

if max_val - min_val > 0:
    df_munic[col_g5_norm] = (df_munic[col_g5_abs] - min_val) / (max_val - min_val)
else:
    df_munic[col_g5_norm] = 0

# Dictionary separating the final columns by indicator (using dynamic names)
ind_dataframes = {
    'g2': df_munic[['cd_mun', col_g2_norm]],
    'g3': df_munic[['cd_mun', col_g3_norm]],
    'g4': df_munic[['cd_mun', col_g4_norm]],
    'g5': df_munic[['cd_mun', col_g5_abs, col_g5_norm]]
}

# ==============================================================================
# 5. H3 CROSSING AND LOADING (Load to Parquet in the clean folder)
# ==============================================================================
print("3/4 - Merging with H3 grid and saving Parquets...")

# Loads the base spatial grid (straight from config!)
df_h3 = pd.read_parquet(cfg.FILES_H3["base_metadata"], columns=['h3_id', 'cd_mun'])
df_h3['cd_mun'] = df_h3['cd_mun'].astype(str)

generated_files = {}

for ind_key, df_indicator in ind_dataframes.items():
    # Ensures join keys are strings
    df_indicator['cd_mun'] = df_indicator['cd_mun'].astype(str)
    
    # Left join of the H3 grid with the indicator
    df_final_h3 = df_h3.merge(df_indicator, on='cd_mun', how='left')
    
    # Saves the individual result in the CLEAN_DIR folder (pulled from config.py)
    out_path = cfg.FILES_H3[ind_key]
    df_final_h3.to_parquet(out_path, index=False)
    
    # Keeps the dataframe in memory to generate the diagnostic in the next step
    generated_files[ind_key] = (out_path.name, df_final_h3)
    print(f"  ✓ Saved: {out_path.name}")

# ==============================================================================
# 6. DIAGNOSTICS AND EXPORT (.txt)
# ==============================================================================
print("4/4 - Generating diagnostic file...")

with open(DIAGNOSTIC_TXT, 'w', encoding='utf-8') as f:
    f.write("=== PARQUET FILES DIAGNOSTIC (H3 Level) ===\n")
    f.write("Note: The counts below reflect the number of hexagons, not municipalities.\n\n")
    
    for key, (file_name, df_diag) in generated_files.items():
        f.write(f"--- {key.upper()} : {file_name} ---\n")
        
        # Isolates only the value columns (ignores h3_id and cd_mun automatically)
        value_cols = [c for c in df_diag.columns if c not in ['h3_id', 'cd_mun']]
        
        for col in value_cols:
            nulls = df_diag[col].isna().sum()
            
            # The logic uses 'norm' and 'abs' in the name to identify the type, matching your JSON
            if 'norm' in col:
                ones = (df_diag[col] == 1).sum()
                zeros = (df_diag[col] == 0).sum()
                f.write(f"Column: {col}\n")
                f.write(f"  > Value 1 (Yes/Maximum): {ones}\n")
                f.write(f"  > Value 0 (No/Minimum): {zeros}\n")
                f.write(f"  > Null Values (NaN): {nulls}\n")
            
            if 'abs' in col:
                f.write(f"Column: {col}\n")
                f.write(f"  > Mean: {df_diag[col].mean():.2f}\n")
                f.write(f"  > Median: {df_diag[col].median()}\n")
                f.write(f"  > Maximum: {df_diag[col].max()}\n")
                f.write(f"  > Minimum: {df_diag[col].min()}\n")
                f.write(f"  > Null Values (NaN): {nulls}\n")
        
        f.write("-" * 50 + "\n")

print(f"\n✅ Pipeline completed successfully! Diagnostic saved at: {DIAGNOSTIC_TXT}")