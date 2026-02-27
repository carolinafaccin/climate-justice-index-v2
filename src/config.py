import os
import json
from pathlib import Path

# ==============================================================================
# 1. LOCAL PATHS CONFIGURATION
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.local.json"

if CONFIG_PATH.exists():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config_local = json.load(f)
    DATA_DIR = Path(config_local["data_dir"])
else:
    DATA_DIR = BASE_DIR / "data"

# Main Folders
INPUTS_DIR = DATA_DIR / "inputs"
OUTPUTS_DIR = DATA_DIR / "outputs"

# Input Folders
CLEAN_DIR = INPUTS_DIR / "clean"
RAW_DIR = INPUTS_DIR / "raw"

# Output Folders
DIAGNOSE_DIR = OUTPUTS_DIR / "diagnose"
FIGURES_DIR = OUTPUTS_DIR / "figures"
RESULTS_DIR = OUTPUTS_DIR / "results"

for d in [OUTPUTS_DIR, RESULTS_DIR, FIGURES_DIR, DIAGNOSE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 2. GLOBAL PROJECT DEFINITIONS
# ==============================================================================
# Grid Resolution
H3_RES = 9
COL_ID_H3 = 'h3_id'

# Versioning
INDEX_VERSION = "v2.0"

# Formats the version for the filename (e.g., 'v1.0' becomes 'v1_0')
_formatted_version = INDEX_VERSION.replace('.', '_')

# Main file names
FILE_BASE_H3 = "br_h3_res9.parquet"
BASE_H3_DIR = RAW_DIR / "h3" / FILE_BASE_H3
FILE_FINAL_INDEX = f"br_h3_res9_ijc_{_formatted_version}.parquet"

# ==============================================================================
# 3. METADATA LOADING (INDICATORS)
# ==============================================================================
INDICATORS_PATH = BASE_DIR / "indicators.json"

if INDICATORS_PATH.exists():
    with open(INDICATORS_PATH, 'r', encoding='utf-8') as f:
        INDICATORS = json.load(f)
else:
    print(f"Warning: File {INDICATORS_PATH} not found.")
    INDICATORS = {}

# ==============================================================================
# 4. AUTOMATIC DICTIONARY GENERATION
# ==============================================================================
# Alterado de MAPA_COLUNAS para COLUMN_MAP
COLUMN_MAP = {k: v['col'] for k, v in INDICATORS.items()}

FILES_H3 = {k: CLEAN_DIR / v['file'] for k, v in INDICATORS.items()}

# Alterado de "base_metadados" para "base_metadata"
FILES_H3["base_metadata"] = BASE_H3_DIR

# Alterado de DIMENSOES para DIMENSIONS
DIMENSIONS = {}
for k, v in INDICATORS.items():
    # Puxa a chave "dimension" (ou "dimensao" se ainda não tiver traduzido o JSON)
    dim = v.get('dimension', v.get('dimensao')) 
    DIMENSIONS.setdefault(dim, []).append(k)

FILES = {
    "h3": FILES_H3,
    "output": {
        "h3_final": RESULTS_DIR / FILE_FINAL_INDEX
    }
}