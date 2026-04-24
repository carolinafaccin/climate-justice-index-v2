import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.append(PROJECT_ROOT)
from src import config as cfg
from src import utils

# ==============================================================================
# 1. PATHS
# ==============================================================================
ICM_DIR = cfg.RAW_DIR / cfg.INDICATORS["g7"]["source"]["dir"]
SRC_COL  = cfg.INDICATORS["g7"]["source"]["col"]   # "v7"

now = datetime.now().strftime("%Y%m%d_%H%M%S")
DIAGNOSTIC_TXT = cfg.DIAGNOSE_DIR / f"diagnostic_h3_g7_midr_{now}.txt"

col_g7_norm = cfg.COLUMN_MAP["g7"]
col_g7_abs  = col_g7_norm.replace("_norm", "_abs")

# ==============================================================================
# 2. LOAD AND COMBINE ALL LISTS (lista-a.csv … lista-d.csv)
# ==============================================================================
print("=" * 60)
print("ETL: MIDR/ICM — G7 (Cadastro de famílias em áreas de risco)")
print(f"Fonte: {ICM_DIR}")
print("=" * 60)

print("\n1/4 - Carregando CSVs do ICM...")
csv_files = sorted(ICM_DIR.glob("lista-*.csv"))
if not csv_files:
    raise FileNotFoundError(f"Nenhum CSV 'lista-*.csv' encontrado em: {ICM_DIR}")

parts = []
for path in csv_files:
    try:
        df = pd.read_csv(path, sep=';', usecols=["cod_mun", SRC_COL], dtype={"cod_mun": str})
    except Exception:
        df = pd.read_csv(path, sep=';', encoding='latin1', usecols=["cod_mun", SRC_COL], dtype={"cod_mun": str})
    parts.append(df)
    print(f"   ✓ {path.name}  ({len(df):,} municípios)")

df_icm = pd.concat(parts, ignore_index=True)
print(f"\n   Total antes de deduplicar: {len(df_icm):,} linhas")

# A municipality could appear in more than one list — keep the highest v7 value
df_icm["cod_mun"] = df_icm["cod_mun"].astype(str).str.strip()
df_icm[SRC_COL]   = pd.to_numeric(df_icm[SRC_COL], errors="coerce")
df_icm = df_icm.groupby("cod_mun", as_index=False)[SRC_COL].max()
print(f"   Municípios únicos: {len(df_icm):,}")

# ==============================================================================
# 3. INDICATOR — binary: 1 = has registry, 0 = doesn't
# ==============================================================================
print("\n2/4 - Calculando indicador...")
df_icm[col_g7_abs]  = df_icm[SRC_COL]
df_icm[col_g7_norm] = df_icm[col_g7_abs]  # already 0/1 — no normalization needed

n_yes = (df_icm[col_g7_abs] == 1).sum()
n_no  = (df_icm[col_g7_abs] == 0).sum()
print(f"   v7=1 (tem cadastro):    {n_yes:,} municípios")
print(f"   v7=0 (não tem cadastro): {n_no:,} municípios")

# ==============================================================================
# 4. MERGE WITH H3 BASE
# ==============================================================================
print("\n3/4 - Mesclando com malha H3 base...")
df_h3 = pd.read_parquet(cfg.FILES_H3["base_metadata"], columns=["h3_id", "cd_mun"])
df_h3["cd_mun"] = df_h3["cd_mun"].astype(str).str.strip()

df_final = df_h3.merge(
    df_icm[["cod_mun", col_g7_abs, col_g7_norm]].rename(columns={"cod_mun": "cd_mun"}),
    on="cd_mun", how="left"
)

n_matched   = df_final[col_g7_abs].notna().sum()
n_unmatched = df_final[col_g7_abs].isna().sum()
print(f"   Hexágonos com dado:       {n_matched:,}")
print(f"   Hexágonos sem dado (NaN): {n_unmatched:,}")

# ==============================================================================
# 5. SAVE
# ==============================================================================
print("\n4/4 - Salvando parquet...")
df_export = df_final[["h3_id", col_g7_abs, col_g7_norm]]
utils.save_parquet(df_export, cfg.FILES_H3["g7"])
print(f"   ✓ Salvo: {cfg.FILES_H3['g7'].name}")

# ==============================================================================
# 6. DIAGNOSTIC
# ==============================================================================
with open(DIAGNOSTIC_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 60 + "\n")
    f.write("MIDR/ICM — G7 Cadastro de Famílias em Risco ETL Diagnostic\n")
    f.write(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("=" * 60 + "\n\n")
    f.write(f"Diretório ICM : {ICM_DIR}\n")
    f.write(f"Arquivos lidos: {[p.name for p in csv_files]}\n")
    f.write(f"Coluna fonte  : {SRC_COL}\n\n")
    f.write(f"Municípios únicos processados: {len(df_icm):,}\n")
    f.write(f"  v7=1 (tem cadastro)    : {n_yes:,}\n")
    f.write(f"  v7=0 (não tem cadastro): {n_no:,}\n\n")
    f.write(f"Hexágonos com dado      : {n_matched:,}\n")
    f.write(f"Hexágonos sem dado (NaN): {n_unmatched:,}\n")

print(f"\nDiagnóstico: {DIAGNOSTIC_TXT}")
print("Concluído!")
