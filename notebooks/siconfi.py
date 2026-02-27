import pandas as pd
import numpy as np
import glob
import os
from scipy.stats import mstats

# Configuração de caminhos
input_dir = '../data/raw/siconfi/clean_filter_per-capita/'
h3_path = '../data/raw/h3/br_h3_res9.parquet'
output_path = '../data/clean/b3_h3_g1_mun_despesas_liquidadas.parquet'

# 1. Carregar e filtrar dados do Siconfi (2015-2024)
years = range(2015, 2025)
all_dfs = []

print("Lendo arquivos Siconfi...")
for year in years:
    file_pattern = os.path.join(input_dir, f'finbra_mun_despesas-por-funcao_{year}.csv')
    files = glob.glob(file_pattern)
    
    for f in files:
        df_year = pd.read_csv(f, sep=';', encoding='utf-8')
        
        # Filtrar apenas Gestão Ambiental
        df_filtered = df_year[df_year['coluna'].str.contains('Despesas Liquidadas', na=False) & 
                              df_year['conta'].str.contains('Gestão Ambiental', case=False, na=False)].copy()
        
        all_dfs.append(df_filtered[['cd_mun', 'valor_per_capita']])

# Consolidar soma por município
df_siconfi = pd.concat(all_dfs).groupby('cd_mun')['valor_per_capita'].sum().reset_index()
df_siconfi.rename(columns={'valor_per_capita': 'g1_inv_abs'}, inplace=True)

# 2. Carregar base H3
print("Carregando base H3...")
df_h3 = pd.read_parquet(h3_path)

# ==========================================
# CORREÇÃO: Padronizar a chave de cruzamento para texto (string)
# ==========================================
df_siconfi['cd_mun'] = df_siconfi['cd_mun'].astype(str)
df_h3['cd_mun'] = df_h3['cd_mun'].astype(str)

# 3. Join entre H3 e Siconfi
df_final = df_h3.merge(df_siconfi, on='cd_mun', how='left').fillna({'g1_inv_abs': 0})

# 4. Tratamento de Outliers (Winsorização)
df_final['g1_inv_abs'] = mstats.winsorize(df_final['g1_inv_abs'], limits=[0.01, 0.01])

# 5. Normalização (0 a 1)
v_min = df_final['g1_inv_abs'].min()
v_max = df_final['g1_inv_abs'].max()

if v_max - v_min > 0:
    df_final['g1_inv_norm'] = (df_final['g1_inv_abs'] - v_min) / (v_max - v_min)
else:
    df_final['g1_inv_norm'] = 0

# 6. Salvar resultado
df_export = df_final[['h3_id', 'g1_inv_abs', 'g1_inv_norm']]
df_export.to_parquet(output_path, index=False)

print(f"✅ Arquivo salvo com sucesso em: {output_path}")
print(f"Total de registros: {len(df_export)}")