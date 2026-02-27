import pandas as pd
import sys
from pathlib import Path

# ==============================================================================
# 1. CONFIGURAÇÃO DE AMBIENTE E IMPORTAÇÃO
# ==============================================================================
# Descobre a raiz do projeto e adiciona ao sys.path para conseguir importar o src
RAIZ_PROJETO = str(Path(__file__).resolve().parent.parent)
sys.path.append(RAIZ_PROJETO)

from src import config as cfg

# ==============================================================================
# 2. CAMINHOS DE ENTRADA
# ==============================================================================
# Caminho da malha H3 (agora puxado direto do dicionário FILES_H3 do config)
h3_path = cfg.FILES_H3["base_metadados"]

# Diretório onde estão os CSVs do IBGE/Munic
input_dir = cfg.RAW_DIR / 'ibge' / 'munic' / 'final'

# ==============================================================================
# 3. CARREGAMENTO DA BASE H3
# ==============================================================================
print("Carregando base H3...")
df_h3 = pd.read_parquet(h3_path, columns=['h3_id', 'cd_mun'])

# Padronizar a chave de cruzamento para texto (evita o erro int vs object)
df_h3['cd_mun'] = df_h3['cd_mun'].astype(str)

# ==============================================================================
# 4. MAPEAMENTO DE INDICADORES (Chave -> Arquivo CSV de entrada)
# ==============================================================================
# Não precisamos mais escrever o nome do Parquet de saída aqui, o config já sabe!
arquivos_entrada = {
    'g2': 'g2_mun_nupdec.csv',
    'g3': 'g3_mun_alerta.csv',
    'g4': 'g4_mun_mapeamento.csv',
    'g5': 'g5_mun_politicas_direitos_humanos.csv'
}

# ==============================================================================
# 5. PROCESSAMENTO E CRUZAMENTO
# ==============================================================================
print("\nIniciando o cruzamento espacial...")

for ind_key, csv_in in arquivos_entrada.items():
    csv_path = input_dir / csv_in
    
    # O caminho de saída (CLEAN_DIR + Nome do Parquet) já vem pronto do config.py
    out_path = cfg.FILES_H3[ind_key]
    
    if csv_path.exists():
        print(f"Processando indicador {ind_key.upper()}: {csv_in}...")
        
        # Carregar o CSV do indicador
        df_indicador = pd.read_csv(csv_path)
        
        # Padronizar a chave de cruzamento
        df_indicador['cd_mun'] = df_indicador['cd_mun'].astype(str)
        
        # Merge: Adiciona as colunas do indicador ao DF H3
        df_final = df_h3.merge(df_indicador, on='cd_mun', how='left')
        
        # Salvar o resultado
        df_final.to_parquet(out_path, index=False)
        
        # out_path.name pega apenas o nome final do arquivo para imprimir bonito no terminal
        print(f"✅ Salvo com sucesso: {out_path.name}")
        
    else:
        print(f"⚠️ Arquivo não encontrado: {csv_path}")

print("\nProcessamento dos indicadores concluído!")