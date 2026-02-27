import pandas as pd
import logging
from pathlib import Path

# Importações internas
from . import config as cfg
from . import utils
from . import calculations as calc

def consolidar_inputs(files_dict: dict, join_key: str) -> pd.DataFrame:
    df_master = None
    # Colunas que queremos trazer APENAS do arquivo base_metadados
    METADATA_COLS = ['cd_setor', 'cd_mun', 'nm_mun', 'cd_uf', 'nm_uf', 'sigla_uf', 'area_km2', 'peso_dom', 'qtd_dom']

    # 1. Primeiro, carregamos a base de metadados para garantir que ela seja o df_master
    path_base = files_dict.get("base_metadados")
    if path_base and path_base.is_file():
        logging.info("Carregando base de metadados...")
        df_master = pd.read_parquet(path_base)
        # Mantém apenas join_key + metadados que existirem
        cols_existentes = [join_key] + [c for c in METADATA_COLS if c in df_master.columns]
        df_master = df_master[cols_existentes]
    else:
        logging.error("Arquivo base_metadados não encontrado! O merge falhará.")
        return None

# 2. Agora fazemos o loop pelos indicadores (e1, v1, etc)
    for apelido, path in files_dict.items():
        if apelido == "base_metadados": continue # Pula pois já carregamos
        
        if not Path(path).is_file():
            logging.warning(f"Arquivo não encontrado para {apelido}: {path}")
            continue
            
        # LÊ O ARQUIVO PRIMEIRO
        df_temp = pd.read_parquet(path)
        # DEPOIS FAZ O LOG DO SHAPE (Isso corrige o bug de variável não declarada)
        logging.info(f"Integrando {apelido} | Shape original: {df_temp.shape}")
        
        nome_real_coluna = cfg.MAPA_COLUNAS.get(apelido)
        
        if nome_real_coluna in df_temp.columns:
            # Seleciona apenas o ID e a coluna do dado, renomeando para o apelido (e1, v1...)
            df_temp = df_temp[[join_key, nome_real_coluna]].rename(columns={nome_real_coluna: apelido})
            
            # Merge left: mantém todos os H3 da base e traz o dado se existir
            df_master = pd.merge(df_master, df_temp, on=join_key, how='left')
            
            # Novo Log: Mostra como ficou o arquivo principal após colar a nova coluna
            logging.debug(f"Shape após merge de {apelido}: {df_master.shape}")
        else:
            logging.warning(f"Coluna {nome_real_coluna} não encontrada em {path.name}")

def run_h3():
    logging.info("=== INICIANDO PIPELINE: MALHA H3 (SIMPLIFICADO) ===")
    
    # 1. Consolida os dados
    df_dados = consolidar_inputs(cfg.FILES['h3'], cfg.COL_ID_H3)
    
    if df_dados is None or df_dados.empty:
        logging.error("Nenhum dado encontrado para H3.")
        return

    # 2. Calcula o Índice
    df_calculado = calc.calcular_ijc_simples(df_dados)

    # =========================================================================
    # 3. DIAGNÓSTICO PARA OS LOGS
    # =========================================================================
    logging.info("--- DIAGNÓSTICO DO ARQUIVO FINAL ---")
    
    # A) Logando as colunas
    colunas = df_calculado.columns.tolist()
    logging.info(f"Total de colunas geradas: {len(colunas)}")
    logging.info(f"Lista de colunas: {colunas}")
    
    # B) Logando as estatísticas (apenas das colunas numéricas para não quebrar)
    # O .to_string() é essencial aqui para que o Pandas formate como texto no log
    estatisticas = df_calculado.describe().to_string()
    logging.info(f"Resumo Estatístico do DataFrame Final:\n{estatisticas}")
    # =========================================================================

    # 4. Salva o arquivo
    path_output = cfg.FILES['output']['h3_final']
    utils.save_parquet(df_calculado, path_output)
    logging.info("Processo concluído com sucesso!")

def run():
    try:
        run_h3()
    except Exception as e:
        logging.error(f"Falha crítica no pipeline H3: {e}")

if __name__ == "__main__":
    run()