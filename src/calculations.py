import pandas as pd
import logging
from . import config as cfg

def calcular_ijc_simples(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Calculando IJC via Média Simples...")
    df = df.copy()

    # 1. Calcula a média de cada dimensão
    for dimensao, colunas in cfg.DIMENSOES.items():
        # Garante que as colunas existam, se não existir, assume 0
        existentes = [c for c in colunas if c in df.columns]
        df[f'ind_{dimensao}'] = df[existentes].mean(axis=1)
        logging.info(f"Dimensão {dimensao} calculada.")

    # 2. Índice Final (Média das 4 dimensões)
    # Lista das colunas criadas no passo anterior
    cols_indices = [f'ind_{d}' for d in cfg.DIMENSOES.keys()]
    
    # IJC Final = Média simples entre Exposição, Vulnerabilidade, Interseccionalidade e Governança
    df['ijc_final'] = df[cols_indices].mean(axis=1)
    
    return df