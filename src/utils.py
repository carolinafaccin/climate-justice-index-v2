import pandas as pd
import numpy as np
import logging
import re
from pathlib import Path
from datetime import datetime
from . import config as cfg  # Precisamos importar o config para saber onde é a pasta logs

# Função para configurar os logs (Apague o logging.basicConfig antigo daqui)
def setup_logging():
    # Gera um nome de arquivo único com a data/hora atual (ex: pipeline_20260226_093000.log)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = cfg.LOGS_DIR / f"pipeline_{timestamp}.log"

    # Criar um logger raiz (root)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG) # O logger mestre captura tudo

    # Limpa configurações antigas caso você rode mais de uma vez na mesma sessão
    if logger.hasHandlers():
        logger.handlers.clear()

    # 1. Configuração do arquivo de texto (Salva tudo, até o DEBUG)
    file_format = logging.Formatter('%(asctime)s | %(levelname)-8s | %(module)s:%(funcName)s:%(lineno)d - %(message)s')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_format)

    # 2. Configuração do Terminal (Mostra apenas INFO para cima, para não poluir a tela)
    console_format = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_format)

    # Adiciona os dois comportamentos ao projeto
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info(f"Logs configurados. Arquivo de log detalhado em: {log_file}")

def get_next_version_path(path: Path) -> Path:
    """
    Verifica se o arquivo existe. Se existir, incrementa a versão (v1 -> v2 -> v3).
    Exemplo: 'resultado.parquet' -> 'resultado_v1.parquet' -> 'resultado_v2.parquet'
    """
    path = Path(path)
    
    if not path.exists():
        if not re.search(r'_v\d+$', path.stem):
            return path.with_name(f"{path.stem}_v1{path.suffix}")
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    match = re.search(r'_v(\d+)$', stem)
    
    if match:
        current_version = int(match.group(1))
        base_name = stem[:match.start()]
        next_version = current_version + 1
    else:
        base_name = stem
        next_version = 1

    while True:
        new_path = parent / f"{base_name}_v{next_version}{suffix}"
        if not new_path.exists():
            return new_path
        next_version += 1

def normalize_minmax(series: pd.Series, winsorize: bool = False, limits: tuple = (0.01, 0.99)) -> pd.Series:
    """
    Normaliza uma série pandas entre 0 e 1 (Min-Max Scaling).
    """
    s = pd.to_numeric(series, errors='coerce')
    
    if winsorize:
        lower_bound = s.quantile(limits[0])
        upper_bound = s.quantile(limits[1])
        s = s.clip(lower=lower_bound, upper=upper_bound)
    
    min_val = s.min()
    max_val = s.max()
    
    if max_val == min_val:
        return pd.Series(0.0, index=s.index)
        
    return (s - min_val) / (max_val - min_val)

def save_parquet(df: pd.DataFrame, path: Path):
    """Salva Parquet com versionamento automático."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    final_path = get_next_version_path(path)
    
    logging.info(f"Salvando Parquet em: {final_path.name}...")
    df.to_parquet(final_path)
    logging.info("Salvo com sucesso.")