import pandas as pd
import geopandas as gpd
import numpy as np
import logging
import re
from pathlib import Path

# Configuração de Logs básica
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_next_version_path(path: Path) -> Path:
    """
    Verifica se o arquivo existe. Se existir, incrementa a versão (v1 -> v2 -> v3).
    Exemplo: 'arquivo.gpkg' -> 'arquivo_v1.gpkg' -> 'arquivo_v2.gpkg'
    """
    path = Path(path)
    
    # Se o arquivo não existe e não tem padrão de versão, adiciona _v1
    if not path.exists():
        # Se o nome original já não termina com _vX, adicionamos _v1 para começar organizado
        if not re.search(r'_v\d+$', path.stem):
            return path.with_name(f"{path.stem}_v1{path.suffix}")
        return path

    # Se já existe, vamos descobrir qual a próxima versão
    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    # Procura por padrão "_v1", "_v2" no final do nome
    match = re.search(r'_v(\d+)$', stem)
    
    if match:
        current_version = int(match.group(1))
        base_name = stem[:match.start()] # Remove a versão antiga
        next_version = current_version + 1
    else:
        # Se o arquivo existe mas não tem versão (ex: "dados.parquet"), o próximo é "dados_v1.parquet"
        base_name = stem
        next_version = 1

    # Loop para garantir que encontramos um slot vazio (caso pule de v1 para v5 manualmente)
    while True:
        new_path = parent / f"{base_name}_v{next_version}{suffix}"
        if not new_path.exists():
            return new_path
        next_version += 1

def normalize_minmax(series: pd.Series, winsorize: bool = False, limits: tuple = (0.01, 0.99)) -> pd.Series:
    """
    Normaliza uma série pandas entre 0 e 1 (Min-Max Scaling).
    
    Parâmetros:
        series: A coluna de dados.
        winsorize: Se True, aplica corte nos percentis extremos antes de normalizar.
        limits: Tupla (min, max) dos quantis para corte (ex: 0.01 e 0.99).
    """
    # Garante numérico e substitui infinitos
    s = pd.to_numeric(series, errors='coerce')
    
    # 1. Aplica Winsorization se solicitado
    if winsorize:
        lower_bound = s.quantile(limits[0])
        upper_bound = s.quantile(limits[1])
        s = s.clip(lower=lower_bound, upper=upper_bound)
    
    # 2. Normalização Min-Max padrão
    min_val = s.min()
    max_val = s.max()
    
    # Evita divisão por zero
    if max_val == min_val:
        return pd.Series(0.0, index=s.index)
        
    return (s - min_val) / (max_val - min_val)

def load_gpkg(path: Path, layer: str = None) -> gpd.GeoDataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    logging.info(f"Carregando GPKG: {path.name}...")
    try:
        return gpd.read_file(path, engine='pyogrio', layer=layer)
    except Exception as e:
        logging.warning(f"Falha com pyogrio ({e}), tentando driver padrão fiona...")
        return gpd.read_file(path, layer=layer)

def save_gpkg(gdf: gpd.GeoDataFrame, path: Path, layer: str = None):
    """Salva GPKG com versionamento automático."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Gera o caminho com versão (ex: _v2)
    final_path = get_next_version_path(path)
    
    logging.info(f"Salvando GPKG em: {final_path.name}...")
    try:
        gdf.to_file(final_path, driver='GPKG', engine='pyogrio', layer=layer)
    except Exception as e:
        logging.warning(f"Erro ao salvar com pyogrio ({e}), tentando padrão...")
        gdf.to_file(final_path, driver='GPKG', layer=layer)
    logging.info("Salvo com sucesso.")

def save_parquet(df: pd.DataFrame, path: Path):
    """Salva Parquet com versionamento automático."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Gera o caminho com versão
    final_path = get_next_version_path(path)
    
    logging.info(f"Salvando Parquet em: {final_path.name}...")
    df.to_parquet(final_path)
    logging.info("Salvo com sucesso.")

def merge_csv_to_gdf(gdf: gpd.GeoDataFrame, csv_path: Path, join_key: str, cols_to_keep: list = None) -> gpd.GeoDataFrame:
    if not csv_path.exists():
        logging.warning(f"Arquivo CSV não encontrado: {csv_path.name}. Merge ignorado.")
        return gdf
    logging.info(f"Integrando {csv_path.name}...")
    try:
        usecols = ([join_key] + cols_to_keep) if cols_to_keep else None
        df_temp = pd.read_csv(csv_path, usecols=usecols, dtype={join_key: str})
        gdf[join_key] = gdf[join_key].astype(str)
        gdf = gdf.merge(df_temp, on=join_key, how='left')
        if cols_to_keep:
            for col in cols_to_keep:
                if col in gdf.columns:
                    gdf[col] = gdf[col].fillna(0)
        return gdf
    except Exception as e:
        logging.error(f"Erro ao integrar {csv_path.name}: {e}")
        return gdf