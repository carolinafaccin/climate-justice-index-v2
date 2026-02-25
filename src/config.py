import os
from pathlib import Path

# ==============================================================================
# 1. DIRETÓRIOS
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
RESULTS_DIR = DATA_DIR / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 2. DEFINIÇÕES GLOBAIS
# ==============================================================================
H3_RES = 9
COL_ID_H3 = 'h3_id'

# ==============================================================================
# 3. MAPEAMENTO DE COLUNAS (O ÚNICO LUGAR ONDE VOCÊ PRECISARÁ MEXER)
# ==============================================================================
# Chave: Apelido usado no código | Valor: Nome real da coluna no arquivo .parquet
MAPA_COLUNAS = {
    "e1": "e1_des_norm",
    "e2": "e2_inu_norm",
    "e3": "e3_cos_norm",
    "e4": "e4_cal_norm",
    "e5": "e5_que_norm",
    
    "v1": "v1_ren_norm",
    "v2": "v2_mor_norm",
    "v3": "v3_inf_norm",
    "v4": "v4_edu_norm",
    "v5": "v5_sau_norm",
    
    "p1": "p1_gen_norm",
    "p2": "p2_cri_norm",
    "p3": "p3_ido_norm",
    "p4": "p4_pre_norm",
    "p5": "p5_ind_norm",
    
    "g1": "g1_inv_norm",
    "g2": "g2_par_norm",
    "g3": "g3_res_norm",
    "g4": "g4_rec_norm",
    "g5": "g5_ass_norm"
}

# Estrutura das Dimensões usando APENAS os apelidos
DIMENSOES = {
    "exposicao_climatica": ["e1", "e2", "e3", "e4", "e5"],
    "vulnerabilidade":     ["v1", "v2", "v3", "v4", "v5"],
    "grupos_prioritarios": ["p1", "p2", "p3", "p4", "p5"],
    "gestao_municipal":    ["g1", "g2", "g3", "g4", "g5"]
}

# ==============================================================================
# 4. INPUTS
# ==============================================================================
FILES = {
    "h3": {
        "base_metadados": RAW_DIR / "h3"/ "br_h3_res9.parquet",
        
        "e1": CLEAN_DIR / "br_h3_deslizamentos.parquet",
        "e2": CLEAN_DIR / "br_h3_inundacoes.parquet",
        "e3": CLEAN_DIR / "br_h3_vulnerabilidade_costeira.parquet",
        "e4": CLEAN_DIR / "br_h3_calor.parquet",
        "e5": CLEAN_DIR / "br_h3_queimadas.parquet",
        
        "v1": CLEAN_DIR / "br_h3_vulnerabilidade.parquet",
        "v2": CLEAN_DIR / "br_h3_vulnerabilidade.parquet",
        "v3": CLEAN_DIR / "br_h3_vulnerabilidade.parquet",
        "v4": CLEAN_DIR / "br_h3_vulnerabilidade.parquet",
        "v5": CLEAN_DIR / "br_h3_acessibilidade_saude.parquet",

        "p1": CLEAN_DIR / "br_h3_grupos_prioritarios.parquet",
        "p2": CLEAN_DIR / "br_h3_grupos_prioritarios.parquet",
        "p3": CLEAN_DIR / "br_h3_grupos_prioritarios.parquet",
        "p4": CLEAN_DIR / "br_h3_grupos_prioritarios.parquet",
        "p5": CLEAN_DIR / "br_h3_grupos_prioritarios.parquet",
        
        "g1": CLEAN_DIR / "g1_mun_despesas_liquidadas.csv",
        "g2": CLEAN_DIR / "g2_mun_nupdec.csv",
        "g3": CLEAN_DIR / "g3_mun_alerta.csv",
        "g4": CLEAN_DIR / "g4_mun_mapeamento.csv",
        "g5": CLEAN_DIR / "g5_mun_politicas_direitos_humanos.csv"
    },
    "output": {
        "h3_final": RESULTS_DIR / "br_h3_res9_final.parquet"
    }
}