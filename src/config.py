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

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 2. DEFINIÇÕES GLOBAIS
# ==============================================================================
H3_RES = 9
COL_ID_H3 = 'h3_id'

# ==============================================================================
# 3. INDICADORES
# ==============================================================================
INDICADORES = {
    # EXPOSIÇÃO CLIMÁTICA
    "e1": {"dimensao": "exposicao", "col": "e1_des_norm", "file": "br_h3_e1_deslizamentos.parquet"},
    "e2": {"dimensao": "exposicao", "col": "e2_inu_norm", "file": "br_h3_e2_inundacoes.parquet"},
    "e3": {"dimensao": "exposicao", "col": "e3_cos_norm", "file": "br_h3_e3_vulnerabilidade_costeira.parquet"},
    "e4": {"dimensao": "exposicao", "col": "e4_cal_norm", "file": "br_h3_e4_calor.parquet"},
    "e5": {"dimensao": "exposicao", "col": "e5_que_norm", "file": "br_h3_e5_queimadas.parquet"},

    # VULNERABILIDADE
    "v1": {"dimensao": "vulnerabilidade", "col": "v1_ren_norm", "file": "br_h3_v1_renda.parquet"},
    "v2": {"dimensao": "vulnerabilidade", "col": "v2_mor_norm", "file": "br_h3_v2_moradia.parquet"},
    "v3": {"dimensao": "vulnerabilidade", "col": "v3_inf_norm", "file": "br_h3_v3_infraestrutura.parquet"},
    "v4": {"dimensao": "vulnerabilidade", "col": "v4_edu_norm", "file": "br_h3_v4_educacao.parquet"},
    "v5": {"dimensao": "vulnerabilidade", "col": "v5_sau_norm", "file": "br_h3_v5_modelo_gravitacional_saude.parquet"},

    # GRUPOS PRIORITÁRIOS
    "p1": {"dimensao": "grupos_prioritarios", "col": "p1_gen_norm", "file": "br_h3_p1_mulheres_chefes_familia.parquet"},
    "p2": {"dimensao": "grupos_prioritarios", "col": "p2_cri_norm", "file": "br_h3_p2_criancas.parquet"},
    "p3": {"dimensao": "grupos_prioritarios", "col": "p3_ido_norm", "file": "br_h3_p3_idosos.parquet"},
    "p4": {"dimensao": "grupos_prioritarios", "col": "p4_pre_norm", "file": "br_h3_p4_pretos_pardos.parquet"},
    "p5": {"dimensao": "grupos_prioritarios", "col": "p5_ind_norm", "file": "br_h3_p5_indigenas_quilombolas.parquet"},

    # CAPACIDADE DE GESTÃO MUNICIPAL
    "g1": {"dimensao": "gestao_municipal", "col": "g1_inv_norm", "file": "b3_h3_g1_mun_despesas_liquidadas.parquet"},
    "g2": {"dimensao": "gestao_municipal", "col": "g2_par_norm", "file": "b3_h3_g2_mun_nupdec.parquet"},
    "g3": {"dimensao": "gestao_municipal", "col": "g3_alerta_norm", "file": "b3_h3_g3_mun_alerta.parquet"},
    "g4": {"dimensao": "gestao_municipal", "col": "g4_map_norm", "file": "b3_h3_g4_mun_mapeamento.parquet"},
    "g5": {"dimensao": "gestao_municipal", "col": "g5_pol_norm", "file": "b3_h3_g5_mun_politicas_direitos_humanos.parquet"},
}

# ==============================================================================
# 4. GERAÇÃO AUTOMÁTICA DOS DICIONÁRIOS
# ==============================================================================

# Gera o MAPA_COLUNAS: {'e1': 'e1_des_norm', ...}
MAPA_COLUNAS = {k: v['col'] for k, v in INDICADORES.items()}

# Gera o FILES['h3']: {'e1': PosixPath('.../br_h3_e1_deslizamentos.parquet'), ...}
FILES_H3 = {k: CLEAN_DIR / v['file'] for k, v in INDICADORES.items()}
FILES_H3["base_metadados"] = RAW_DIR / "h3" / "br_h3_res9.parquet"

# Gera o DIMENSOES agrupando os apelidos por categoria
DIMENSOES = {}
for k, v in INDICADORES.items():
    dim = v['dimensao']
    if dim not in DIMENSOES:
        DIMENSOES[dim] = []
    DIMENSOES[dim].append(k)

# Dicionário de arquivos final
FILES = {
    "h3": FILES_H3,
    "output": {
        "h3_final": RESULTS_DIR / "br_h3_res9_ijc.parquet"
    }
}