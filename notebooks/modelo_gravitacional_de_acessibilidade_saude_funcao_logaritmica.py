import pandas as pd
import numpy as np
import h3
import geopandas as gpd
from scipy.spatial import cKDTree

"""
Modelo Gravitacional de Acessibilidade.

Medição da "Força de Atração" da infraestrutura de saúde. Um hospital grande atrai pessoas de longe (tem muito peso), enquanto um posto de saúde atrai pessoas apenas das ruas vizinhas.

Nós calculamos um índice dividindo a Capacidade pela Distância.
Se a distância aumenta, o valor daquele hospital para o hexágono diminui.

Para ficar perfeito, nós usamos um algoritmo rápido para encontrar os 3 estabelecimentos mais próximos de cada hexágono.
Assim, se o hexágono está do lado de um postinho, mas a 2km de um hospitalão, o cálculo vai somar a influência dos dois.

A fórmula da sua coluna v5_sau_abs será:
v5_sau_abs = Soma( Capacidade / (Distância_em_metros + 100) )
(Somamos 100 metros na base apenas para evitar que um hexágono colado no hospital divida a conta por zero).

Nota Metodológica: Indicador de Acesso à Saúde (v5_sau)
O indicador de acesso à infraestrutura de saúde foi desenvolvido a partir de um modelo espacial gravitacional, que mensura simultaneamente a distância euclidiana (em metros) e a capacidade de atendimento dos estabelecimentos listados no CNES. 
Para cada hexágono da malha (H3, resolução 9), calculou-se a razão entre o escore de capacidade estrutural e a distância aos três estabelecimentos mais próximos (v5_sau_abs). Para atenuar as disparidades extremas de oferta em grandes complexos hospitalares urbanos, aplicou-se uma transformação logarítmica (base 10) ao resultado absoluto, seguida por uma normalização em escala linear de 0 a 1 (v5_sau_norm), indicando o grau relativo de proteção por oferta de saúde.
"""

# =====================================================================
# 1. CARREGAMENTO E PADRONIZAÇÃO DOS DADOS
# =====================================================================
print("Carregando os dados...")
df_h3 = pd.read_parquet('../data/raw/h3/br_h3_res9.parquet')
df_h3.columns = df_h3.columns.str.lower()

df_cnes = pd.read_csv('../data/raw/cnes/cnes_estabelecimentos.csv', sep=';', low_memory=False, encoding='latin1')
df_cnes.columns = df_cnes.columns.str.lower()

# =====================================================================
# 2. PROCESSAMENTO DO CNES (COORDENADAS E CAPACIDADE)
# =====================================================================
print("Limpando coordenadas e calculando Escore de Capacidade...")
df_cnes['nu_latitude'] = pd.to_numeric(df_cnes['nu_latitude'], errors='coerce')
df_cnes['nu_longitude'] = pd.to_numeric(df_cnes['nu_longitude'], errors='coerce')
df_cnes = df_cnes.dropna(subset=['nu_latitude', 'nu_longitude'])

servicos = [
    'st_centro_cirurgico', 'st_centro_obstetrico', 'st_centro_neonatal',
    'st_atend_hospitalar', 'st_servico_apoio', 'st_atend_ambulatorial'
]

for col in servicos:
    if col in df_cnes.columns:
        df_cnes[col] = pd.to_numeric(df_cnes[col], errors='coerce').fillna(0)

# Escore de Capacidade do Estabelecimento (Peso)
df_cnes['score_capacidade'] = df_cnes[servicos].sum(axis=1) + 1

# =====================================================================
# 3. PREPARAÇÃO ESPACIAL (CONVERSÃO PARA METROS - EPSG:5880)
# =====================================================================
print("Convertendo coordenadas para sistema métrico (SIRGAS 2000)...")

# Transforma o CNES em GeoDataFrame e converte para metros (EPSG:5880 - Policônica do Brasil)
gdf_cnes = gpd.GeoDataFrame(
    df_cnes, 
    geometry=gpd.points_from_xy(df_cnes.nu_longitude, df_cnes.nu_latitude),
    crs="EPSG:4326"
).to_crs("EPSG:5880")

# Função para pegar o centroide (lat, lon) de cada hexágono H3
def get_h3_centroid(h3_id):
    try:
        return h3.cell_to_latlng(h3_id) if hasattr(h3, 'cell_to_latlng') else h3.h3_to_geo(h3_id)
    except:
        return (np.nan, np.nan)

# Extrai o centroide de todos os 4.5 milhões de hexágonos
centroides = df_h3['h3_id'].apply(get_h3_centroid)
df_h3['lat'] = [c[0] for c in centroides]
df_h3['lng'] = [c[1] for c in centroides]

# Transforma os hexágonos em GeoDataFrame e converte para metros
gdf_h3 = gpd.GeoDataFrame(
    df_h3,
    geometry=gpd.points_from_xy(df_h3.lng, df_h3.lat),
    crs="EPSG:4326"
).to_crs("EPSG:5880")

# =====================================================================
# 4. CÁLCULO DA DISTÂNCIA EUCLIDIANA E MODELO GRAVITACIONAL
# =====================================================================
print("Calculando Distância Euclidiana e Escore Gravitacional...")

# Extrai as coordenadas X e Y em metros
coords_cnes = np.array(list(zip(gdf_cnes.geometry.x, gdf_cnes.geometry.y)))
coords_h3 = np.array(list(zip(gdf_h3.geometry.x, gdf_h3.geometry.y)))
capacidades = gdf_cnes['score_capacidade'].values

# Cria a árvore espacial (MUITO rápido para buscar distâncias)
arvore = cKDTree(coords_cnes)

# Busca os 3 estabelecimentos de saúde mais próximos para CADA hexágono
# distances: matriz com as distâncias em metros
# indices: matriz com a posição do estabelecimento no array do CNES
distances, indices = arvore.query(coords_h3, k=3)

# Calcula o peso gravitacional para os 3 mais próximos: (Capacidade / Distância ajustada)
pesos_gravitacionais = capacidades[indices] / (distances + 100)

# O Escore Absoluto (v5_sau_abs) é a soma da atração desses 3 estabelecimentos
df_h3['v5_sau_abs'] = np.sum(pesos_gravitacionais, axis=1)

# Remove as colunas auxiliares de coordenadas
df_h3 = df_h3.drop(columns=['lat', 'lng', 'geometry'], errors='ignore')

# =====================================================================
# 5. TRATAMENTO COM FUNÇÃO LOGARÍTMICA E NORMALIZAÇÃO
# =====================================================================
print("Aplicando função logarítmica e normalizando os dados...")

# Aplica a função logarítmica (ln(1 + x)) para achatar os outliers gigantes
df_h3['v5_sau_log'] = np.log1p(df_h3['v5_sau_abs'])

# Normalização Min-Max (escala 0 a 1) usando a nova coluna logarítmica
min_val = df_h3['v5_sau_log'].min()
max_val = df_h3['v5_sau_log'].max()

if max_val > min_val:
    df_h3['v5_sau_norm'] = (df_h3['v5_sau_log'] - min_val) / (max_val - min_val)
else:
    df_h3['v5_sau_norm'] = 0.0

# Remove a coluna intermediária do log para deixar o dataframe limpo
df_h3 = df_h3.drop(columns=['v5_sau_log'])

# =====================================================================
# 6. DIAGNÓSTICO DOS DADOS
# =====================================================================
print("\n" + "="*50)
print("DIAGNÓSTICO DO MODELO GRAVITACIONAL E DISTÂNCIA")
print("="*50)

print(f"Total de estabelecimentos CNES processados: {len(df_cnes):,}")
print(f"Total de hexágonos H3 processados: {len(df_h3):,}")

# A variável 'distances' (matriz gerada pela KDTree) guarda as distâncias em metros.
# A coluna 0 guarda a distância para o 1º estabelecimento mais próximo.
distancia_minima = distances[:, 0]

print(f"\n--- DISTÂNCIA ATÉ O ESTABELECIMENTO MAIS PRÓXIMO ---")
print(f"Média no Brasil: {np.mean(distancia_minima)/1000:.2f} km")
print(f"Mediana (50% do Brasil está a até): {np.median(distancia_minima)/1000:.2f} km")
print(f"Distância Máxima (O local mais isolado): {np.max(distancia_minima)/1000:.2f} km")

print("\n--- ESTATÍSTICAS DA VARIÁVEL NORMALIZADA (v5_sau_norm) ---")
print(df_h3['v5_sau_norm'].describe())

# Define as colunas para mostrar no Top/Bottom
cols_show = ['h3_id', 'nm_mun', 'v5_sau_abs', 'v5_sau_norm']
if 'nm_uf' in df_h3.columns:
    cols_show.insert(2, 'nm_uf')
if 'qtd_dom' in df_h3.columns:
    cols_show.insert(len(cols_show)-2, 'qtd_dom')

print("\n--- TOP 5 HEXÁGONOS COM MAIOR ACESSIBILIDADE (Centros Urbanos) ---")
print(df_h3.sort_values(by='v5_sau_norm', ascending=False)[cols_show].head())

print("\n--- BOTTOM 5 HEXÁGONOS COM MENOR ACESSIBILIDADE (Locais Isolados) ---")
print(df_h3.sort_values(by='v5_sau_norm', ascending=True)[cols_show].head())

print("="*50 + "\n")


# =====================================================================
# 7. SALVAR ARQUIVO FINAL
# =====================================================================
df_h3[['h3_id', 'v5_sau_abs', 'v5_sau_norm']].to_parquet('../data/clean/br_h3_modelo_gravitacional_saude.parquet', index=False)