import pandas as pd
import os

# 1. Configuração de caminhos
h3_path = '../data/raw/h3/br_h3_res9.parquet'

# Pelo seu código anterior, os CSVs foram salvos em '../data/clean'
# (Se estiverem na pasta 'final' que aparece no seu print, ajuste o input_dir)
input_dir = '../data/raw/ibge/munic/final' 
output_dir = '../data/clean'

# 2. Carregar a malha H3 (apenas colunas necessárias para poupar memória)
print("Carregando base H3...")
df_h3 = pd.read_parquet(h3_path, columns=['h3_id', 'cd_mun'])

# Padronizar a chave de cruzamento para texto (evita o erro int vs object)
df_h3['cd_mun'] = df_h3['cd_mun'].astype(str)

# 3. Dicionário mapeando os CSVs de entrada para os Parquets de saída
# Nota: Padronizei os prefixos para 'br_h3_' para acompanhar o seu arquivo base.
arquivos_indicadores = {
    'g2_mun_nupdec.csv': 'br_h3_g2_mun_nupdec.parquet',
    'g3_mun_alerta.csv': 'br_h3_g3_mun_alerta.parquet',
    'g4_mun_mapeamento.csv': 'br_h3_g4_mun_mapeamento.parquet',
    'g5_mun_politicas_direitos_humanos.csv': 'br_h3_g5_mun_politicas_direitos_humanos.parquet'
}

# 4. Loop para processar e salvar cada indicador
print("\nIniciando o cruzamento espacial...")
for csv_in, parquet_out in arquivos_indicadores.items():
    csv_path = os.path.join(input_dir, csv_in)
    out_path = os.path.join(output_dir, parquet_out)
    
    if os.path.exists(csv_path):
        print(f"Processando: {csv_in}...")
        
        # Carregar o CSV do indicador
        df_indicador = pd.read_csv(csv_path)
        
        # Padronizar a chave de cruzamento
        df_indicador['cd_mun'] = df_indicador['cd_mun'].astype(str)
        
        # Merge: Adiciona as colunas do indicador ao DF H3
        df_final = df_h3.merge(df_indicador, on='cd_mun', how='left')
        
        # Salvar o resultado
        df_final.to_parquet(out_path, index=False)
        print(f"✅ Salvo: {parquet_out}")
        
    else:
        print(f"⚠️ Arquivo não encontrado: {csv_path}")

print("\nProcessamento dos indicadores concluído!")