#Bibliotecas
import pandas as pd
import logging
import os, re

#Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#importando os arquivos
caminho = os.getcwd() + "\\data\\raw\\"
lista = os.listdir(caminho)

#Função para carregar os dados processados
def load_raw_data(data_path):
    """
    Carrega os dados processados a partir de um arquivo CSV.
    """
    logger.info(f"Carregando dados processados de {data_path}...")
    data = pd.read_csv(data_path, sep=";")
    return data

#Função para extrair as siglas dos estados
def extrair_UF(string):
    m = re.search(r"/([A-Z]{2})/", string)
    if m:
        return m.group(1)
    
    return None

#função para transformar colunas UF em linha
def melt_UF(df):
    #Colunas que tem UF
    cols_com_uf = [i for i in df.columns if extrair_UF(i)]
    # melt df
    df_long = df.melt(
        id_vars=df.columns[0],
        value_vars=cols_com_uf,
        var_name="coluna_original",
        value_name="preco_medio"
    )
    #Extrair UF para uma nova coluna
    df_long["uf"] = df_long["coluna_original"].apply(extrair_UF)

    #Renomear as colunas
    df_long.columns = ["data", "coluna_original", "preco_medio", "uf"]

    return df_long


## transforar colunas "FEV-2014" em 01/02/2014
def transform_month_abbr_date_strings(df, column_name):
    """
    Transforms date strings like 'FEV-2014' in a DataFrame column
    to '01/MM/YYYY' string format.
    """
    month_map = {
        "JAN": "01", "FEV": "02", "MAR": "03", "ABR": "04",
        "MAI": "05", "JUN": "06", "JUL": "07", "AGO": "08",
        "SET": "09", "OUT": "10", "NOV": "11", "DEZ": "12"
    }

    def _transform_single_date_str(date_str):
        if pd.isna(date_str) or not isinstance(date_str, str):
            return date_str # Mantém NaN/None ou valores não-string como estão
        parts = date_str.upper().split('-') # Converte para maiúsculas para correspondência robusta
        if len(parts) == 2:
            month_abbr, year = parts
            month_num = month_map.get(month_abbr)
            if month_num:
                return f"01/{month_num}/{year}"
        return date_str # Retorna o original se o formato não corresponder ou o mês não for encontrado

    df[column_name] = df[column_name].apply(_transform_single_date_str)
    return df

# Função para salvar os dados com novas features
def save_engineerd_data(data, save_path):
    """
    Salva os dados transformados em um arquivo parquet.
    """
    # Isso converterá as strings '01/MM/YYYY' em objetos datetime apropriados.
    data['data'] = pd.to_datetime(data['data'], format="%d/%m/%Y", errors='coerce')
    #nome do arquivo
    nome_salvo = save_path.replace(".csv", ".parquet")
    #Salvando arquivo parquet de milho
    logger.info(f"Salvando dados com novas features em {save_path}...")
    data[["data", "uf", "preco_medio"]].to_parquet("data/processed/" + nome_salvo, index=False)

# Função principal
def main():
    """
    Função principal para executar o pipeline de engenharia de features.
    """
    for i in lista:
        arquivo = caminho + i
        try:
            # 1. Carregar dados processados
            processed_data = load_raw_data(arquivo)

            # 2. Alongar o df transformando as colunas UF em linhas
            long_data = melt_UF(processed_data)

            # 3. Transformar a coluna 'data' em string '%d/%m/%Y'
            transform_data = transform_month_abbr_date_strings(long_data, 'data')

            # 4. Salvar dados transformado
            save_engineerd_data(transform_data, i)

            logger.info("Engenharia de features concluída com sucesso!")

        except Exception as e:
            logger.error(f"Erro durante e engenharia de features: {e}")

# Executar o pipeline
if __name__ == "__main__":
    main()


