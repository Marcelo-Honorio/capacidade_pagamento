#!pip install python-bcb
#!pip install requests

import datetime
import pandas as pd
import requests, os, re, logging
from bcb import sgs
from datetime import datetime

#Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


#importando os arquivos
caminho = os.getcwd() + "\\data\\processed\\"
lista = os.listdir(caminho)
lista = [i for i in lista if not re.search(r"bacen", i)]



# Extrair valores máximo e minimo
def max_min(lista):
    dataInicial = datetime.now().strftime("%d/%m/%Y")
    dataFinal =  datetime.strptime("01/01/2000", "%d/%m/%Y").strftime("%d/%m/%Y")

    #lista de arquivos
    for i in lista:
        df = pd.read_parquet(caminho + i)
        minimo = df.data.min().strftime('%d/%m/%Y')
        maximo = df.data.max().strftime('%d/%m/%Y')
        #Testar valores minimo e máximo
        if minimo < dataInicial:
            dataInicial = minimo
        if maximo > dataFinal:
            dataFinal = maximo

    return dataInicial, dataFinal 

# Extrair dados do IPCA do BC
def IPCA_API_BANCE(dataInicial, dataFinal):
    #Parâmetros da API do BC
    ipca = 433
    dataInicial = dataInicial
    dataFinal = dataFinal

    #importação dos dados do Banco Central
    url = requests.get(
        f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{ipca}/dados?formato=json&dataInicial={dataInicial}&dataFinal={dataFinal}'
    )
    logger.info(f"Capturando IPCA BACEN..{dataInicial} a {dataFinal}")
    dados = url.json()
    
    df_ipca = pd.DataFrame(dados)
    df_ipca = df_ipca.rename(columns={"valor": "IPCA"})
    #transformar string valor
    df_ipca["IPCA"] = df_ipca.IPCA.astype("float64")

    return df_ipca

# Função para salvar os dados com novas features
def save_captured_data(data, save_path):
    """
    Salva os dados IPCA em um arquivo parquet.
    """
    #Salvando arquivo parquet de milho
    logger.info(f"Salvando IPCA BACEN {save_path}...")
    data.to_parquet("data/processed/" + save_path, index=False)

def main():
    """
    Função principal para executar o pipeline de engenharia de features.
    """
    try:
        # 1. Buscar as datas minimas e máximas
        dataInicial, dataFinal = max_min(lista)
        # 2. Capturar dados do BACEN
        df_ipca = IPCA_API_BANCE(dataInicial, dataFinal)
        # 3. Salvar os arquivos na pasta /processed
        save_captured_data(df_ipca, "ipca_bacen.parquet")
    except Exception as e:
        logger.error(f"Erro na captura de dados: {e}")

# Executar o pipeline
if __name__ == "__main__":
    main()
