#!pip install python-bcb
#!pip install requests

import datetime
import numpy as np
import pandas as pd
import requests, os
from bcb import sgs
from .utils import load_processed_data

import logging


#Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


#importando os arquivos
caminho = os.getcwd() + "\\data\\processed\\"
lista = os.listdir(caminho)


# Extrair dados do IPCA do BC
def IPCA_API_BANCE(df):
    #Parâmetros da API do BC
    ipca = 433
    dataInicial = df.data.min().strftime('%d/%m/%Y')
    dataFinal = df.data.max().strftime('%d/%m/%Y')

    #importação dos dados do Banco Central
    url = requests.get(
        f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{ipca}/dados?formato=json&dataInicial={dataInicial}&dataFinal={dataFinal}'
    )

    dados = url.json()

    df_ipca = pd.DataFrame(dados)
    df_ipca = df_ipca.rename(columns={"valor": "IPCA"})

    return df_ipca

# Função para salvar os dados com novas features
def save_engineerd_data(data, save_path):
    """
    Salva os dados IPCA em um arquivo parquet.
    """
    #Salvando arquivo parquet de milho
    logger.info(f"Salvando IPCA BACEN {save_path}...")
    data[["data", "uf", "preco_medio"]].to_parquet("data/processed/" + save_path, index=False)

