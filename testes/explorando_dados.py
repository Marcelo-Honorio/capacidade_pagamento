import pandas as pd
import os, re

#importando o arquivo de milho
caminho = os.getcwd() + "\\data\\raw\\"
lista = os.listdir(caminho, )

#Função para extrair as siglas dos estados
def extrair_UF(string):
    m = re.search(r"/([A-Z]{2})/", string)
    if m:
        return m.group(1)
    
    return None

#função para transformar colunas UF em linha
def melt_UF(df):
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


# Tratamento em todos os arquivos
for i in lista:
    arquivo = caminho + i
    #Importando os arquivos
    df = pd.read_csv(arquivo, sep=";")
    #Colunas que tem UF
    cols_com_uf = [i for i in df.columns if extrair_UF(i)]

    #Alongar o df transformando as colunas UF em linhas
    df = melt_UF(df)

    #nome do arquivo
    nome_salvo = i.replace(".csv", ".parquet") 

    #Salvando arquivo parquet de milho
    df[["data", "uf", "preco_medio"]].to_parquet("data/processed/" + nome_salvo, index=False)