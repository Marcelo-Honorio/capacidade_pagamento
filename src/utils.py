import pandas as pd
import logging 

#Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Função para carregar os dados processados
def load_processed_data(data_path):
    """
    Carrega os dados processados a partid de um arquivo .parquet
    """
    logger.info(f"Carregando dados processados de {data_path}...")
    data = pd.read_csv(data_path, parse_dates=["data_venda"])
    data = data.rename(columns={"data_venda": "ds", "sku": "unique_id", "venda": "y"})
    return data
