from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUTS_DIR = BASE_DIR / "outputs"
EDA_DIR = OUTPUTS_DIR / "eda"
REPORTS_DIR = OUTPUTS_DIR / "reports"
MODELS_DIR = OUTPUTS_DIR / "models"
PLOTS_DIR = OUTPUTS_DIR / "plots"

DATA_FILE = DATA_DIR / "timeseries.csv"  # Espera colunas: data, preco_milho_atualizado

DATE_COL = "data"
VALUE_COL = "preco_milho_atualizado"  # altere para "preco_soja_atualizado" se necessário

FREQ = "MS"  # mensal

SPLIT_DATE = None
TEST_SIZE_RATIO = 0.2

WINDOW_SIZE = 24
BATCH_SIZE = 32
EPOCHS = 500
LR = 5e-3
PATIENCE = 10

N_FUTURE = 12