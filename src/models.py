#bibliotecas
import logging, os, re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau, ModelCheckpoint
from . import config
from .utils import make_supervised

#Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Caminho e lista de documentos 
caminho = os.getcwd() + "\\data\\processed\\"
lista = os.listdir(caminho)
arquivo = [i for i in lista if re.search(r"bacen", i)][0]

#Função para carregar os dados processados
def load_processed_data(data_path):
    """
    Carrega os dados processados na pasta \\data\\processed.
    """
    logger.info(f"Carregando dados processados de {data_path}...")
    data = pd.read_parquet(data_path, sep=";")
    return data

# Decomposição e dessazonalização
def decomposicao_dessazonalizacao(vetor_preco):
    # Decomposição e dessazonalização
    ts = vetor_preco
    decomposition = seasonal_decompose(ts, model="multiplicative", period=12)
    seasonal = decomposition.seasonal
    ts_deseasonalized = ts / seasonal
    
    return ts, decomposition, seasonal, ts_deseasonalized

# Ajustando e Construindo ARIMA
def build_ARIMA_fit(ts_deseasonalized, size_train, ts, seasonal):
    model = ARIMA(ts_deseasonalized,  order = (2, 1, 2))
    model_fit = model.fit()

    # Separar treino/teste
    split = int(len(ts_deseasonalized)*size_train)
    test_dates = ts_deseasonalized.index[split+1:]
    forecast_arima = model_fit.forecast(steps=len(ts) - split - 1)

    # Replicar sazonalidade
    seasonal_avg = seasonal.groupby(seasonal.index.month).mean()
    forecast_arima_real = forecast_arima.values * [seasonal_avg[d.month] for d in test_dates]
    real = ts[test_dates]

    return forecast_arima_real, real

# Construindo LSTM
def build_lstm(input_len: int, lstm_units: int = 200) -> Sequential:
    model = Sequential([
        LSTM(lstm_units, input_shape=(input_len, 1), return_sequences=False),
        Dense(1)
    ])
    model.compile(optimizer=Adam(learning_rate=config.LR), loss="mse")

# Fit model LSTM
def fit_lstm(train_series: pd.Series, lstm_units: int = 200, scaler_type: str = "minmax"):
    scaler = StandardScaler() if scaler_type.lower()=="standard" else MinMaxScaler()
    s = scaler.fit_transform(train_series.values.reshape(-1,1)).ravel()

    X, y = make_supervised(s, config.WINDOW_SIZE)
    X = X[..., None]

    model = build_lstm(config.WINDOW_SIZE, lstm_units=lstm_units)
    es = EarlyStopping(patience=config.PATIENCE, restore_best_weights=True, monitor="val_loss")
    rl = ReduceLROnPlateau(patience=max(3, config.PATIENCE//3), factor=0.5, monitor="val_loss")
    ckpt_path = str(config.MODELS_DIR / f"lstm_{lstm_units}_{scaler_type}.keras")
    ck = ModelCheckpoint(ckpt_path, save_best_only=True, monitor="val_loss")

    split = int(len(X) * 0.9)
    Xtr, ytr = X[:split], y[:split]
    Xva, yva = X[split:], y[split:]

    hist = model.fit(
        Xtr, ytr,
        validation_data=(Xva, yva),
        epochs=config.EPOCHS,
        batch_size=config.BATCH_SIZE,
        callbacks=[es, rl, ck],
        verbose=1
    )

    return {"model": model, "scaler": scaler, "history": hist}

# Forecast e Window size
def forecast(model, scaler, series: pd.Series, n_future: int):
    s = scaler.transform(series.values.reshape(-1,1)).ravel()
    window = s[-config.WINDOW_SIZE:]
    preds = []
    for _ in range(n_future):
        x = window.reshape(1, config.WINDOW_SIZE, 1)
        yhat = model.predict(x, verbose=0).ravel()[0]
        preds.append(yhat)
        window = np.concatenate([window[1:], [yhat]])
    preds = scaler.inverse_transform(np.array(preds).reshape(-1,1)).ravel()
    return preds

