from __future__ import annotations
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import acf, pacf, adfuller, kpss
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.tsa.seasonal import STL
from typing import Dict, Tuple

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
    
##########################################################################################################
def make_supervised(series: np.ndarray, window_size: int):
    X, y = [], []
    for i in range(len(series) - window_size):
        X.append(series[i:i+window_size])
        y.append(series[i+window_size])
    return np.array(X), np.array(y)

def seasonal_strength(stl_res) -> Dict[str, float]:
    resid_var = np.var(stl_res.resid[~np.isnan(stl_res.resid)])
    seas_var  = np.var((stl_res.seasonal + stl_res.resid)[~np.isnan(stl_res.seasonal + stl_res.resid)])
    trend_var = np.var((stl_res.trend + stl_res.resid)[~np.isnan(stl_res.trend + stl_res.resid)])
    Fs = max(0.0, 1 - resid_var / seas_var) if seas_var > 0 else 0.0
    Ft = max(0.0, 1 - resid_var / trend_var) if trend_var > 0 else 0.0
    return {"F_seasonal": Fs, "F_trend": Ft}

def run_adf(series: pd.Series):
    res = adfuller(series.dropna(), autolag="AIC")
    return {"statistic": res[0], "pvalue": res[1], "lags": res[2], "nobs": res[3]}

def run_kpss(series: pd.Series, regression="c"):
    stat, pval, lags, crit = kpss(series.dropna(), regression=regression, nlags="auto")
    return {"statistic": stat, "pvalue": pval, "lags": lags, "crit": crit}

def run_ljung_box(series: pd.Series, lags=24):
    lb = acorr_ljungbox(series.dropna(), lags=[lags], return_df=True)
    row = lb.iloc[0]
    return {"lb_stat": float(row["lb_stat"]), "lb_pvalue": float(row["lb_pvalue"])}

def plot_ts(df: pd.DataFrame, out_path: str, title="Série temporal"):
    plt.figure()
    df.plot(legend=False)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def plot_acf_pacf(series: pd.Series, out_acf: str, out_pacf: str, nlags=24):
    plt.figure()
    vals = acf(series.dropna(), nlags=nlags)
    plt.stem(range(len(vals)), vals, use_line_collection=True)
    plt.title("ACF")
    plt.tight_layout()
    plt.savefig(out_acf)
    plt.close()
    plt.figure()
    vals = pacf(series.dropna(), nlags=nlags, method="ywunbiased")
    plt.stem(range(len(vals)), vals, use_line_collection=True)
    plt.title("PACF")
    plt.tight_layout()
    plt.savefig(out_pacf)
    plt.close()

def stl_decompose(series: pd.Series, period: int):
    stl = STL(series, period=period, robust=True)
    res = stl.fit()
    return stl, res

def plot_stl(res, out_path: str):
    fig = res.plot()
    fig.set_size_inches(8,6)
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)

# --- Métricas ---
def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true).ravel()
    y_pred = np.asarray(y_pred).ravel()
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true).ravel()
    y_pred = np.asarray(y_pred).ravel()
    return float(np.mean(np.abs(y_true - y_pred)))

def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true).ravel()
    y_pred = np.asarray(y_pred).ravel()
    denom = np.where(y_true == 0, 1e-8, np.abs(y_true))
    return float(np.mean(np.abs((y_true - y_pred) / denom)) * 100.0)
