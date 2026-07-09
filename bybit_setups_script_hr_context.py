'''
Versão Hostinger ajustada a partir do bybit_setups_script_lv.py.
Mantém a execução no VPS, geração do ativos_opt_hr.xlsx e upload para Google Drive,
incorporando SL/TP por ATR, parâmetros otimizados/cache JSON e cálculo de swings.
'''
# === IMPORTAÇÕES NECESSÁRIAS ===
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import requests
from pybit.unified_trading import HTTP
import logging
import mplfinance as mpf
from openpyxl import Workbook
from openpyxl.drawing.image import Image as OpenpyxlImage
from openpyxl.utils.dataframe import dataframe_to_rows
from PIL import Image
import io
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from io import BytesIO

# Optimizer imceremnta SL e TP otimizado com base no histórico
import json, os
import time
import argparse
from dotenv import load_dotenv
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.client import OAuth2Credentials
from optimizer_atr_sl_tp import run_optimization_with_setups

from pathlib import Path
from dataclasses import dataclass, asdict, fields
from typing import Any, Dict, Iterable, List, Optional, Sequence

# Diretório base do script
BASE_DIR = Path(__file__).resolve().parent
# Pasta onde ficarão os JSONs de parâmetros
DIRETORIO_OPT = BASE_DIR / "opt_params"
DIRETORIO_OPT.mkdir(parents=True, exist_ok=True)
REOTIMIZAR_APOS_DIAS = 7

def normalize_timeframe(tf) -> str:
    """Converte 15, '15', 15.0 -> '15' (string sem decimais)."""
    try:
        return str(int(float(tf)))
    except Exception:
        return str(tf).strip()

# Define o fuso de Brasília (UTC-3)
fuso_brasilia = timezone(timedelta(hours=-3))

# Para usar o horário atual de Brasília:
agora_brasilia = datetime.now(fuso_brasilia)
#print("[INFO] Execução iniciada em horário de Brasília:", agora_brasilia.strftime("%Y-%m-%d %H:%M:%S"))

# Desativa logs de DEBUG do matplotlib e mplfinance
logging.getLogger('matplotlib').setLevel(logging.WARNING)
logging.getLogger('PIL').setLevel(logging.WARNING)
matplotlib.set_loglevel('warning')  # Apenas se sua versão do matplotlib suportar

# === CONFIGURAÇÕES INICIAIS ===
PERIODOS_TENDENCIA = 10  # Número de candles para confirmar tendência predominante (usado no 9.1)
PERIODOS_SEQUENCIA_TENDENCIA = 10  # Número de candles para confirmar tendência predominante (usado no 9.1)
PERIODOS_TENDENCIA_SUAVE = 6  # Número de candles consecutivos para confirmar sequência de tendência (9.2, 9.3, 9.4, PC)
LOOKBACK_PAVIO_CORPO = 10 #Número de candles para calcular a relação Pavio/Corpo
SLOPE_MME9_PERIODOS = 10
PASSO_TENDENCIA_SUAVE = 2  # Intervalo usado para suavizar a comparação entre médias (ex: compara -9 com -11)
PERIODOS_MINIMO = 30 #Número mínimo de períodos para considerar análise do ativo 
CASAS_DECIMAIS_GATILHO = 7  # Número de casas decimais para exibir os gatilhos
ENVIAR_ALERTA_TELEGRAM = False  # Enviar alertas automáticos via Telegram
DEBUG_MODE = False  # Para exibir mensagens detalhadas no futuro (opcional)
GERAR_GRAFICOS = False   # True = gera gráficos | False = desativa gráficos

# Parâmetros de Horário de Execução
USAR_HORARIO_LOCAL = True
HORA_INICIO = 0
HORA_FIM = 23
INTERVALO_MINUTOS = 1
PERMITIR_FIM_DE_SEMANA = True

# === PARÂMETROS SL/TP por ATR ===
ATR_PERIODO_SLTP = 14
K_SL_PADRAO = 1.5
K_TP_PADRAO = 2.5

# ===   PARÂMETROS SWING ===
SWING_LOOKBACK_9_1 = PERIODOS_SEQUENCIA_TENDENCIA
SWING_LOOKBACK_SUAVE = PERIODOS_TENDENCIA_SUAVE + 4

"""
Este script identifica os setups 9.1 a 9.4 (Larry Williams) e PC (Ponto Contínuo),
aplicando a lógica:
- Candle [-1]: ARMAR o gatilho
- Candle [0]: DISPARAR, se houver rompimento
- Setups 9.2, 9.3 e PC admitem escorregamento de gatilho
"""

# === CAMINHO DO ARQUIVO EXCEL COM ATIVOS ===
# >>> AJUSTE O CAMINHO CONFORME SEU COMPUTADOR <<<
ARQUIVO_EXCEL = "ativos.xlsx"

# Configuração central de logging
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

# Lista que armazenará dados de integridade dos candles
dados_integridade = []

# === FUNÇÕES AUXILIARES ===
# Função para checar tendência predominante (?)
def tendencia_predominante(mme, tipo='alta'):
    if tipo == 'alta':
        return mme.iloc[-PERIODOS_TENDENCIA - 2] < mme.iloc[-2]
    elif tipo == 'baixa':
        return mme.iloc[-PERIODOS_TENDENCIA - 2] > mme.iloc[-2]
    return False

# Função para checar sequência de tendência (para Setups 9.1)
def tendencia_sequencia(mme, tipo='alta'):
    if tipo == 'alta':
        return all(mme.iloc[i] < mme.iloc[i+1] for i in range(-PERIODOS_SEQUENCIA_TENDENCIA-1, -2))
    elif tipo == 'baixa':
        return all(mme.iloc[i] > mme.iloc[i+1] for i in range(-PERIODOS_SEQUENCIA_TENDENCIA-1, -2))
    return False

# === Função de tendência suavizada (para Setups 9.2, 9.3, 9.4, PC) ===
def tendencia_suave(mme, tipo='alta', passo=2, periodo=PERIODOS_TENDENCIA_SUAVE):
    """
    Verifica tendência suavizada com comparação espaçada (ex: [-9] > [-11], etc.)
    Ideal para setups sensíveis a microcorreções (como 9.2, 9.3, 9.4 e PC).
    """
    if len(mme) < periodo + passo:
        return False

    for i in range(-periodo - passo + 1, -passo + 2):
        if tipo == 'alta' and not (mme.iloc[i] > mme.iloc[i - passo]):
            return False
        if tipo == 'baixa' and not (mme.iloc[i] < mme.iloc[i - passo]):
            return False

    return True

def compute_atr(df, period=14, method="wilder"):
    """ATR clássico (Wilder por padrão) sobre colunas high/low/close."""
    h, l, c = df['high'], df['low'], df['close']
    prev_c = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    if method == "wilder":
        atr = tr.ewm(alpha=1/period, adjust=False).mean()
    elif method == "ema":
        atr = tr.ewm(span=period, adjust=False).mean()
    else:
        atr = tr.rolling(period, min_periods=1).mean()
    return atr

def calcular_slope_mme9(df, periodos=SLOPE_MME9_PERIODOS):
    """
    Calcula a inclinação percentual da MME9 nos últimos `periodos` candles fechados.
    Remove o candle [0] em formação.
    """
    if df is None or df.empty:
        return None

    df_fechados = df.iloc[:-1].copy()

    if 'MME9' not in df_fechados.columns:
        df_fechados['MME9'] = df_fechados['close'].ewm(span=9).mean()

    if len(df_fechados) < periodos + 1:
        return None

    mme_inicio = df_fechados['MME9'].iloc[-periodos]
    mme_fim = df_fechados['MME9'].iloc[-1]

    if mme_inicio == 0 or pd.isna(mme_inicio) or pd.isna(mme_fim):
        return None

    return round(((mme_fim - mme_inicio) / mme_inicio) * 100, 4)

def calcular_razao_pavio_corpo(df, lookback=LOOKBACK_PAVIO_CORPO):
    """
    Calcula a razão média pavio/corpo dos últimos `lookback` candles fechados.
    
    corpo = abs(close - open)
    pavio_total = (high - low) - corpo
    razão = média(pavio_total / corpo)
    """
    if df is None or df.empty:
        return None

    df_fechados = df.iloc[:-1].copy()

    if len(df_fechados) < lookback:
        return None

    trecho = df_fechados.iloc[-lookback:].copy()

    corpo = (trecho['close'] - trecho['open']).abs()
    range_total = trecho['high'] - trecho['low']
    pavio_total = range_total - corpo

    corpo = corpo.replace(0, pd.NA)

    razao = pavio_total / corpo
    razao = razao.dropna()

    if razao.empty:
        return None

    return round(razao.mean(), 4)

def obter_intervalo_fixo_swing(df, lookback):
    """
    Retorna intervalo [idx_inicio, idx_fim] para swing por janela fixa.
    Usa apenas candles fechados, portanto idx_fim = len(df_fechados)-1.
    """
    if df is None or df.empty:
        return None, None

    df_fechados = df.iloc[:-1].copy()
    if len(df_fechados) < lookback:
        return None, None

    idx_fim = len(df_fechados) - 1
    idx_inicio = max(0, len(df_fechados) - lookback)

    return idx_inicio, idx_fim


def obter_intervalo_estrutural_compra(df):
    """
    Para setups com escorregamento em COMPRA:
    volta desde o último candle fechado enquanto as máximas seguem descendentes.
    Usa o candle anterior ao início da sequência como início do swing, se existir.
    """
    if df is None or df.empty:
        return None, None

    df_fechados = df.iloc[:-1].copy()
    n = len(df_fechados)

    if n < 3:
        return None, None

    idx_fim = n - 1
    idx = idx_fim

    while idx - 1 >= 0:
        if df_fechados.iloc[idx - 1]['high'] > df_fechados.iloc[idx]['high']:
            idx -= 1
        else:
            break

    idx_inicio_correcao = idx
    idx_inicio_swing = max(0, idx_inicio_correcao - 1)

    return idx_inicio_swing, idx_fim


def obter_intervalo_estrutural_venda(df):
    """
    Para setups com escorregamento em VENDA:
    volta desde o último candle fechado enquanto as mínimas seguem ascendentes.
    Usa o candle anterior ao início da sequência como início do swing, se existir.
    """
    if df is None or df.empty:
        return None, None

    df_fechados = df.iloc[:-1].copy()
    n = len(df_fechados)

    if n < 3:
        return None, None

    idx_fim = n - 1
    idx = idx_fim

    while idx - 1 >= 0:
        if df_fechados.iloc[idx - 1]['low'] < df_fechados.iloc[idx]['low']:
            idx -= 1
        else:
            break

    idx_inicio_correcao = idx
    idx_inicio_swing = max(0, idx_inicio_correcao - 1)

    return idx_inicio_swing, idx_fim


def obter_intervalo_swing_por_setup(df, nome_setup, direcao):
    """
    Define o intervalo do swing conforme o setup e a direção.
    """
    nome_setup = str(nome_setup).strip().upper()
    direcao = str(direcao).strip().upper()

    # Setups sem escorregamento
    if nome_setup == '9.1':
        return obter_intervalo_fixo_swing(df, PERIODOS_SEQUENCIA_TENDENCIA)

    if nome_setup == '9.4':
        return obter_intervalo_fixo_swing(df, PERIODOS_TENDENCIA_SUAVE + 3)

    # Setups com escorregamento
    if nome_setup in ['9.2', '9.3', 'PC']:
        if direcao == 'COMPRA':
            return obter_intervalo_estrutural_compra(df)
        elif direcao == 'VENDA':
            return obter_intervalo_estrutural_venda(df)

    return None, None


def calcular_swing_absoluto_intervalo(df, idx_inicio, idx_fim, direcao):
    """
    Calcula swing absoluto dentro de um intervalo [idx_inicio, idx_fim]
    usando apenas candles na direção.
    """
    if df is None or df.empty:
        return None

    df_fechados = df.iloc[:-1].copy()

    if idx_inicio is None or idx_fim is None:
        return None

    if idx_inicio < 0 or idx_fim >= len(df_fechados) or idx_inicio > idx_fim:
        return None

    trecho = df_fechados.iloc[idx_inicio:idx_fim + 1].copy()
    direcao = str(direcao).strip().upper()

    if direcao == 'COMPRA':
        trecho_dir = trecho[trecho['close'] > trecho['open']].copy()
        if trecho_dir.empty:
            return None

        menor_open = trecho_dir['open'].min()
        maior_close = trecho_dir['close'].max()

        if pd.isna(menor_open) or pd.isna(maior_close):
            return None

        return round(maior_close - menor_open, 8)

    elif direcao == 'VENDA':
        trecho_dir = trecho[trecho['close'] < trecho['open']].copy()
        if trecho_dir.empty:
            return None

        maior_open = trecho_dir['open'].max()
        menor_close = trecho_dir['close'].min()

        if pd.isna(maior_open) or pd.isna(menor_close):
            return None

        return round(maior_open - menor_close, 8)

    return None


def calcular_swing_percentual_intervalo(df, idx_inicio, idx_fim, direcao):
    """
    Calcula swing percentual dentro de um intervalo [idx_inicio, idx_fim]
    usando apenas candles na direção.
    """
    if df is None or df.empty:
        return None

    df_fechados = df.iloc[:-1].copy()

    if idx_inicio is None or idx_fim is None:
        return None

    if idx_inicio < 0 or idx_fim >= len(df_fechados) or idx_inicio > idx_fim:
        return None

    trecho = df_fechados.iloc[idx_inicio:idx_fim + 1].copy()
    direcao = str(direcao).strip().upper()

    if direcao == 'COMPRA':
        trecho_dir = trecho[trecho['close'] > trecho['open']].copy()
        if trecho_dir.empty:
            return None

        menor_open = trecho_dir['open'].min()
        maior_close = trecho_dir['close'].max()

        if pd.isna(menor_open) or pd.isna(maior_close) or menor_open == 0:
            return None

        swing_abs = maior_close - menor_open
        return round((swing_abs / menor_open) * 100, 4)

    elif direcao == 'VENDA':
        trecho_dir = trecho[trecho['close'] < trecho['open']].copy()
        if trecho_dir.empty:
            return None

        maior_open = trecho_dir['open'].max()
        menor_close = trecho_dir['close'].min()

        if pd.isna(maior_open) or pd.isna(menor_close) or maior_open == 0:
            return None

        swing_abs = maior_open - menor_close
        return round((swing_abs / maior_open) * 100, 4)

    return None


# =============================================================================
# === CONTEXTO DE MERCADO BYBIT: UNIVERSO, LIQUIDEZ, REGIME, FORÇA E SCORE =====
# =============================================================================

BYBIT_MAINNET = "https://api.bybit.com"

CONTEXT_OUTPUT_COLUMNS = [
    "REGIME", "SCORE_TOTAL", "APROVADO_SCORE", "BLOQUEIO_MOTIVO",
    "SCORE_LIQUIDEZ", "SCORE_REGIME", "SCORE_FORCA", "SCORE_FUNDING_OI", "SCORE_SETUP_BASE",
    "RANK_FORCA", "RET_3C", "RET_6C", "RET_12C",
    "TURNOVER24H", "VOLUME24H", "SPREAD_PCT",
    "DEPTH_BID_1PCT", "DEPTH_ASK_1PCT", "DEPTH_1PCT", "BOOK_IMBALANCE_1PCT", "SLIPPAGE_EST_PCT",
    "FUNDING_RATE", "FUNDING_ZSCORE", "NEXT_FUNDING_TIME",
    "OPEN_INTEREST", "OPEN_INTEREST_VALUE", "OI_CHANGE_1", "OI_CHANGE_3", "OI_CHANGE_6", "OI_CHANGE_24H",
    "MARK_PRICE", "INDEX_PRICE", "BASIS_MARK_INDEX", "BASIS_LAST_MARK",
    "LONG_RATIO", "SHORT_RATIO", "LONG_SHORT_RATIO",
    "ATR_PCT", "ATR_PERCENTIL", "ADX", "CHOP", "EFFICIENCY_RATIO", "BB_WIDTH_PERCENTIL",
    "SLOPE_MME9_ATR_5", "DIST_MME9_ATR", "DIST_MMA21_ATR",
    "BODY_RANGE_RATIO", "RANGE_ATR", "CLOSE_POSITION",
    "GRID_ESTIMADO", "TP_ESTIMADO_PCT", "SL_ESTIMADO_PCT"
]

UNIVERSO_BYBIT_DF = pd.DataFrame()
FORCA_RELATIVA_DF = pd.DataFrame()
CONFIG_CONTEXT = None


def _to_float(value, default=np.nan):
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str):
            value = value.strip().replace(" ", "")
            if value.endswith("%"):
                value = value[:-1].replace(".", "").replace(",", ".")
                return float(value) / 100.0
            value = value.replace(",", ".")
        return float(value)
    except Exception:
        return default


def _to_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", ".")))
    except Exception:
        return default


def _safe_div(num, den, default=np.nan):
    try:
        if den is None or float(den) == 0 or pd.isna(den):
            return default
        return float(num) / float(den)
    except Exception:
        return default


def _ms_now():
    return int(time.time() * 1000)


def _days_since_ms(ms):
    ts = _to_int(ms, 0)
    if ts <= 0:
        return None
    return (_ms_now() - ts) / (1000 * 60 * 60 * 24)


def _percentile_rank(arr):
    s = pd.Series(arr).dropna()
    if s.empty:
        return np.nan
    return float((s <= s.iloc[-1]).mean() * 100)


def _config_parse_bool(value, default=False):
    if isinstance(value, bool):
        return value
    txt = str(value).strip().upper()
    if txt in {"SIM", "TRUE", "VERDADEIRO", "1", "YES", "Y"}:
        return True
    if txt in {"NAO", "NÃO", "FALSE", "FALSO", "0", "NO", "N"}:
        return False
    return default


@dataclass
class MarketContextConfig:
    # Universo
    modo_universo: str = "HIBRIDO"       # AUTO | HIBRIDO | MANUAL
    category: str = "linear"
    quote_coin: str = "USDT"
    settle_coin: str = "USDT"
    contract_type: str = "LinearPerpetual"
    min_age_days: int = 90
    default_timeframes: str = "240"
    kline_limit: int = 200

    # Liquidez / execução
    min_turnover24h_usdt: float = 10_000_000
    max_spread_pct: float = 0.0005       # 0,05%
    min_open_interest_value: float = 1_000_000
    orderbook_limit: int = 200
    min_depth_1pct_usdt: float = 250_000
    max_slippage_pct: float = 0.003      # 0,30%

    # Captura profunda
    capturar_orderbook: bool = True
    capturar_oi_historico: bool = True
    capturar_funding_historico: bool = True
    capturar_long_short: bool = True
    api_sleep_s: float = 0.08

    # Funding/OI
    max_abs_funding_rate: float = 0.003
    max_funding_zscore: float = 2.5
    oi_interval: str = "4h"
    account_ratio_period: str = "4h"

    # Regime técnico
    adx_period: int = 14
    atr_period: int = 14
    chop_period: int = 14
    er_period: int = 10
    bb_period: int = 20
    bb_std: float = 2.0
    percentile_lookback: int = 90
    adx_min_trend: float = 18.0
    chop_max_trend: float = 55.0
    er_min_trend: float = 0.25
    atr_percentile_min: float = 25.0
    atr_percentile_max: float = 85.0
    bb_width_percentile_compression: float = 20.0

    # Força relativa
    rs_short_window: int = 3
    rs_mid_window: int = 6
    rs_long_window: int = 12
    rs_min_long: float = 65.0
    rs_max_short: float = 35.0

    # Score / Grid
    score_min_setup: float = 70.0
    grid_spacing_pct: float = 0.005      # 0,50%


class BybitPublicV5:
    """Cliente REST público V5. Usa apenas endpoints públicos de Market Data."""

    def __init__(self, base_url=BYBIT_MAINNET, timeout=15, sleep_s=0.08):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.sleep_s = sleep_s
        self.session = requests.Session()

    def get(self, path, params=None, retries=3):
        url = f"{self.base_url}{path}"
        params = {k: v for k, v in (params or {}).items() if v is not None}
        last_error = None
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()
                if data.get("retCode") not in (0, "0", None):
                    raise RuntimeError(f"Bybit retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
                if self.sleep_s:
                    time.sleep(self.sleep_s)
                return data.get("result", {}) or {}
            except Exception as exc:
                last_error = exc
                time.sleep((attempt + 1) * 0.5)
        raise RuntimeError(f"Falha ao consultar {path} params={params}: {last_error}")

    def get_paginated(self, path, params, list_key="list"):
        all_rows = []
        cursor = None
        while True:
            q = dict(params)
            if cursor:
                q["cursor"] = cursor
            result = self.get(path, q)
            rows = result.get(list_key, []) or []
            all_rows.extend(rows)
            cursor = result.get("nextPageCursor")
            if not cursor:
                break
        return all_rows

    def instruments_info(self, category="linear"):
        return self.get_paginated("/v5/market/instruments-info", {"category": category, "limit": 1000})

    def tickers(self, category="linear", symbol=None):
        result = self.get("/v5/market/tickers", {"category": category, "symbol": symbol})
        return result.get("list", []) or []

    def orderbook(self, symbol, category="linear", limit=200):
        return self.get("/v5/market/orderbook", {"category": category, "symbol": symbol, "limit": limit})

    def open_interest(self, symbol, category="linear", interval="4h", limit=50):
        result = self.get(
            "/v5/market/open-interest",
            {"category": category, "symbol": symbol, "intervalTime": interval, "limit": limit},
        )
        return result.get("list", []) or []

    def funding_history(self, symbol, category="linear", limit=200):
        result = self.get("/v5/market/funding/history", {"category": category, "symbol": symbol, "limit": limit})
        return result.get("list", []) or []

    def long_short_ratio(self, symbol, category="linear", period="4h", limit=50):
        result = self.get("/v5/market/account-ratio", {"category": category, "symbol": symbol, "period": period, "limit": limit})
        return result.get("list", []) or []


def _set_cfg_attr(cfg, key, value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return
    key = str(key).strip()
    if not key:
        return

    aliases = {
        "MODO_UNIVERSO": "modo_universo",
        "CATEGORIA": "category",
        "QUOTE_COIN": "quote_coin",
        "SETTLE_COIN": "settle_coin",
        "CONTRACT_TYPE": "contract_type",
        "IDADE_MIN_DIAS": "min_age_days",
        "TIMEFRAMES_PADRAO": "default_timeframes",
        "KLINE_LIMIT": "kline_limit",
        "TURNOVER24H_MIN": "min_turnover24h_usdt",
        "SPREAD_MAX_PCT": "max_spread_pct",
        "OI_VALUE_MIN": "min_open_interest_value",
        "DEPTH_1PCT_MIN": "min_depth_1pct_usdt",
        "SLIPPAGE_MAX_PCT": "max_slippage_pct",
        "FUNDING_ABS_MAX": "max_abs_funding_rate",
        "FUNDING_ZSCORE_MAX": "max_funding_zscore",
        "ADX_MIN_TENDENCIA": "adx_min_trend",
        "CHOP_MAX_TENDENCIA": "chop_max_trend",
        "EFFICIENCY_RATIO_MIN": "er_min_trend",
        "ATR_PERCENTIL_MIN": "atr_percentile_min",
        "ATR_PERCENTIL_MAX": "atr_percentile_max",
        "BB_WIDTH_PERCENTIL_COMPRESSAO": "bb_width_percentile_compression",
        "RANK_MIN_LONG": "rs_min_long",
        "RANK_MAX_SHORT": "rs_max_short",
        "SCORE_MIN_SETUP": "score_min_setup",
        "GRID_SPACING_PCT": "grid_spacing_pct",
        "CAPTURAR_ORDERBOOK": "capturar_orderbook",
        "CAPTURAR_OI_HISTORICO": "capturar_oi_historico",
        "CAPTURAR_FUNDING_HISTORICO": "capturar_funding_historico",
        "CAPTURAR_LONG_SHORT": "capturar_long_short",
    }
    attr = aliases.get(key.upper(), key)
    valid = {f.name: f.type for f in fields(cfg)}
    if attr not in valid:
        return

    current = getattr(cfg, attr)
    if isinstance(current, bool):
        parsed = _config_parse_bool(value, default=current)
    elif isinstance(current, int) and not isinstance(current, bool):
        parsed = _to_int(value, current)
    elif isinstance(current, float):
        parsed = _to_float(value, current)
    else:
        parsed = str(value).strip()
    setattr(cfg, attr, parsed)


def ler_config_contexto(arquivo_excel=ARQUIVO_EXCEL):
    cfg = MarketContextConfig()
    try:
        xf = pd.ExcelFile(arquivo_excel)
        for sheet_name in ["CONFIG_UNIVERSO", "CONFIG_REGIME", "CONFIG_FORCA_RELATIVA", "CONFIG_FUNDING_OI", "CONFIG_GRID"]:
            if sheet_name not in xf.sheet_names:
                continue
            df_cfg = pd.read_excel(arquivo_excel, sheet_name=sheet_name)
            if df_cfg.empty:
                continue
            cols = {str(c).strip().lower(): c for c in df_cfg.columns}
            pcol = cols.get("parametro") or cols.get("parâmetro") or cols.get("chave") or df_cfg.columns[0]
            vcol = cols.get("valor") or cols.get("value") or (df_cfg.columns[1] if len(df_cfg.columns) > 1 else None)
            if vcol is None:
                continue
            for _, row in df_cfg.iterrows():
                _set_cfg_attr(cfg, row.get(pcol), row.get(vcol))
    except Exception as exc:
        logging.info(f"[CTX] Configuração de contexto não encontrada ou inválida em {arquivo_excel}. Usando padrões. Detalhe: {exc}")
    return cfg


def context_config_to_df(cfg):
    rows = []
    for k, v in asdict(cfg).items():
        rows.append({"Parametro": k, "Valor": v})
    return pd.DataFrame(rows)


def _split_timeframes(txt, fallback=("240",)):
    if txt is None or (isinstance(txt, float) and pd.isna(txt)):
        return list(fallback)
    if isinstance(txt, (int, float)):
        return [normalize_timeframe(txt)]
    parts = [p.strip() for p in str(txt).replace(";", ",").split(",") if p.strip()]
    return [normalize_timeframe(p) for p in parts] or list(fallback)


def ler_excecoes_ativos(arquivo_excel=ARQUIVO_EXCEL):
    forced_include, forced_exclude = set(), set()
    timeframe_override = {}
    mercado_override = {}
    try:
        xf = pd.ExcelFile(arquivo_excel)
        if "EXCECOES_ATIVOS" not in xf.sheet_names:
            return forced_include, forced_exclude, timeframe_override, mercado_override
        df = pd.read_excel(arquivo_excel, sheet_name="EXCECOES_ATIVOS")
        if df.empty or "Par" not in df.columns:
            return forced_include, forced_exclude, timeframe_override, mercado_override
        for _, row in df.iterrows():
            par = str(row.get("Par", "")).upper().strip()
            if not par or par == "NAN":
                continue
            acao = str(row.get("Acao", row.get("Ação", "AUTO"))).upper().strip()
            if acao in {"FORCAR_INCLUSAO", "INCLUIR", "INCLUDE", "SIM"}:
                forced_include.add(par)
            elif acao in {"EXCLUIR", "FORCAR_EXCLUSAO", "EXCLUDE", "NAO", "NÃO"}:
                forced_exclude.add(par)
            tf = row.get("Timeframes", row.get("Timeframe", None))
            if tf is not None and not (isinstance(tf, float) and pd.isna(tf)):
                timeframe_override[par] = _split_timeframes(tf)
            mercado = row.get("Mercado", None)
            if mercado is not None and not (isinstance(mercado, float) and pd.isna(mercado)):
                mercado_override[par] = str(mercado).lower().strip()
    except Exception as exc:
        logging.info(f"[CTX] Sem exceções de ativos: {exc}")
    return forced_include, forced_exclude, timeframe_override, mercado_override


def build_auto_universe_context(client, cfg, forced_include=None, forced_exclude=None):
    forced_include = {str(s).upper().strip() for s in (forced_include or []) if s}
    forced_exclude = {str(s).upper().strip() for s in (forced_exclude or []) if s}

    instruments = client.instruments_info(cfg.category)
    tickers = client.tickers(cfg.category)
    tmap = {t.get("symbol"): t for t in tickers}

    rows = []
    for inst in instruments:
        symbol = inst.get("symbol")
        if not symbol or symbol in forced_exclude:
            continue

        ticker = tmap.get(symbol, {})
        lot = inst.get("lotSizeFilter", {}) or {}
        price_filter = inst.get("priceFilter", {}) or {}
        leverage_filter = inst.get("leverageFilter", {}) or {}

        age_days = _days_since_ms(inst.get("launchTime"))
        last = _to_float(ticker.get("lastPrice"))
        bid = _to_float(ticker.get("bid1Price"))
        ask = _to_float(ticker.get("ask1Price"))
        spread_pct = _safe_div(ask - bid, last) if last and bid and ask else np.nan
        turnover24h = _to_float(ticker.get("turnover24h"))
        oi_value = _to_float(ticker.get("openInterestValue"))
        funding_rate = _to_float(ticker.get("fundingRate"))
        is_forced = symbol in forced_include

        structural_ok = (
            inst.get("status") == "Trading"
            and inst.get("quoteCoin") == cfg.quote_coin
            and inst.get("settleCoin") == cfg.settle_coin
            and inst.get("contractType") == cfg.contract_type
            and not bool(inst.get("isPreListing", False))
            and (age_days is None or age_days >= cfg.min_age_days)
        )
        liquidity_ok = (
            turnover24h >= cfg.min_turnover24h_usdt
            and (np.isnan(spread_pct) or spread_pct <= cfg.max_spread_pct)
            and (np.isnan(oi_value) or oi_value >= cfg.min_open_interest_value)
            and (np.isnan(funding_rate) or abs(funding_rate) <= cfg.max_abs_funding_rate)
        )

        eligible = bool(is_forced or (structural_ok and liquidity_ok))
        reasons = []
        if inst.get("status") != "Trading": reasons.append("status_not_trading")
        if inst.get("quoteCoin") != cfg.quote_coin: reasons.append("quote_not_usdt")
        if inst.get("settleCoin") != cfg.settle_coin: reasons.append("settle_not_usdt")
        if inst.get("contractType") != cfg.contract_type: reasons.append("not_linear_perp")
        if bool(inst.get("isPreListing", False)): reasons.append("pre_listing")
        if age_days is not None and age_days < cfg.min_age_days: reasons.append("young_asset")
        if turnover24h < cfg.min_turnover24h_usdt: reasons.append("low_turnover24h")
        if not np.isnan(spread_pct) and spread_pct > cfg.max_spread_pct: reasons.append("high_spread")
        if not np.isnan(oi_value) and oi_value < cfg.min_open_interest_value: reasons.append("low_oi")
        if not np.isnan(funding_rate) and abs(funding_rate) > cfg.max_abs_funding_rate: reasons.append("funding_extreme")
        if is_forced: reasons.append("forced_include")

        rows.append({
            "Par": symbol,
            "Status": inst.get("status"),
            "ContractType": inst.get("contractType"),
            "QuoteCoin": inst.get("quoteCoin"),
            "SettleCoin": inst.get("settleCoin"),
            "LaunchTime": inst.get("launchTime"),
            "Idade_Dias": age_days,
            "TickSize": price_filter.get("tickSize"),
            "QtyStep": lot.get("qtyStep"),
            "MinOrderQty": lot.get("minOrderQty"),
            "MaxLeverage": leverage_filter.get("maxLeverage"),
            "FundingInterval": inst.get("fundingInterval"),
            "LastPrice": last,
            "Bid1": bid,
            "Ask1": ask,
            "Spread_Pct": spread_pct,
            "Turnover24h": turnover24h,
            "Volume24h": _to_float(ticker.get("volume24h")),
            "Price24hPcnt": _to_float(ticker.get("price24hPcnt")),
            "FundingRate": funding_rate,
            "OpenInterest": _to_float(ticker.get("openInterest")),
            "OpenInterestValue": oi_value,
            "MarkPrice": _to_float(ticker.get("markPrice")),
            "IndexPrice": _to_float(ticker.get("indexPrice")),
            "NextFundingTime": ticker.get("nextFundingTime"),
            "Estrutural_OK": structural_ok,
            "Liquidez_OK": liquidity_ok,
            "Elegivel_Universo": eligible,
            "Bloqueio_Universo": ";".join(reasons),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["Elegivel_Universo", "Turnover24h"], ascending=[False, False]).reset_index(drop=True)
    return df, tmap


def montar_universo_para_scan(arquivo_excel, manual_df, cfg, client):
    modo = str(cfg.modo_universo).upper().strip()
    if modo not in {"AUTO", "HIBRIDO", "MANUAL"}:
        modo = "HIBRIDO"

    forced_include, forced_exclude, tf_override, mercado_override = ler_excecoes_ativos(arquivo_excel)

    # Timeframes padrão: se não vierem na CONFIG, usa os que já existirem na planilha manual.
    manual_tfs = []
    if manual_df is not None and not manual_df.empty and "Timeframe" in manual_df.columns:
        manual_tfs = [normalize_timeframe(x) for x in manual_df["Timeframe"].dropna().unique().tolist()]
    default_tfs = _split_timeframes(cfg.default_timeframes, fallback=manual_tfs or ["240"])

    if modo == "MANUAL":
        out = manual_df.copy()
        if "Mercado" not in out.columns:
            out["Mercado"] = cfg.category
        logging.info(f"[CTX] Modo MANUAL: {len(out)} linhas vindas de {arquivo_excel}")
        return out, pd.DataFrame(), {}

    universo_df, ticker_map = build_auto_universe_context(client, cfg, forced_include, forced_exclude)
    elegiveis = universo_df[universo_df["Elegivel_Universo"] == True].copy()

    if elegiveis.empty:
        logging.warning("[CTX] Universo automático retornou vazio. Fallback para MANUAL.")
        out = manual_df.copy()
        if "Mercado" not in out.columns:
            out["Mercado"] = cfg.category
        return out, universo_df, ticker_map

    rows = []
    for _, row in elegiveis.iterrows():
        par = str(row["Par"]).upper()
        tfs = tf_override.get(par, default_tfs)
        mercado = mercado_override.get(par, cfg.category)
        for tf in tfs:
            base = {"Par": par, "Timeframe": normalize_timeframe(tf), "Mercado": mercado}
            # Leva contexto básico já capturado em universo/tickers.
            for k, v in row.items():
                if k not in base:
                    base[k] = v
            rows.append(base)

    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=["Par", "Timeframe", "Mercado"]).reset_index(drop=True)
    logging.info(f"[CTX] Modo {modo}: {len(out)} linhas para análise após filtros de universo.")
    return out, universo_df, ticker_map


def compute_adx_context(df, period=14):
    high, low = df["high"], df["low"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr = compute_atr(df, period=period, method="wilder")
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / period, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / period, adjust=False).mean() / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def compute_choppiness_context(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    tr_sum = tr.rolling(period).sum()
    highest = high.rolling(period).max()
    lowest = low.rolling(period).min()
    return 100 * np.log10(tr_sum / (highest - lowest).replace(0, np.nan)) / np.log10(period)


def compute_efficiency_ratio_context(df, period=10):
    direction = (df["close"] - df["close"].shift(period)).abs()
    volatility = df["close"].diff().abs().rolling(period).sum()
    return direction / volatility.replace(0, np.nan)


def enriquecer_candles_contexto(df, cfg):
    out = df.copy()
    if "MME9" not in out.columns:
        out["MME9"] = out["close"].ewm(span=9).mean()
    if "MMA21" not in out.columns:
        out["MMA21"] = out["close"].rolling(window=21).mean()

    out["ATR_CTX"] = compute_atr(out, period=cfg.atr_period, method="wilder")
    out["ATR_PCT"] = out["ATR_CTX"] / out["close"]
    out["ADX"] = compute_adx_context(out, cfg.adx_period)
    out["CHOP"] = compute_choppiness_context(out, cfg.chop_period)
    out["EFFICIENCY_RATIO"] = compute_efficiency_ratio_context(out, cfg.er_period)

    ma = out["close"].rolling(cfg.bb_period).mean()
    sd = out["close"].rolling(cfg.bb_period).std(ddof=0)
    out["BB_WIDTH"] = ((ma + cfg.bb_std * sd) - (ma - cfg.bb_std * sd)) / ma.replace(0, np.nan)

    lb = cfg.percentile_lookback
    out["ATR_PERCENTIL"] = out["ATR_PCT"].rolling(lb, min_periods=max(20, min(lb, 30))).apply(_percentile_rank, raw=False)
    out["BB_WIDTH_PERCENTIL"] = out["BB_WIDTH"].rolling(lb, min_periods=max(20, min(lb, 30))).apply(_percentile_rank, raw=False)

    out["SLOPE_MME9_PCT_5"] = out["MME9"].pct_change(5)
    out["SLOPE_MMA21_PCT_5"] = out["MMA21"].pct_change(5)
    out["SLOPE_MME9_ATR_5"] = (out["MME9"] - out["MME9"].shift(5)) / out["ATR_CTX"].replace(0, np.nan)
    out["DIST_MME9_ATR"] = (out["close"] - out["MME9"]) / out["ATR_CTX"].replace(0, np.nan)
    out["DIST_MMA21_ATR"] = (out["close"] - out["MMA21"]) / out["ATR_CTX"].replace(0, np.nan)

    rng = out["high"] - out["low"]
    body = (out["close"] - out["open"]).abs()
    out["BODY_RANGE_RATIO"] = body / rng.replace(0, np.nan)
    out["RANGE_ATR"] = rng / out["ATR_CTX"].replace(0, np.nan)
    out["CLOSE_POSITION"] = (out["close"] - out["low"]) / rng.replace(0, np.nan)

    trend = (
        (out["ADX"] >= cfg.adx_min_trend)
        & (out["CHOP"] <= cfg.chop_max_trend)
        & (out["EFFICIENCY_RATIO"] >= cfg.er_min_trend)
        & (out["ATR_PERCENTIL"].between(cfg.atr_percentile_min, cfg.atr_percentile_max, inclusive="both"))
    )
    compression = out["BB_WIDTH_PERCENTIL"] <= cfg.bb_width_percentile_compression
    out["REGIME"] = np.select([trend, compression], ["TENDENCIA", "COMPRESSAO"], default="NEUTRO_RANGE")
    return out


def calcular_forca_relativa(candles_dict, cfg):
    rows = []
    min_len = max(cfg.rs_long_window + 2, 20)
    for symbol, df in candles_dict.items():
        if df is None or df.empty or len(df) < min_len:
            continue
        fechados = df.iloc[:-1].copy() if len(df) > 1 else df.copy()
        if len(fechados) < min_len:
            continue
        close = pd.to_numeric(fechados["close"], errors="coerce")
        last = close.iloc[-1]
        rows.append({
            "Par": symbol,
            "RET_3C": _safe_div(last, close.iloc[-cfg.rs_short_window - 1]) - 1,
            "RET_6C": _safe_div(last, close.iloc[-cfg.rs_mid_window - 1]) - 1,
            "RET_12C": _safe_div(last, close.iloc[-cfg.rs_long_window - 1]) - 1,
        })
    rs = pd.DataFrame(rows)
    if rs.empty:
        return rs
    rs["RET_SCORE_RAW"] = rs[["RET_3C", "RET_6C", "RET_12C"]].mean(axis=1)
    rs["RANK_FORCA"] = rs["RET_SCORE_RAW"].rank(pct=True) * 100
    return rs.sort_values("RANK_FORCA", ascending=False).reset_index(drop=True)


def calculate_orderbook_metrics_context(book, last_price):
    bids = book.get("b", []) or []
    asks = book.get("a", []) or []
    if not bids or not asks or not last_price or np.isnan(last_price):
        return {}

    bid_cut = last_price * 0.99
    ask_cut = last_price * 1.01
    depth_bid = 0.0
    depth_ask = 0.0
    for price_s, qty_s in bids:
        price = _to_float(price_s)
        qty = _to_float(qty_s)
        if price >= bid_cut:
            depth_bid += price * qty
    for price_s, qty_s in asks:
        price = _to_float(price_s)
        qty = _to_float(qty_s)
        if price <= ask_cut:
            depth_ask += price * qty

    depth_min = min(depth_bid, depth_ask)
    imbalance = _safe_div(depth_bid - depth_ask, depth_bid + depth_ask)
    notional_ref = 10_000.0
    slippage_est = min(0.02, _safe_div(notional_ref, depth_min, default=np.nan) * 0.01) if depth_min else np.nan
    return {
        "DepthBid1Pct": depth_bid,
        "DepthAsk1Pct": depth_ask,
        "DepthMin1Pct": depth_min,
        "BookImbalance1Pct": imbalance,
        "SlippageEstPct": slippage_est,
    }


def capturar_contexto_profundo(client, symbol, cfg, ticker_row=None):
    ctx = {}
    if ticker_row is None:
        ticks = client.tickers(cfg.category, symbol=symbol)
        ticker_row = ticks[0] if ticks else {}

    last = _to_float(ticker_row.get("lastPrice"))
    bid = _to_float(ticker_row.get("bid1Price"))
    ask = _to_float(ticker_row.get("ask1Price"))
    mark = _to_float(ticker_row.get("markPrice"))
    index = _to_float(ticker_row.get("indexPrice"))

    ctx.update({
        "LastPrice": last,
        "Bid1": bid,
        "Ask1": ask,
        "Spread_Pct": _safe_div(ask - bid, last) if last and bid and ask else np.nan,
        "Turnover24h": _to_float(ticker_row.get("turnover24h")),
        "Volume24h": _to_float(ticker_row.get("volume24h")),
        "Price24hPcnt": _to_float(ticker_row.get("price24hPcnt")),
        "FundingRate": _to_float(ticker_row.get("fundingRate")),
        "NextFundingTime": ticker_row.get("nextFundingTime"),
        "OpenInterest": _to_float(ticker_row.get("openInterest")),
        "OpenInterestValue": _to_float(ticker_row.get("openInterestValue")),
        "MarkPrice": mark,
        "IndexPrice": index,
        "BasisMarkIndex": _safe_div(mark - index, index) if mark and index else np.nan,
        "BasisLastMark": _safe_div(last - mark, mark) if last and mark else np.nan,
    })

    if cfg.capturar_orderbook:
        try:
            ctx.update(calculate_orderbook_metrics_context(client.orderbook(symbol, cfg.category, cfg.orderbook_limit), last))
        except Exception as exc:
            ctx["OrderbookErro"] = str(exc)

    if cfg.capturar_oi_historico:
        try:
            oi_rows = client.open_interest(symbol, cfg.category, cfg.oi_interval, limit=50)
            oi_sorted = sorted(oi_rows, key=lambda x: _to_int(x.get("timestamp"), 0))
            vals = [_to_float(x.get("openInterest")) for x in oi_sorted]
            if len(vals) >= 2:
                ctx["OI_Change_1"] = _safe_div(vals[-1], vals[-2]) - 1
                ctx["OI_Change_3"] = _safe_div(vals[-1], vals[-4]) - 1 if len(vals) >= 4 else np.nan
                ctx["OI_Change_6"] = _safe_div(vals[-1], vals[-7]) - 1 if len(vals) >= 7 else np.nan
                ctx["OI_Change_24h"] = _safe_div(vals[-1], vals[-7]) - 1 if cfg.oi_interval == "4h" and len(vals) >= 7 else np.nan
        except Exception as exc:
            ctx["OIErro"] = str(exc)

    if cfg.capturar_funding_historico:
        try:
            fund_rows = client.funding_history(symbol, cfg.category, limit=200)
            fund_vals = pd.Series([_to_float(x.get("fundingRate")) for x in fund_rows], dtype="float64").dropna()
            if not fund_vals.empty:
                ctx["Funding_Mean_21"] = fund_vals.head(21).mean()
                ctx["Funding_Mean_90"] = fund_vals.head(90).mean()
                ctx["Funding_Std_90"] = fund_vals.head(90).std(ddof=0)
                ctx["Funding_ZScore"] = _safe_div(ctx.get("FundingRate", np.nan) - ctx["Funding_Mean_90"], ctx["Funding_Std_90"])
        except Exception as exc:
            ctx["FundingErro"] = str(exc)

    if cfg.capturar_long_short:
        try:
            ls_rows = client.long_short_ratio(symbol, cfg.category, cfg.account_ratio_period, limit=50)
            latest = sorted(ls_rows, key=lambda x: _to_int(x.get("timestamp"), 0))[-1] if ls_rows else None
            if latest:
                buy_ratio = _to_float(latest.get("buyRatio"))
                sell_ratio = _to_float(latest.get("sellRatio"))
                ctx["LongRatio"] = buy_ratio
                ctx["ShortRatio"] = sell_ratio
                ctx["LongShortRatio"] = _safe_div(buy_ratio, sell_ratio)
        except Exception as exc:
            ctx["LongShortErro"] = str(exc)
    return ctx


def _score_between(value, lo, hi):
    if value is None or pd.isna(value):
        return 50.0
    if lo <= value <= hi:
        return 100.0
    if value < lo:
        dist = _safe_div(lo - value, abs(lo) if lo else 1, default=1)
    else:
        dist = _safe_div(value - hi, abs(hi) if hi else 1, default=1)
    return max(0.0, 100.0 - 100.0 * min(1.0, dist))


def score_setup_candidate_context(side, setup, last_row, context, relative_strength_row, cfg):
    side = str(side).upper().strip()
    setup = str(setup).upper().strip()
    is_long = side in {"COMPRA", "LONG", "BUY"}

    bloqueios = []

    turnover = _to_float(context.get("Turnover24h"))
    spread = _to_float(context.get("Spread_Pct"))
    depth = _to_float(context.get("DepthMin1Pct"))
    slippage = _to_float(context.get("SlippageEstPct"))

    score_liq = np.nanmean([
        100.0 if turnover >= cfg.min_turnover24h_usdt else max(0, turnover / cfg.min_turnover24h_usdt * 100),
        100.0 if pd.isna(spread) or spread <= cfg.max_spread_pct else max(0, 100 - (spread / cfg.max_spread_pct - 1) * 100),
        100.0 if pd.isna(depth) or depth >= cfg.min_depth_1pct_usdt else max(0, depth / cfg.min_depth_1pct_usdt * 100),
        100.0 if pd.isna(slippage) or slippage <= cfg.max_slippage_pct else max(0, 100 - (slippage / cfg.max_slippage_pct - 1) * 100),
    ])
    if turnover < cfg.min_turnover24h_usdt: bloqueios.append("turnover24h_baixo")
    if not pd.isna(spread) and spread > cfg.max_spread_pct: bloqueios.append("spread_alto")
    if not pd.isna(depth) and depth < cfg.min_depth_1pct_usdt: bloqueios.append("depth_baixo")

    adx = _to_float(last_row.get("ADX"))
    chop = _to_float(last_row.get("CHOP"))
    er = _to_float(last_row.get("EFFICIENCY_RATIO"))
    atr_pctile = _to_float(last_row.get("ATR_PERCENTIL"))
    regime = str(last_row.get("REGIME", ""))

    score_regime = np.nanmean([
        min(100, adx / cfg.adx_min_trend * 100) if not pd.isna(adx) else 50,
        100 if pd.isna(chop) or chop <= cfg.chop_max_trend else max(0, 100 - (chop - cfg.chop_max_trend) * 3),
        min(100, er / cfg.er_min_trend * 100) if not pd.isna(er) else 50,
        _score_between(atr_pctile, cfg.atr_percentile_min, cfg.atr_percentile_max),
    ])
    if setup in {"9.2", "9.3", "9.4", "PC"} and regime != "TENDENCIA":
        bloqueios.append(f"regime_{regime.lower()}_nao_ideal")

    rank_forca = _to_float(relative_strength_row.get("RANK_FORCA")) if relative_strength_row is not None else np.nan
    if is_long:
        score_forca = rank_forca if not pd.isna(rank_forca) else 50
        if not pd.isna(rank_forca) and rank_forca < cfg.rs_min_long:
            bloqueios.append("forca_relativa_insuficiente_long")
    else:
        score_forca = 100 - rank_forca if not pd.isna(rank_forca) else 50
        if not pd.isna(rank_forca) and rank_forca > cfg.rs_max_short:
            bloqueios.append("forca_relativa_insuficiente_short")

    funding = _to_float(context.get("FundingRate"))
    funding_z = _to_float(context.get("Funding_ZScore"))
    oi_change = _to_float(context.get("OI_Change_1"))
    funding_score = 100.0
    if not pd.isna(funding):
        funding_score = max(0, 100 - abs(funding) / cfg.max_abs_funding_rate * 50)
        if is_long and funding > cfg.max_abs_funding_rate:
            bloqueios.append("funding_extremo_contra_long")
        if (not is_long) and funding < -cfg.max_abs_funding_rate:
            bloqueios.append("funding_extremo_contra_short")
    if not pd.isna(funding_z) and abs(funding_z) > cfg.max_funding_zscore:
        bloqueios.append("funding_zscore_extremo")
    oi_score = 50.0 if pd.isna(oi_change) else max(0, min(100, 50 + oi_change * 500))
    score_funding_oi = np.nanmean([funding_score, oi_score])

    setup_base = {"9.1": 60, "9.2": 75, "9.3": 75, "9.4": 70, "PC": 78, "BREAKOUT": 70}.get(setup, 65)

    score_total = (
        score_liq * 0.20
        + score_regime * 0.25
        + score_forca * 0.20
        + setup_base * 0.20
        + score_funding_oi * 0.10
        + 70.0 * 0.05
    )
    bloqueio_txt = ";".join(bloqueios)
    return {
        "REGIME": regime,
        "SCORE_TOTAL": round(float(score_total), 2),
        "SCORE_LIQUIDEZ": round(float(score_liq), 2),
        "SCORE_REGIME": round(float(score_regime), 2),
        "SCORE_FORCA": round(float(score_forca), 2),
        "SCORE_FUNDING_OI": round(float(score_funding_oi), 2),
        "SCORE_SETUP_BASE": setup_base,
        "RANK_FORCA": rank_forca,
        "BLOQUEIO_MOTIVO": bloqueio_txt,
        "APROVADO_SCORE": bool(score_total >= cfg.score_min_setup and "spread_alto" not in bloqueio_txt and "depth_baixo" not in bloqueio_txt),
    }


def aplicar_contexto_na_linha(ativos_df, idx, df, setup, direcao, context, rs_row, cfg, gatilho=None, tp=None, sl=None, swing_pct=None):
    last_closed = df.iloc[-2] if df is not None and len(df) >= 2 else pd.Series(dtype="object")
    score = score_setup_candidate_context(direcao, setup, last_closed, context, rs_row, cfg)

    # Score
    for k, v in score.items():
        ativos_df.at[idx, k] = v

    # Força relativa
    if rs_row is not None:
        for k in ["RET_3C", "RET_6C", "RET_12C", "RANK_FORCA"]:
            ativos_df.at[idx, k] = rs_row.get(k, np.nan)

    # Contexto de mercado
    mapping = {
        "Turnover24h": "TURNOVER24H",
        "Volume24h": "VOLUME24H",
        "Spread_Pct": "SPREAD_PCT",
        "DepthBid1Pct": "DEPTH_BID_1PCT",
        "DepthAsk1Pct": "DEPTH_ASK_1PCT",
        "DepthMin1Pct": "DEPTH_1PCT",
        "BookImbalance1Pct": "BOOK_IMBALANCE_1PCT",
        "SlippageEstPct": "SLIPPAGE_EST_PCT",
        "FundingRate": "FUNDING_RATE",
        "Funding_ZScore": "FUNDING_ZSCORE",
        "NextFundingTime": "NEXT_FUNDING_TIME",
        "OpenInterest": "OPEN_INTEREST",
        "OpenInterestValue": "OPEN_INTEREST_VALUE",
        "OI_Change_1": "OI_CHANGE_1",
        "OI_Change_3": "OI_CHANGE_3",
        "OI_Change_6": "OI_CHANGE_6",
        "OI_Change_24h": "OI_CHANGE_24H",
        "MarkPrice": "MARK_PRICE",
        "IndexPrice": "INDEX_PRICE",
        "BasisMarkIndex": "BASIS_MARK_INDEX",
        "BasisLastMark": "BASIS_LAST_MARK",
        "LongRatio": "LONG_RATIO",
        "ShortRatio": "SHORT_RATIO",
        "LongShortRatio": "LONG_SHORT_RATIO",
    }
    for src, dest in mapping.items():
        ativos_df.at[idx, dest] = context.get(src, np.nan)

    # Indicadores de regime no candle fechado [-1]
    for k in [
        "ATR_PCT", "ATR_PERCENTIL", "ADX", "CHOP", "EFFICIENCY_RATIO", "BB_WIDTH_PERCENTIL",
        "SLOPE_MME9_ATR_5", "DIST_MME9_ATR", "DIST_MMA21_ATR",
        "BODY_RANGE_RATIO", "RANGE_ATR", "CLOSE_POSITION",
    ]:
        ativos_df.at[idx, k] = last_closed.get(k, np.nan)

    # Grid/TP/SL estimados
    try:
        if gatilho and tp:
            ativos_df.at[idx, "TP_ESTIMADO_PCT"] = abs((float(tp) - float(gatilho)) / float(gatilho)) * 100
        if gatilho and sl:
            ativos_df.at[idx, "SL_ESTIMADO_PCT"] = abs((float(sl) - float(gatilho)) / float(gatilho)) * 100
        if swing_pct is not None and not pd.isna(swing_pct):
            ativos_df.at[idx, "GRID_ESTIMADO"] = int(max(0, round((float(swing_pct) / 100) / cfg.grid_spacing_pct)))
    except Exception:
        pass


def gerar_excel_com_graficos(candles_dict, ativos_df, nome_arquivo='ativos_opt.xlsx',
                            universo_bybit_df=None, config_context=None, forca_relativa_df=None):
    writer = pd.ExcelWriter(nome_arquivo, engine='xlsxwriter')
    workbook = writer.book

    # === ABA 1: Tabela de resultados ===
    colunas_base_saida = [
        'Par', 'Timeframe', 'Mercado', 'Time Stamp', 'Setup', 'COMPRA/VENDA', 'ARMAR/DISPARAR',
        'GATILHO', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'MME9', 'MMA21', 'VOLUME', 'VOLUME_MMA21', 'CLOSE_ZERO', 'OPEN_ZERO',
        'ATR_PERIOD','PARAM_ORIGEM','ATR_M1','K_SL','K_TP','SL','TP',
        'SWING_ABS', 'SWING_PCT', 'SLOPE_MME9_PCT', 'RAZAO_PAVIO_CORPO'
    ]
    colunas_saida = colunas_base_saida + CONTEXT_OUTPUT_COLUMNS

    tabela_saida = []

    for _, linha in ativos_df.iterrows():
        resultado = str(linha.get('Último Setup Identificado', '')).strip()
        if resultado.startswith('ARMAR') or resultado.startswith('DISPARAR'):
            par = linha['Par']
            df = candles_dict.get(par)
            if df is None or len(df) < 21:
                continue

            # Cálculo da média móvel de 21 períodos do volume
            df['VOLUME_MMA21'] = df['volume'].rolling(window=21).mean()
            candle_zero = df.iloc[-1]
            candle_m1 = df.iloc[-2]

            try:
                partes = resultado.split(' ')
                status = partes[0]
                setup = partes[2]
                direcao = partes[1]  # COMPRA ou VENDA
                gatilho = float(resultado.split('gatilho: ')[1].split(' ')[0])
            except Exception:
                continue

            tf_norm = normalize_timeframe(linha['Timeframe'])
            params = {
                "atr_period": int(float(linha.get('_ATR_PERIOD', ATR_PERIODO_SLTP) or ATR_PERIODO_SLTP)),
                "k_sl": float(linha.get('_K_SL', K_SL_PADRAO) or K_SL_PADRAO),
                "k_tp": float(linha.get('_K_TP', K_TP_PADRAO) or K_TP_PADRAO),
                "origem": linha.get('_PARAM_ORIGEM', 'padrao')
            }
            atr_period = params['atr_period']; k_sl = params['k_sl']; k_tp = params['k_tp']; origem = params['origem']

            atr_series = compute_atr(df, period=atr_period, method="wilder")
            atr_m1 = float(atr_series.iloc[-2]) if len(atr_series) >= 2 else None

            idx_inicio_swing, idx_fim_swing = obter_intervalo_swing_por_setup(
                df,
                nome_setup=setup,
                direcao=direcao
            )

            swing_abs = calcular_swing_absoluto_intervalo(
                df,
                idx_inicio=idx_inicio_swing,
                idx_fim=idx_fim_swing,
                direcao=direcao
            )

            swing_pct = calcular_swing_percentual_intervalo(
                df,
                idx_inicio=idx_inicio_swing,
                idx_fim=idx_fim_swing,
                direcao=direcao
            )
            slope_mme9_pct = calcular_slope_mme9(df, periodos=SLOPE_MME9_PERIODOS)
            razao_pavio_corpo = calcular_razao_pavio_corpo(df, lookback=LOOKBACK_PAVIO_CORPO)

            SL = TP = None
            if atr_m1 is not None:
                if direcao.upper() == 'COMPRA':
                    SL = float(gatilho) - k_sl * atr_m1
                    TP = float(gatilho) + k_tp * atr_m1
                elif direcao.upper() == 'VENDA':
                    SL = float(gatilho) + k_sl * atr_m1
                    TP = float(gatilho) - k_tp * atr_m1

            row_base = [
                par,
                linha['Timeframe'],
                linha['Mercado'],
                candle_zero['timestamp'].strftime('%d/%m/%Y %H:%M'),
                setup,
                direcao,
                status.capitalize(),
                gatilho,
                candle_m1['open'],
                candle_m1['high'],
                candle_m1['low'],
                candle_m1['close'],
                candle_m1.get('MME9', None),
                candle_m1.get('MMA21', None),
                float(candle_m1.get('volume', 0) or 0),
                candle_m1.get('VOLUME_MMA21', None),
                candle_zero['close'],
                candle_zero['open'],
                atr_period, origem, atr_m1, k_sl, k_tp, SL, TP,
                swing_abs, swing_pct, slope_mme9_pct, razao_pavio_corpo
            ]
            row_context = [linha.get(col, None) for col in CONTEXT_OUTPUT_COLUMNS]
            tabela_saida.append(row_base + row_context)

    df_saida = pd.DataFrame(tabela_saida, columns=colunas_saida)
    df_saida.to_excel(writer, sheet_name='Setups Identificados', index=False, startrow=1, header=False)

    # Formatação de cabeçalhos
    worksheet_tabela = writer.sheets['Setups Identificados']
    header_format = workbook.add_format({'bold': True, 'bg_color': '#0F766E', 'font_color': '#FFFFFF', 'border': 1})
    for col_num, value in enumerate(df_saida.columns.values):
        worksheet_tabela.write(0, col_num, value, header_format)

    worksheet_tabela.freeze_panes(1, 0)
    worksheet_tabela.autofilter(0, 0, max(len(df_saida), 1), max(len(df_saida.columns)-1, 0))

    # Formatação
    formato_decimal = workbook.add_format({'num_format': '#,##0.00000000'})
    formato_pct = workbook.add_format({'num_format': '0.00%'})
    formato_int = workbook.add_format({'num_format': '#,##0'})
    formato_texto = workbook.add_format({'text_wrap': True})

    colunas_decimal = [
        'GATILHO', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'MME9', 'MMA21', 'CLOSE_ZERO', 'OPEN_ZERO',
        'ATR_M1','K_SL','K_TP','SL','TP', 'SWING_ABS',
        'SLOPE_MME9_PCT', 'RAZAO_PAVIO_CORPO', 'SCORE_TOTAL', 'SCORE_LIQUIDEZ',
        'SCORE_REGIME', 'SCORE_FORCA', 'SCORE_FUNDING_OI', 'RANK_FORCA',
        'DEPTH_BID_1PCT', 'DEPTH_ASK_1PCT', 'DEPTH_1PCT', 'OPEN_INTEREST', 'OPEN_INTEREST_VALUE',
        'MARK_PRICE', 'INDEX_PRICE', 'ADX', 'CHOP', 'EFFICIENCY_RATIO',
        'GRID_ESTIMADO', 'TP_ESTIMADO_PCT', 'SL_ESTIMADO_PCT'
    ]
    # Campos em fração real, nos quais 0,01 representa 1%.
    # Campos já expressos em pontos/percentis (SWING_PCT, ATR_PERCENTIL, SCORE etc.) ficam em decimal comum.
    colunas_pct = [
        'RET_3C', 'RET_6C', 'RET_12C', 'SPREAD_PCT', 'SLIPPAGE_EST_PCT',
        'FUNDING_RATE', 'OI_CHANGE_1', 'OI_CHANGE_3', 'OI_CHANGE_6', 'OI_CHANGE_24H',
        'BASIS_MARK_INDEX', 'BASIS_LAST_MARK', 'ATR_PCT'
    ]
    colunas_int = ['VOLUME', 'VOLUME_MMA21', 'VOLUME24H', 'TURNOVER24H']

    for col_nome in df_saida.columns:
        col_idx = df_saida.columns.get_loc(col_nome)
        width = 16
        if col_nome in colunas_decimal:
            worksheet_tabela.set_column(col_idx, col_idx, width, formato_decimal)
        elif col_nome in colunas_pct:
            worksheet_tabela.set_column(col_idx, col_idx, width, formato_pct)
        elif col_nome in colunas_int:
            worksheet_tabela.set_column(col_idx, col_idx, width, formato_int)
        elif col_nome in ['BLOQUEIO_MOTIVO']:
            worksheet_tabela.set_column(col_idx, col_idx, 38, formato_texto)
        else:
            worksheet_tabela.set_column(col_idx, col_idx, min(max(len(str(col_nome)) + 2, 12), 22))

    # === Abas auxiliares do contexto ===
    if config_context is not None:
        cfg_df = context_config_to_df(config_context)
        cfg_df.to_excel(writer, sheet_name='CONFIG_CONTEXT', index=False)
        ws_cfg = writer.sheets['CONFIG_CONTEXT']
        ws_cfg.set_column(0, 0, 32)
        ws_cfg.set_column(1, 1, 24)

    if universo_bybit_df is not None and not universo_bybit_df.empty:
        universo_bybit_df.to_excel(writer, sheet_name='UNIVERSO_BYBIT', index=False)
        ws_uni = writer.sheets['UNIVERSO_BYBIT']
        ws_uni.freeze_panes(1, 0)
        ws_uni.autofilter(0, 0, max(len(universo_bybit_df), 1), max(len(universo_bybit_df.columns)-1, 0))
        ws_uni.set_column(0, min(len(universo_bybit_df.columns)-1, 20), 16)

    if forca_relativa_df is not None and not forca_relativa_df.empty:
        forca_relativa_df.to_excel(writer, sheet_name='FORCA_RELATIVA', index=False)
        ws_rs = writer.sheets['FORCA_RELATIVA']
        ws_rs.freeze_panes(1, 0)
        ws_rs.autofilter(0, 0, max(len(forca_relativa_df), 1), max(len(forca_relativa_df.columns)-1, 0))
        ws_rs.set_column(0, min(len(forca_relativa_df.columns)-1, 10), 16)

    # === ABA 2: Gráficos ===
    if not GERAR_GRAFICOS:
        writer.close()
        return

    worksheet = workbook.add_worksheet('Graficos')
    writer.sheets['Graficos'] = worksheet

    linha_atual = 0

    for _, linha in ativos_df.iterrows():
        resultado = linha['Último Setup Identificado']
        par = linha['Par']

        if not (resultado.startswith('ARMAR') or resultado.startswith('DISPARAR')):
            continue

        df = candles_dict.get(par)
        if df is None or len(df) < 13:
            continue

        df_plot = df.iloc[-13:].copy()
        df_plot.set_index('timestamp', inplace=True)
        df_plot.index.name = 'Date'

        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df_plot.columns:
                df_plot[col] = pd.to_numeric(df_plot[col], errors='coerce')

        if df_plot[['open', 'high', 'low', 'close']].isnull().any().any():
            continue

        apds = []
        if 'MME9' in df_plot.columns:
            apds.append(mpf.make_addplot(df_plot['MME9'], color='blue'))
        if 'MMA21' in df_plot.columns:
            apds.append(mpf.make_addplot(df_plot['MMA21'], color='orange'))

        try:
            img_data = BytesIO()
            mpf.plot(
                df_plot,
                type='candle',
                style='yahoo',
                addplot=apds,
                title=par,
                ylabel='Preço',
                figsize=(8, 4),
                savefig=dict(fname=img_data, dpi=100, format='png')
            )
            img_data.seek(0)
            worksheet.insert_image(linha_atual, 0, '', {'image_data': img_data})
            linha_atual += 21
        except Exception as e:
            print(f"[❌] Erro ao gerar gráfico para {par}: {e}")
            continue

    writer.close()

# Função para buscar candles da Bybit
def obter_candles(par='BTCUSDT', interval='15', limit=50, mercado='linear'):
    session = HTTP(testnet=False)
    dados = session.get_kline(
        category=mercado,
        symbol=par,
        interval=interval,
        limit=limit
    )
    df = pd.DataFrame(dados['result']['list'], columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
    ])
    df['open'] = df['open'].astype(float)
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['timestamp'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit='ms')
    df = df.iloc[::-1]      # ordena em ordem cronológica (do mais antigo ao mais recente)
#    df = df[:-1]           #cancela a remoção do [0] anulando>>> # remove o último candle (em formação)>>[-1] representa o último candle fechado de verdade

    return df

# Função para enviar alerta via Telegram
def enviar_alerta_telegram(mensagem):
    bot_token = '7564918167:AAGm5HpWXGQ3boiKgaivan3_JSsmv-fO4a0'
    chat_id = '6883823169'
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {'chat_id': chat_id, 'text': mensagem}
    try:
        if ENVIAR_ALERTA_TELEGRAM:
            requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"Erro ao enviar alerta Telegram: {e}")

# Função para configurar horário de execução
from datetime import datetime

def dentro_do_horario():
    agora = datetime.now() if USAR_HORARIO_LOCAL else datetime.utcnow()
    hora = agora.hour
    minuto = agora.minute
    dia_semana = agora.weekday()

    # Calcula o "erro" entre o minuto atual e o mais próximo múltiplo de intervalo
    erro_minuto = abs(minuto % INTERVALO_MINUTOS)
    margem_erro = 1  # Aceita 1 minuto de tolerância

    logging.debug(f"    Condições: "
          f"permite dia útil (dia) ou fim de semana (True)? {dia_semana < 5 or PERMITIR_FIM_DE_SEMANA}, "
          f"hora entre {HORA_INICIO}-{HORA_FIM}? {HORA_INICIO <= hora <= HORA_FIM}, "
          f"minuto dentro de tolerância ±{margem_erro}? {erro_minuto <= margem_erro or INTERVALO_MINUTOS - erro_minuto <= margem_erro}")

    if not PERMITIR_FIM_DE_SEMANA and dia_semana >= 5:
        return False

    if HORA_INICIO <= hora <= HORA_FIM:
        if erro_minuto <= margem_erro or INTERVALO_MINUTOS - erro_minuto <= margem_erro:
            return True

    return False

# Salvar no Google Drive a partir do Hostinger
load_dotenv()

def upload_file_to_drive(local_file_path, drive_folder_id):
    """
    Faz upload de um arquivo para o Google Drive, substituindo-o se já existir.
    As credenciais devem estar no .env:
    GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET, GDRIVE_REFRESH_TOKEN, GDRIVE_FOLDER_ID.
    """
    client_id = os.environ.get("GDRIVE_CLIENT_ID")
    client_secret = os.environ.get("GDRIVE_CLIENT_SECRET")
    refresh_token = os.environ.get("GDRIVE_REFRESH_TOKEN")

    if not drive_folder_id:
        raise RuntimeError("GDRIVE_FOLDER_ID não encontrado nas variáveis de ambiente.")

    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Credenciais do Google Drive não encontradas nas variáveis de ambiente.")

    gauth = GoogleAuth()
    gauth.settings["client_config_backend"] = "settings"
    gauth.settings["client_config"] = {
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://accounts.google.com/o/oauth2/token",
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob"
    }

    credentials = OAuth2Credentials(
        None,
        client_id,
        client_secret,
        refresh_token,
        None,
        "https://accounts.google.com/o/oauth2/token",
        None
    )
    gauth.credentials = credentials

    if gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()

    drive = GoogleDrive(gauth)
    file_name = os.path.basename(local_file_path)
    query = f"title='{file_name}' and '{drive_folder_id}' in parents and trashed=false"
    file_list = drive.ListFile({'q': query}).GetList()

    if file_list:
        existing_file = file_list[0]
        logging.info(f"Arquivo encontrado no Drive (ID={existing_file['id']}), atualizando conteúdo...")
        existing_file.SetContentFile(local_file_path)
        existing_file.Upload()
        logging.info(f"Arquivo '{file_name}' atualizado com sucesso no Google Drive.")
    else:
        logging.info("Arquivo não encontrado no Drive, fazendo upload como novo arquivo.")
        new_file = drive.CreateFile({'title': file_name, 'parents': [{'id': drive_folder_id}]})
        new_file.SetContentFile(local_file_path)
        new_file.Upload()
        logging.info(f"Arquivo '{file_name}' enviado com sucesso para a pasta do Drive.")

# Função para gerar valores de SL E TP
def caminho_json(par, timeframe, objective="mar") -> Path:
    tf = normalize_timeframe(timeframe)
    fname = f"opt_{par}_{tf}m_{objective}.json"
    return DIRETORIO_OPT / fname

def json_otimizacao_ainda_valido(par, timeframe, objective="mar", dias_validade=7):
    path = caminho_json(par, timeframe, objective)
    if not path.exists():
        return False

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        generated_at = data.get("generated_at")
        if not generated_at:
            return False

        dt_gen = datetime.strptime(generated_at, "%Y-%m-%d %H:%M:%S")
        idade = datetime.now() - dt_gen

        return idade <= timedelta(days=dias_validade)

    except Exception as e:
        logging.warning(f"[OPT] Não foi possível validar idade do JSON {path}: {e}")
        return False
    
def carregar_params_otimizados(par, timeframe, objective="mar"):
    """
    Lê opt_{par}_{tf}m_{objective}.json e extrai atr_period, k_sl e k_tp
    aceitando variações de chaves tanto em best_params quanto na raiz.
    """
    path = caminho_json(par, timeframe, objective)
    if not path.exists():
        return None

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logging.warning(f"[OPT] Falha ao ler {path}: {e}")
        return None

    # Onde procurar: best_params primeiro, depois raiz
    candidates = []
    if isinstance(data, dict):
        if isinstance(data.get("best_params"), dict):
            candidates.append(data["best_params"])
        candidates.append(data)

    def pick_float(d, keys, default=None):
        for k in keys:
            if k in d and d[k] is not None:
                try:
                    return float(d[k])
                except Exception:
                    pass
        return default

    def pick_int(d, keys, default=None):
        for k in keys:
            if k in d and d[k] is not None:
                try:
                    return int(float(d[k]))
                except Exception:
                    pass
        return default

    atr_period = k_sl = k_tp = None

    # aliases aceitos
    ATR_KEYS = ["atr_period", "ATR_PERIOD", "atr", "atr_p", "atr_len"]
    KSL_KEYS = ["k_sl", "K_SL", "ksl", "sl_mult", "sl", "k_stop", "k_sl_mult"]
    KTP_KEYS = ["k_tp", "K_TP", "ktp", "tp_mult", "tp", "k_take", "k_tp_mult"]

    for d in candidates:
        if atr_period is None:
            atr_period = pick_int(d, ATR_KEYS)
        if k_sl is None:
            k_sl = pick_float(d, KSL_KEYS)
        if k_tp is None:
            k_tp = pick_float(d, KTP_KEYS)

    if atr_period is None: atr_period = ATR_PERIODO_SLTP
    if k_sl is None:       k_sl       = K_SL_PADRAO
    if k_tp is None:       k_tp       = K_TP_PADRAO

    logging.info(f"[OPT] Carregado de {path.name} | atr={atr_period} k_sl={k_sl} k_tp={k_tp}")
    return {"atr_period": atr_period, "k_sl": k_sl, "k_tp": k_tp, "origem": "otimizado"}

def salvar_params_otimizados(par, timeframe, objective, best_params, best_score):
    """Salva JSON em DIRETORIO_OPT com nome normalizado."""
    path = caminho_json(par, timeframe, objective)
    payload = {
        "symbol": par,
        "interval": normalize_timeframe(timeframe),
        "objective": objective,
        "best_params": best_params,
        "best_score": best_score,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logging.info(f"[OPT] ✅ Salvo: {path}")

def garantir_params(par, timeframe, df_hist=None, objective="mar",
                    auto_optimize=False, use_optuna=False, limit=1000):
    """
    Regras:
    1) Se existir JSON válido (recente), usa.
    2) Se não existir JSON, ou estiver vencido, e auto_optimize=True, reotimiza.
    3) Se existir JSON vencido e auto_optimize=False, usa mesmo assim.
    4) Se não existir nada, usa padrão.
    """
    params = carregar_params_otimizados(par, timeframe, objective=objective)

    json_valido = json_otimizacao_ainda_valido(
        par, timeframe,
        objective=objective,
        dias_validade=REOTIMIZAR_APOS_DIAS
    )

    # Caso 1: JSON existe e está válido
    if params and json_valido:
        logging.info(f"[OPT] JSON válido encontrado para {par} {timeframe}m -> usando parâmetros otimizados")
        return params

    # Caso 2: JSON não existe ou venceu, e auto_optimize está ativo
    if auto_optimize:
        try:
            from optimizer_atr_sl_tp import run_optimization_with_setups

            if df_hist is None:
                df_hist = obter_candles(par=par, interval=str(timeframe), limit=limit, mercado="linear")
                if df_hist is None or df_hist.empty:
                    raise RuntimeError("Sem dados para otimização.")
                if "timestamp" in df_hist.columns:
                    df_hist = df_hist.set_index("timestamp")

            SETUPS = [setup_9_1, setup_9_2, setup_9_3, setup_9_4, setup_pc]

            logging.info(f"[OPT] Reotimizando {par} {timeframe}m...")
            best_params, best_score = run_optimization_with_setups(
                df_hist,
                setup_funcs=SETUPS,
                objective=objective,
                use_optuna=use_optuna
            )

            salvar_params_otimizados(par, timeframe, objective, best_params, best_score)

            return {
                "atr_period": int(best_params.get("atr_period", ATR_PERIODO_SLTP)),
                "k_sl": float(best_params.get("k_sl", K_SL_PADRAO)),
                "k_tp": float(best_params.get("k_tp", K_TP_PADRAO)),
                "origem": "otimizado"
            }

        except Exception as e:
            logging.warning(f"[OPT] Auto-optimize falhou p/ {par} {timeframe}m: {e}")

    # Caso 3: JSON existe, mas está vencido, e não foi pedido auto-optimize
    if params:
        logging.info(f"[OPT] JSON vencido para {par} {timeframe}m -> usando mesmo assim")
        return params

    # Caso 4: fallback padrão
    logging.info(f"[OPT] Sem JSON para {par} {timeframe}m -> usando parâmetros padrão")
    return {
        "atr_period": ATR_PERIODO_SLTP,
        "k_sl": K_SL_PADRAO,
        "k_tp": K_TP_PADRAO,
        "origem": "padrao"
    }
# Lê todos os pares/timeframes da ativos.xlsx, baixa dados e gera/atualiza os JSONs opt_*.json.
def cli_optimize_from_excel(objective="mar", use_optuna=False, limit=1000, mercado_default="linear"):
    try:
        ativos_df = pd.read_excel(ARQUIVO_EXCEL)
    except Exception as e:
        logging.error(f"Erro ao carregar o arquivo Excel: {e}")
        return 1

    pares = ativos_df[['Par','Timeframe']].dropna().drop_duplicates()
    from optimizer_atr_sl_tp import run_optimization_with_setups
    SETUPS = [setup_9_1, setup_9_2, setup_9_3, setup_9_4, setup_pc]

    for _, row in pares.iterrows():
        par = row['Par']; timeframe = row['Timeframe']; tf_norm = normalize_timeframe(timeframe)
        df_hist = obter_candles(par=par, interval=tf_norm, limit=limit, mercado=mercado_default)
        if df_hist is None or df_hist.empty:
            logging.warning(f"[OPT-ALL] Sem dados para {par} {tf_norm}m")
            continue
        if "timestamp" in df_hist.columns:
            df_hist = df_hist.set_index("timestamp")

        best_params, best_score = run_optimization_with_setups(
            df_hist, setup_funcs=SETUPS, objective=objective, use_optuna=use_optuna
        )

        salvar_params_otimizados(par, timeframe, objective, best_params, best_score)
        logging.info(f"[OPT-ALL] ✅ {par} {tf_norm}m salvo em {caminho_json(par, timeframe, objective)}")

    return 0

# Lê o universo dinâmico da Bybit, aplica CONFIG_UNIVERSO/EXCECOES_ATIVOS e gera/atualiza os JSONs opt_*.json.
def cli_optimize_from_universe(objective="mar", use_optuna=False, limit=1000, modo_universo=None, mercado_default="linear"):
    try:
        manual_df = pd.read_excel(ARQUIVO_EXCEL)
        manual_df = manual_df.loc[:, ~manual_df.columns.duplicated(keep='first')]
    except Exception as e:
        logging.warning(f"[OPT-UNIVERSE] Não foi possível carregar {ARQUIVO_EXCEL}. Usando universo automático puro. Detalhe: {e}")
        manual_df = pd.DataFrame(columns=["Par", "Timeframe", "Mercado"])

    cfg = ler_config_contexto(ARQUIVO_EXCEL)
    if modo_universo:
        cfg.modo_universo = str(modo_universo).upper().strip()
    if not cfg.modo_universo or str(cfg.modo_universo).upper().strip() == "MANUAL":
        # Para este comando, MANUAL continua permitido, mas AUTO/HIBRIDO são os usos naturais.
        logging.info(f"[OPT-UNIVERSE] MODO_UNIVERSO={cfg.modo_universo}")

    client = BybitPublicV5(sleep_s=cfg.api_sleep_s)
    try:
        ativos_universo_df, universo_bybit_df, _ = montar_universo_para_scan(
            ARQUIVO_EXCEL, manual_df, cfg, client
        )
    except Exception as e:
        logging.error(f"[OPT-UNIVERSE] Erro ao montar universo dinâmico: {e}")
        return 1

    if ativos_universo_df is None or ativos_universo_df.empty:
        logging.error("[OPT-UNIVERSE] Universo dinâmico vazio. Nenhum JSON será gerado.")
        return 1

    if "Mercado" not in ativos_universo_df.columns:
        ativos_universo_df["Mercado"] = mercado_default or cfg.category

    pares = (
        ativos_universo_df[["Par", "Timeframe", "Mercado"]]
        .dropna(subset=["Par", "Timeframe"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    logging.info(f"[OPT-UNIVERSE] Iniciando otimização para {len(pares)} par(es)/timeframe(s) do universo dinâmico.")
    from optimizer_atr_sl_tp import run_optimization_with_setups
    SETUPS = [setup_9_1, setup_9_2, setup_9_3, setup_9_4, setup_pc]

    sucessos = 0
    falhas = 0
    for _, row in pares.iterrows():
        par = str(row["Par"]).upper().strip()
        timeframe = normalize_timeframe(row["Timeframe"])
        mercado = str(row.get("Mercado", mercado_default or cfg.category)).lower().strip()
        if not mercado or mercado == "nan":
            mercado = mercado_default or cfg.category

        try:
            logging.info(f"[OPT-UNIVERSE] Otimizando {par} {timeframe}m mercado={mercado}...")
            df_hist = obter_candles(par=par, interval=timeframe, limit=limit, mercado=mercado)
            if df_hist is None or df_hist.empty:
                logging.warning(f"[OPT-UNIVERSE] Sem dados para {par} {timeframe}m")
                falhas += 1
                continue
            if "timestamp" in df_hist.columns:
                df_hist = df_hist.set_index("timestamp")

            best_params, best_score = run_optimization_with_setups(
                df_hist, setup_funcs=SETUPS, objective=objective, use_optuna=use_optuna
            )

            salvar_params_otimizados(par, timeframe, objective, best_params, best_score)
            sucessos += 1
            logging.info(f"[OPT-UNIVERSE] ✅ {par} {timeframe}m salvo em {caminho_json(par, timeframe, objective)}")

        except Exception as e:
            falhas += 1
            logging.warning(f"[OPT-UNIVERSE] Falha ao otimizar {par} {timeframe}m: {e}")

    logging.info(f"[OPT-UNIVERSE] Finalizado. Sucessos={sucessos} | Falhas={falhas}")
    return 0 if sucessos > 0 else 1

# === Setups de Larry Williams ===
# === SETUP 9.1 ===
def setup_9_1(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0] em formação
    df = df.iloc[:-1]          # Remove o candle em formação

    df['MME9'] = df['close'].ewm(span=9).mean()
    mme = df['MME9']

    logging.debug(f"    ANALISANDO 9.1")         

    # COMPRA
    if tendencia_sequencia(mme, 'baixa'):
        logging.debug(f"🔽 Tendência de baixa confirmada. E a reversão?")

        if mme.iloc[-2] < mme.iloc[-1]:
            c1 = df.iloc[-1]
            c2 = df.iloc[-2]
            c3 = df.iloc[-3]

            logging.debug(f"Candle [-1] (c1): close={c1['close']}, low={c1['low']}, high={c1['high']}")
            logging.debug(f"[Candle [-2] (c2): close={c2['close']}, low={c2['low']}")
            logging.debug(f"[Candle [-3] (c3): close={c3['close']}, low={c3['low']}")
            logging.debug(f"[Candle [0] (zero): low={candle_zero['low']}, high={candle_zero['high']}")

            gatilho = df['high'].iloc[-1]
            if candle_zero['high'] > c1['high']:
                logging.debug(f" ✅ Condição de COMPRA atendida - Gatilho em high[-2] = {gatilho:.7f}")
                return {
                    'status': 'DISPARAR COMPRA 9.1',
                    'gatilho': gatilho,
                    'tipo': 'compra',
                    'coluna': 'high'
                }
            return {
                'status': 'ARMAR COMPRA 9.1',
                'gatilho': gatilho,
                'tipo': 'compra',
                'coluna': 'high'
            }
        logging.debug(f"    Reversão para alta não identificada")

    else: logging.debug(f"    Tendência de baixa não identificada")

# VENDA
    if tendencia_sequencia(mme, 'alta'):
        logging.debug(f"🔼 Tendência de alta confirmada. E a reversão?")

        if mme.iloc[-2] > mme.iloc[-1]:
            c1 = df.iloc[-1]
            c2 = df.iloc[-2]
            c3 = df.iloc[-3]

            logging.debug(f"Candle [-1] (c1): close={c1['close']}, low={c1['low']}, high={c1['high']}")
            logging.debug(f"Candle [-2] (c2): close={c2['close']}, low={c2['low']}")
            logging.debug(f"Candle [-3] (c3): close={c3['close']}, low={c3['low']}")
            logging.debug(f"Candle [0] (zero): low={candle_zero['low']}, high={candle_zero['high']}")

            gatilho = df['low'].iloc[-1]
            if candle_zero['low'] < c1['low']:
                logging.debug(f"✅ Condição de VENDA atendida - Gatilho em low[-2] = {gatilho:.7f}")
                return {
                    'status': 'DISPARAR VENDA 9.1',
                    'gatilho': gatilho,
                    'tipo': 'venda',
                    'coluna': 'low'
                }
            return {
                'status': 'ARMAR VENDA 9.1',
                'gatilho': gatilho,
                'tipo': 'venda',
                'coluna': 'low'
            }
        logging.debug(f"    Reversão para baixa não identificada")

    logging.debug(f"    Tendência de alta não identificada")

    return None

# === SETUP 9.2 ===
def setup_9_2(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0] ainda em formação
    df = df.iloc[:-1]          # Remove o candle [0]; agora [-1] é o último fechado
    df['MME9'] = df['close'].ewm(span=9).mean()
    mme = df['MME9']

    logging.debug(f"    ANALISANDO 9.2")         

    # === COMPRA ===
    if tendencia_suave(mme, tipo='alta', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE):
        logging.debug(f"🔽 Tendência de alta confirmada")
        
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]

        logging.debug(f"    Candle [-1] (c1): close={c1['close']}, high={c1['high']}")
        logging.debug(f"    Candle [-2] (c2): close={c2['close']}, high={c2['high']}, low={c2['low']}")
        logging.debug(f"    Candle [-3] (c3): low={c3['low']}")
        logging.debug(f"    Candle [0] (zero): high={candle_zero['high']}, close={candle_zero['close']}")
       
        if c1['close'] < c2['low'] and c2['open'] < c2['close']: # Candle anterior é de ALTA
            if candle_zero['high'] > c1['high']:
                return {
                    'status': 'DISPARAR COMPRA 9.2',
                    'gatilho': c1['high'],
                    'tipo': 'compra',
                    'coluna': 'high',
                    'debug_origem': 'COMPRA-DISPARAR'
                }
            return {
                'status': 'ARMAR COMPRA 9.2',
                'gatilho': c1['high'],
                'tipo': 'compra',
                'coluna': 'high',
                'debug_origem': 'COMPRA-ARMAR'
            }

        logging.debug("." * 92)
        logging.debug(f"    Iniciando avaliação de escorregamento")
        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]

        if candle_m2['high'] > candle_m1['high']:
            for i in range(-2, -7, -1):
                if abs(i) > len(df) - 1:
                    break
                atual = df.iloc[i]
                anterior = df.iloc[i - 1]

                logging.debug(f"    i={i} | atual.close={atual['close']} | anterior.low={anterior['low']}")
                logging.debug(f"    Verificando continuidade: atual.high={atual['high']} vs anterior.high={anterior['high']}")
    
                if atual['close'] < anterior['low'] and anterior['open'] < anterior['close']: # Candle anterior é de ALTA
                    logging.debug(f" ↪️ Escorregamento válido encontrado em i={i}")
                    if candle_zero['high'] > c1['high']:
                        return {
                            'status': f'DISPARAR COMPRA 9.2',
                            'gatilho': c1['high'],
                            'tipo': 'compra',
                            'coluna': 'high',
                            'debug_origem': f'COMPRA-ESCORREGA-DISPARAR-i{i}'
                        }
                    return {
                        'status': f'ARMAR COMPRA 9.2',
                        'gatilho': c1['high'],
                        'tipo': 'compra',
                        'coluna': 'high',
                        'debug_origem': f'COMPRA-ESCORREGA-ARMAR-i{i}'
                    }
                if atual['close'] < anterior['low'] and anterior['open'] > anterior ['close']: # Candle anterior NÃO é de ALTA
                    logging.debug(f"    Não considera porque o candle anterior é de BAIXA → {anterior['open']} >= {anterior['close']}")

                if atual['high'] >= anterior['high']:
                    logging.debug(f"    Interrompe: máxima deixou de ser descendente → {atual['high']} >= {anterior['high']}")
                    break
        else:
            logging.debug(f"    Interrompe: máxima do Candle[-1]→{candle_m1['high']} >= máxima do Candle[-2]→{candle_m2['high']}")
    else: logging.debug(f"    Tendência de alta não identificada")

    # === VENDA ===
    if tendencia_suave(mme, tipo='baixa', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE):
        logging.debug(f"🔽 Tendência de baixa confirmada")
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]

        logging.debug(f"    Candle [-1] (c1): close={c1['close']}, low={c1['low']}")
        logging.debug(f"    Candle [-2] (c2): close={c2['close']}, low={c2['low']}, high={c2['high']}")
        logging.debug(f"    Candle [-3] (c3): high={c3['high']}")
        logging.debug(f"    Candle [0] (zero): low={candle_zero['low']}, close={candle_zero['close']}")

        if c1['close'] > c2['high'] and c2['open'] > c2['close']: # Candle anterior é de BAIXA
            if candle_zero['low'] < c1['low']:
                return {
                    'status': 'DISPARAR VENDA 9.2',
                    'gatilho': c1['low'],
                    'tipo': 'venda',
                    'coluna': 'low',
                    'debug_origem': 'VENDA-DISPARAR'
                }
            return {
                'status': 'ARMAR VENDA 9.2',
                'gatilho': c1['low'],
                'tipo': 'venda',
                'coluna': 'low',
                'debug_origem': 'VENDA-ARMAR'
            }

        logging.debug("." * 92)
        logging.debug(f"    Iniciando avaliação de escorregamento")
        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]

        if candle_m2['low'] < candle_m1['low']:
            for i in range(-2, -7, -1):
                if abs(i) > len(df) - 1:
                    break
                atual = df.iloc[i]
                anterior = df.iloc[i - 1]

                logging.debug(f"    i={i} | atual.close={atual['close']} | anterior.high={anterior['high']}")
                logging.debug(f"    Verificando continuidade: atual.low={atual['low']} vs anterior.low={anterior['low']}")

                if atual['close'] > anterior['high'] and anterior['open'] > anterior['close']:  # Candle anterior é de BAIXA
                    logging.debug(f" ↪️ Escorregamento válido encontrado em i={i}")
                    if candle_zero['low'] < c1['low']:
                        return {
                            'status': f'DISPARAR VENDA 9.2',
                            'gatilho': c1['low'],
                            'tipo': 'venda',
                            'coluna': 'low',
                            'debug_origem': f'VENDA-ESCORREGA-DISPARAR-i{i}'
                        }
                    return {
                        'status': f'ARMAR VENDA 9.2',
                        'gatilho': c1['low'],
                        'tipo': 'venda',
                        'coluna': 'low',
                        'debug_origem': f'VENDA-ESCORREGA-ARMAR-i{i}'
                    }

                if atual['close'] > anterior['low'] and anterior['open'] < anterior ['close']:  # Candle anterior NÃO é de BAIXA
                    logging.debug(f"    Não considera porque o candle anterior é de ALTA → {anterior['open']} < {anterior['close']}")
            
                if atual['low'] <= anterior['low']:
                    logging.debug(f"    Interrompe: mínima deixou de ser ascendente → {atual['low']} <= {anterior['low']}")
                    break
        else:
            logging.debug(f"    Interrompe: mínima do Candle[-1]→{candle_m1['low']} <= mínima do Candle[-2]→{candle_m2['low']}")
    else: logging.debug(f"    Tendência de baixa não identificada")
 
    return None

def setup_9_3(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0] ainda em formação
    df = df.iloc[:-1]          # Remove o candle [0]; agora [-1] é o último fechado

    df['MME9'] = df['close'].ewm(span=9).mean()
    mme = df['MME9']

    logging.debug(f"    ANALISANDO 9.3")         

    # === COMPRA ===
    if tendencia_suave(mme, tipo='alta', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE):
        c1 = df.iloc[-1]   # c[-1]
        c2 = df.iloc[-2]   # c[-2]
        c3 = df.iloc[-3]   # c[-3]

        logging.debug(f"    c1[-1]: close={c1['close']}, high={c1['high']}")
        logging.debug(f"    c2[-2]: close={c2['close']}, high={c2['high']}")
        logging.debug(f"    c3[-3]: close={c3['close']}")
        logging.debug(f"    Candle [0]: high={candle_zero['high']}")

        if c1['close'] < c3['close'] and c2['close'] < c3['close'] and c3['open'] < c3['close']: #Candle [-3] de Alta
            if candle_zero['high'] > c1['high']:
                return {
                    'status': 'DISPARAR COMPRA 9.3',
                    'gatilho': c1['high'],
                    'tipo': 'compra',
                    'coluna': 'high',
                    'debug_origem': 'COMPRA-DISPARAR'
                }
            return {
                'status': 'ARMAR COMPRA 9.3',
                'gatilho': c1['high'],
                'tipo': 'compra',
                'coluna': 'high',
                'debug_origem': 'COMPRA-ARMAR'
            }
        
        logging.debug("." * 92)
        logging.debug(f"    Iniciando avaliação de escorregamento")
        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]
        candle_m3 = df.iloc[-3]

        if candle_m3['high'] > candle_m2['high'] > candle_m1['high']:
            for i in range(-3, -8, -1):  # de -3 até -7
                if abs(i + 2) > len(df):
                    break

                atual = df.iloc[i]
                prox = df.iloc[i + 1]
                prox2 = df.iloc[i + 2]

                logging.debug(f"    i={i} | atual.close={atual['close']} | prox.close={prox['close']} | prox2.close={prox2['close']}")

                if atual['close'] > prox['close'] and atual['close'] > prox2['close'] and atual['open'] < atual['close']: # Atual é Candle de ALTA
                    if candle_zero['high'] > c1['high']:
                        return {
                            'status': 'DISPARAR COMPRA 9.3',
                            'gatilho': c1['high'],
                            'tipo': 'compra',
                            'coluna': 'high',
                            'debug_origem': f'COMPRA-ESCORREGA-DISPARAR-i{i}'
                        }
                    return {
                        'status': 'ARMAR COMPRA 9.3',
                        'gatilho': c1['high'],
                        'tipo': 'compra',
                        'coluna': 'high',
                        'debug_origem': f'COMPRA-ESCORREGA-ARMAR-i{i}'
                    }

                if atual['close'] > prox['close'] and atual['close'] > prox2['close'] and atual['open'] > atual['close']: # Atual NÃO é Candle de ALTA
                    logging.debug(f"    Não considera porque o candle atual é de BAIXA → {atual['open']} >= {atual['close']}")

                if not (atual['high'] > prox['high'] > prox2['high']):
                    logging.debug(f"Interrompe: máxima deixou de ser descendente: {atual['high']} → {prox['high']} → {prox2['high']}")
                    break
        else:
            logging.debug(f"    Interrompe: máxima do Candle[-1]→{candle_m1['high']} >= máxima do Candle[-2]→{candle_m2['high']} ou máxima do Candle[-2] >= Candle[-3]→{candle_m3['high']}")

    # === VENDA ===
    if tendencia_suave(mme, tipo='baixa', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE):
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]
        
        logging.debug(f"    c1[-1]: close={c1['close']}, low={c1['low']}")
        logging.debug(f"    c2[-2]: close={c2['close']}, low={c2['low']}")
        logging.debug(f"    c3[-3]: close={c3['close']}")
        logging.debug(f"    Candle [0]: low={candle_zero['low']}")

        if c1['close'] > c3['close'] and c2['close'] > c3['close'] and c3['open'] > c3['close']:
            if candle_zero['low'] < c1['low']:
                return {
                    'status': 'DISPARAR VENDA 9.3',
                    'gatilho': c1['low'],
                    'tipo': 'venda',
                    'coluna': 'low',
                    'debug_origem': 'VENDA-DISPARAR'
                }
            return {
                'status': 'ARMAR VENDA 9.3',
                'gatilho': c1['low'],
                'tipo': 'venda',
                'coluna': 'low',
                'debug_origem': 'VENDA-ARMAR'
            }

        logging.debug("." * 92)
        logging.debug(f"    Iniciando avaliação de escorregamento")
        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]
        candle_m3 = df.iloc[-3]

        if candle_m3['low'] < candle_m2['low'] < candle_m1['low']:
            for i in range(-3, -8, -1):
                if abs(i + 2) > len(df):
                    break

                atual = df.iloc[i]
                prox = df.iloc[i + 1]
                prox2 = df.iloc[i + 2]

                logging.debug(f"    i={i} | atual.close={atual['close']} | prox.close={prox['close']} | prox2.close={prox2['close']}")

                if atual['close'] < prox['close'] and atual['close'] < prox2['close'] and atual['open'] > atual['close']:
                    if candle_zero['low'] < c1['low']:
                        return {
                            'status': 'DISPARAR VENDA 9.3',
                            'gatilho': c1['low'],
                            'tipo': 'venda',
                            'coluna': 'low',
                            'debug_origem': f'VENDA-ESCORREGA-DISPARAR-i{i}'
                        }
                    return {
                        'status': 'ARMAR VENDA 9.3',
                        'gatilho': c1['low'],
                        'tipo': 'venda',
                        'coluna': 'low',
                        'debug_origem': f'VENDA-ESCORREGA-ARMAR-i{i}'
                    }

                if atual['close'] < prox['close'] and atual['close'] < prox2['close'] and atual['open'] < atual['close']: # Atual NÃO é Candle de BAIXA
                    logging.debug(f"    Não considera porque o candle atual é de ALTA → {atual['open']} >= {atual['close']}")

                if not (atual['low'] < prox['low'] < prox2['low']):
                    logging.debug(f"    Interrompe: mínima deixou de ser ascendente: {atual['low']} → {prox['low']} → {prox2['low']}")
                    break
        else:
            logging.debug(f"    Interrompe: mínima do Candle[-1]→{candle_m1['low']} <= mínima do Candle[-2]→{candle_m2['low']} ou a mínima do Candle[-2] <= mínima do Candle[-3]→{candle_m3['low']}")

    return None

# === SETUP 9.4 ===
def setup_9_4(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0] em formação
    df = df.iloc[:-1]          # Remove o candle em formação

    df['MME9'] = df['close'].ewm(span=9).mean()
    mme = df['MME9']

    logging.debug(f"    ANALISANDO 9.4")         

    # === COMPRA
    if (tendencia_suave(mme, tipo='alta', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE) and
        mme.iloc[-3] > mme.iloc[-2] and
        mme.iloc[-2] < mme.iloc[-1]):

        logging.debug(f"🔼 Tendência de alta e S confirmados")
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]

        if (c2['low'] < c1['low']):
            logging.debug(f"    Candle [-1] (c1): close={c1['close']}, low={c1['low']}, high={c1['high']}")
            logging.debug(f"    Candle [-2] (c2): close={c2['close']}, low={c2['low']}")
            logging.debug(f"    Candle [-3] (c3): close={c3['close']}, low={c3['low']}")
            logging.debug(f"    Candle [0] (zero): low={candle_zero['low']}, high={candle_zero['high']}")

            logging.debug(f"    ----------------------------------------------")
            logging.debug(f"    Curva S estabelecida")
            logging.debug(f"    MME9[-3]: {mme.iloc[-3]} > MME9[-2]: {mme.iloc[-2]} < MME9[-1]: {mme.iloc[-1]}")
            logging.debug(f"    Mínima do candle [-2] NÃO rompida pelo candle [-1]")
            logging.debug(f"    Candle [-1] (c1): low={c1['low']} > candle [-2]: (c2) low={c2['low']}")
            logging.debug(f"    ----------------------------------------------")

            gatilho = c1['high']
            if candle_zero['high'] >= c1['high']:
                logging.debug(f"✅ DISPARAR COMPRA 9.4")
                logging.debug(f"    Gatilho = high[-1] = {gatilho:.7f}")

                return {
                'status': 'DISPARAR COMPRA 9.4',
                'gatilho': gatilho,
                'tipo': 'COMPRA',
                'coluna': 'HIGH'
                }
                
            logging.debug(f"✅ ARMAR COMPRA 9.4")
            logging.debug(f"    MME9: -3 = {mme.iloc[-3]:.7f}, -2 = {mme.iloc[-2]:.7f}, -1 = {mme.iloc[-1]:.7f}")
            logging.debug(f"    Gatilho = high[-1] = {gatilho:.7f}")

            return {
            'status': 'ARMAR COMPRA 9.4',
            'gatilho': gatilho,
            'tipo': 'compra',
            'coluna': 'high'
            }
        logging.debug(f"    ----------------------------------------------")
        logging.debug(f"    Curva S estabelecida")
        logging.debug(f"    MME9[-3]: {mme.iloc[-3]} > MME9[-2]: {mme.iloc[-2]} < MME9[-1]: {mme.iloc[-1]}")
        logging.debug(f"    Mínima do candle [-2] ROMPIDA pelo candle [-1]")
        logging.debug(f"    Candle [-1] (c1): low={c1['low']} < candle [-2]: (c2) low={c2['low']}")
        logging.debug(f"    ----------------------------------------------")

    # === VENDA
    if (tendencia_suave(mme, tipo='baixa', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE) and
        mme.iloc[-3] < mme.iloc[-2] and
        mme.iloc[-2] > mme.iloc[-1]):

        logging.debug(f"[{ativo}] 🔼 Tendência de baixa e S confirmados")
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]

        if (c2['high'] > c1['high']):

            logging.debug(f"    Candle [-1] (c1): close={c1['close']}, low={c1['low']}, high={c1['high']}")
            logging.debug(f"    Candle [-2] (c2): close={c2['close']}, low={c2['low']}")
            logging.debug(f"    Candle [-3] (c3): close={c3['close']}, low={c3['low']}")
            logging.debug(f"    Candle [0] (zero): low={candle_zero['low']}, high={candle_zero['high']}")

            logging.debug(f"    ----------------------------------------------")
            logging.debug(f"    Curva S estabelecida")
            logging.debug(f"    MME9[-3]: {mme.iloc[-3]} < MME9[-2]: {mme.iloc[-2]} > MME9[-1]: {mme.iloc[-1]}")
            logging.debug(f"    Máxima do candle [-2] NÃO rompida pelo candle [-1]")
            logging.debug(f"    Candle [-1] (c1): high={c1['high']} > candle [-2]: (c2) high={c2['high']}")
            logging.debug(f"    ----------------------------------------------")

            gatilho = c1['low']
            if candle_zero['low'] < c1['low']:
                logging.debug(f"✅ DISPARAR VENDA 9.4")
                logging.debug(f"    Gatilho = low[-1] = {gatilho:.7f}")

                return {
                'status': 'DISPARAR VENDA 9.4',
                'gatilho': gatilho,
                'tipo': 'venda',
                'coluna': 'low'
                }
                
            logging.debug(f"✅ ARMAR VENDA 9.4")
            logging.debug(f"    MME9: -3 = {mme.iloc[-3]:.7f}, -2 = {mme.iloc[-2]:.7f}, -1 = {mme.iloc[-1]:.7f}")
            logging.debug(f"    Gatilho = high[-1] = {gatilho:.7f}")

            return {
            'status': 'ARMAR VENDA 9.4',
            'gatilho': gatilho,
            'tipo': 'venda',
            'coluna': 'low'
            }

        logging.debug(f"    ----------------------------------------------")
        logging.debug(f"    Curva S estabelecida")
        logging.debug(f"    MME9[-3]: {mme.iloc[-3]} < MME9[-2]: {mme.iloc[-2]} > MME9[-1]: {mme.iloc[-1]}")
        logging.debug(f"    Máxima do candle [-2] ROMPIDA pelo candle [-1]")
        logging.debug(f"    Candle [-1] (c1): high={c1['high']} < candle [-2]: (c2) high={c2['high']}")
        logging.debug(f"    ----------------------------------------------")

    return None

# === Setups de Alexandre Wolwacz ===
# === SETUP PC (Ponto Contínuo) ===
def setup_pc(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0], ainda em formação
    df = df.iloc[:-1]          # Remove o candle [0]; agora o último é o [-1]

    df['MMA21'] = df['close'].rolling(window=21).mean()
    mma = df['MMA21']

    tendencia_alta = tendencia_suave(mma, tipo='alta', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE)
    tendencia_baixa = tendencia_suave(mma, tipo='baixa', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE)

    logging.debug(f"    ANALISANDO PC")         

    logging.debug(f"🔁 MMA21 subindo? {tendencia_alta}")
    logging.debug(f"🔁 MMA21 descendo? {tendencia_baixa}")

    # === COMPRA ===
    if tendencia_alta:
        candle_menos1 = df.iloc[-1]
        media_menos1 = mma.iloc[-1]
        logging.debug(f"    Candle[-1]: open={candle_menos1['open']}, high={candle_menos1['high']}, low={candle_menos1['low']}, close={candle_menos1['close']}, MMA21={media_menos1}")

        if candle_menos1['close'] < candle_menos1['open'] and candle_menos1['open'] > media_menos1 and candle_menos1['low'] <= media_menos1: # Candle de Baixa e Abre acima da MMA21 e Mínima de [-1] Rompe a MMA21
            if candle_zero['high'] > candle_menos1['high']:
                logging.debug(f"    Disparo direto no candle[0] rompendo o gatilho do candle[-1]")
                return {
                    'status': 'DISPARAR COMPRA PC',
                    'gatilho': candle_menos1['high'],
                    'tipo': 'compra',
                    'coluna': 'high',
                    'debug_origem': 'COMPRA-DISPARO-DIRETO'
                }
            logging.debug(f"    Candle [-1] de baixa tocando MMA21. Armar gatilho: high={candle_menos1['high']}")
            return {
                'status': 'ARMAR COMPRA PC',
                'gatilho': candle_menos1['high'],
                'tipo': 'compra',
                'coluna': 'high',
                'debug_origem': 'COMPRA-ARMAR'
            }
        logging.debug("." * 92)
        logging.debug(f"    Iniciando avaliação de escorregamento")

        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]
        if candle_m2['high'] > candle_m1['high']:
            for i in range(-2, -7, -1):
                if abs(i) > len(df):
                    break
                candle = df.iloc[i]
                media = mma.iloc[i]
                candle_i_menos1 = df.iloc[i-1]

                logging.debug(f"🔍 i={i} | open={candle['open']} close={candle['close']} low={candle['low']} high={candle['high']} MMA21={media}")
                if candle['close'] < candle['open'] and candle['open'] > media and candle['low'] <= media:
                    if candle_zero['high'] > candle_menos1['high']:
                        logging.debug(f"    Gatilho escorregado do candle {i}. Disparo confirmado no candle[0]")
                        return {
                            'status': 'DISPARAR COMPRA PC',
                            'gatilho': candle_menos1['high'],
                            'tipo': 'compra',
                            'coluna': 'high',
                            'debug_origem': f'COMPRA-DISPARO-i{i}'
                        }
                
                    logging.debug(f"    Gatilho escorregado do candle {i}. Ainda aguardando disparo.")
                    return {
                        'status': 'ARMAR COMPRA PC',
                        'gatilho': candle_menos1['high'],
                        'tipo': 'compra',
                        'coluna': 'high',
                        'debug_origem': f'COMPRA-ARMAR-i{i}'
                    }

                if candle_i_menos1['high'] < candle['high']:
                    logging.debug(f"    Gatilho NÃO escorregado, => máxima do candle[{i}]={candle['high']} é MAIOR do que a máxima do candle[{i-1}]={candle_i_menos1['high']}.")
                    return None
        
        logging.debug(f"    Gatilho NÃO escorregado => máxima do candle[-1]={candle_m1['high']} é MAIOR que máxima do candle[-2]={candle_m2['high']}.")
            
    # === VENDA ===
    if tendencia_baixa:
        candle_menos1 = df.iloc[-1]
        media_menos1 = mma.iloc[-1]
        logging.debug(f"    Candle[-1]: open={candle_menos1['open']}, high={candle_menos1['high']}, low={candle_menos1['low']}, close={candle_menos1['close']}, MMA21={media_menos1}")

        if candle_menos1['close'] > candle_menos1['open'] and candle_menos1['open'] < media_menos1 and candle_menos1['high'] >= media_menos1:
            if candle_zero['low'] < candle_menos1['low'] and candle_zero['close'] < candle_zero['open']:
                logging.debug(f"    Disparo direto no candle[0] rompendo o gatilho do candle[-1]")
                return {
                    'status': 'DISPARAR VENDA PC',
                    'gatilho': candle_menos1['low'],
                    'tipo': 'venda',
                    'coluna': 'low',
                    'debug_origem': 'VENDA-DISPARO-DIRETO'
                }
            logging.debug(f"    Candle[-1] de alta tocando MMA21. Armar gatilho: low={candle_menos1['low']}")
            return {
                'status': 'ARMAR VENDA PC',
                'gatilho': candle_menos1['low'],
                'tipo': 'venda',
                'coluna': 'low',
                'debug_origem': 'VENDA-ARMAR'
            }

        logging.debug("." * 92)
        logging.debug(f"    Iniciando avaliação de escorregamento")

        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]

        if candle_m2['low'] < candle_m1['low']:
            for i in range(-2, -7, -1):
                if abs(i) > len(df):
                    break
                candle = df.iloc[i]
                media = mma.iloc[i]
                candle_i_menos1 = df.iloc[i-1]

                logging.debug(f"🔍 i={i} | open={candle['open']} close={candle['close']} low={candle['low']} high={candle['high']} MMA21={media}")
                if candle['close'] > candle['open'] and candle['open'] < media and candle['high'] >= media:
                    if candle_zero['low'] < candle_menos1['low']:
                        logging.debug(f"    Gatilho escorregado do candle {i}. Disparo confirmado no candle[0]")
                        return {
                            'status': 'DISPARAR VENDA PC',
                            'gatilho': candle_menos1['low'],
                            'tipo': 'venda',
                            'coluna': 'low',
                            'debug_origem': f'VENDA-DISPARO-i{i}'
                        }
                        logging.debug(f"    Gatilho escorregado do candle {i}. Ainda aguardando disparo.")
                    return {
                        'status': 'ARMAR VENDA PC',
                        'gatilho': candle_menos1['low'],
                        'tipo': 'venda',
                        'coluna': 'low',
                        'debug_origem': f'VENDA-ARMAR-i{i}'
                    }

                if candle_i_menos1['low'] > candle['low']:
                    logging.debug(f"    Gatilho NÃO escorregado, => mínima do candle[{i}]={candle['low']} é MENOR do que a mínima do candle[{i-1}]={candle_i_menos1['low']}.")
                    return None

        logging.debug(f"    NÃO escorregado => mínima do candle[-1]={candle_m1['low']} é MENOR que mínima do candle[-2]={candle_m2['low']}.")

    return None

#Incluindo para cálculo de SL e TP otimizados
SETUPS = [setup_9_1, setup_9_2, setup_9_3, setup_9_4, setup_pc]

# === EXECUÇÃO PRINCIPAL ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bybit Setups — Scan & Optimization")
    sub = parser.add_subparsers(dest="mode", required=False)

    # (1) SCAN (gera ativos_opt.xlsx com SL/TP + ATR_PERIOD + PARAM_ORIGEM)
    scan_p = sub.add_parser("scan", help="Rodar scanner e exportar planilha")
    scan_p.add_argument("--auto-optimize", action="store_true",
                        help="Se faltar param otimizado, roda otimização on-the-fly para aquele par/timeframe")
    scan_p.add_argument("--objective", choices=["net","mar","sharpe","pf"], default="mar")
    scan_p.add_argument("--optuna", action="store_true")
    scan_p.add_argument("--limit", type=int, default=1000)
    scan_p.add_argument("--modo-universo", choices=["AUTO", "HIBRIDO", "MANUAL"], default=None,
                        help="Sobrescreve MODO_UNIVERSO da planilha CONFIG_UNIVERSO")
    scan_p.add_argument("--sem-contexto-profundo", action="store_true",
                        help="Desativa orderbook, OI histórico, funding histórico e long/short ratio")

    # (2) OPTIMIZE-FROM-EXCEL (gera JSONs para todos os pares/timeframes da planilha)
    optall_p = sub.add_parser("optimize-from-excel", help="Otimiza todos os pares/timeframes listados em ativos.xlsx")
    optall_p.add_argument("--objective", choices=["net","mar","sharpe","pf"], default="mar")
    optall_p.add_argument("--optuna", action="store_true")
    optall_p.add_argument("--limit", type=int, default=1000)
    optall_p.add_argument("--mercado", default="linear")

    # (3) OPTIMIZE-FROM-UNIVERSE (gera JSONs para o universo dinâmico filtrado pela Bybit)
    optuni_p = sub.add_parser("optimize-from-universe", help="Otimiza os pares/timeframes aprovados pelo universo dinâmico da Bybit")
    optuni_p.add_argument("--objective", choices=["net","mar","sharpe","pf"], default="mar")
    optuni_p.add_argument("--optuna", action="store_true")
    optuni_p.add_argument("--limit", type=int, default=1000)
    optuni_p.add_argument("--modo-universo", choices=["AUTO", "HIBRIDO", "MANUAL"], default=None,
                          help="Sobrescreve MODO_UNIVERSO da planilha CONFIG_UNIVERSO")
    optuni_p.add_argument("--mercado", default="linear")

    args = parser.parse_args()

    if args.mode == "optimize-from-excel":
        code = cli_optimize_from_excel(objective=args.objective, use_optuna=args.optuna,
                                       limit=args.limit, mercado_default=args.mercado)
        raise SystemExit(code)

    if args.mode == "optimize-from-universe":
        code = cli_optimize_from_universe(objective=args.objective, use_optuna=args.optuna,
                                          limit=args.limit, modo_universo=args.modo_universo,
                                          mercado_default=args.mercado)
        raise SystemExit(code)

    # ===== Normaliza args para o modo padrão (sem subcomando / scan implícito) =====
    objective     = getattr(args, "objective", "mar")
    auto_optimize = getattr(args, "auto_optimize", False)
    optuna_flag   = getattr(args, "optuna", False)
    limit         = getattr(args, "limit", 1000)
    modo_universo_cli = getattr(args, "modo_universo", None)
    sem_contexto_profundo = getattr(args, "sem_contexto_profundo", False)

    # ====== MODO PADRÃO: SCAN (se args.mode for None ou 'scan') ======
    if not dentro_do_horario():
        logging.info("Fora do horário configurado. Encerrando execução.")
        exit()

    try:
        ativos_df = pd.read_excel(ARQUIVO_EXCEL)
#### >>Reduzir o ativos_df a uma amostra para fim de testes
#        ativos_df = ativos_df.head(30)
#### <<Reduzir o ativos_df a uma amostra para fim de testes

    except Exception as e:
        logging.error(f"Erro ao carregar o arquivo Excel: {e}")
        exit()

    # Remove colunas duplicadas (mantém apenas a primeira ocorrência)
    ativos_df = ativos_df.loc[:, ~ativos_df.columns.duplicated(keep='first')]

    logging.info(f"Execução iniciada em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

#### >>Medição de Tempo do Script
    import time
    tempo_inicio_total = time.time()
    tempo_download = 0
    tempo_setups = 0
    tempo_excel = 0
    tempo_params = 0
    tempo_upload = 0
    tempo_loop_total = 0
#### <<Medição de Tempo do Script

    logging.debug('*' * 92)

    # === CONTEXTO DE MERCADO / UNIVERSO AUTOMÁTICO ===
    context_cfg = ler_config_contexto(ARQUIVO_EXCEL)
    if modo_universo_cli:
        context_cfg.modo_universo = modo_universo_cli
    if sem_contexto_profundo:
        context_cfg.capturar_orderbook = False
        context_cfg.capturar_oi_historico = False
        context_cfg.capturar_funding_historico = False
        context_cfg.capturar_long_short = False

    bybit_client = BybitPublicV5(sleep_s=context_cfg.api_sleep_s)
    try:
        ativos_df, UNIVERSO_BYBIT_DF, ticker_map = montar_universo_para_scan(
            ARQUIVO_EXCEL, ativos_df, context_cfg, bybit_client
        )
    except Exception as exc:
        logging.warning(f"[CTX] Falha ao montar universo automático. Usando lista manual. Erro: {exc}")
        ticker_map = {}
        UNIVERSO_BYBIT_DF = pd.DataFrame()
        if "Mercado" not in ativos_df.columns:
            ativos_df["Mercado"] = context_cfg.category

    # Remove colunas duplicadas novamente após eventual montagem automática
    ativos_df = ativos_df.loc[:, ~ativos_df.columns.duplicated(keep='first')]

    # Criando dicionário candles_dict em dois passos:
    # 1) download/enriquecimento técnico de todos os pares elegíveis
    # 2) cálculo de força relativa e detecção dos setups
    candles_dict = {}
    contexto_base_por_par = {}
    if not ativos_df.empty:
        for _, r in ativos_df.drop_duplicates(subset=["Par"]).iterrows():
            contexto_base_por_par[str(r["Par"]).upper()] = r.to_dict()

    limit_candles_scan = int(min(1000, max(PERIODOS_MINIMO + 10, context_cfg.kline_limit, context_cfg.percentile_lookback + 50)))

    for idx, ativo in ativos_df.iterrows():
        t_loop = time.time()

        par = str(ativo['Par']).upper().strip()
        timeframe = normalize_timeframe(ativo['Timeframe'])
        mercado = str(ativo.get('Mercado', context_cfg.category)).lower()

        try:
            t0 = time.time()
            df = obter_candles(par=par, interval=timeframe, limit=limit_candles_scan, mercado=mercado)
            tempo_download += time.time() - t0

            if df.empty or len(df) < PERIODOS_MINIMO:
                logging.warning(f"Poucos dados para {par} ({timeframe}min)")
                ativos_df.at[idx, 'Último Setup Identificado'] = "Sem dados"
                continue

            # Calcula médias móveis com candle [0]
            df['MME9'] = df['close'].ewm(span=9).mean()
            df['MMA21'] = df['close'].rolling(window=21).mean()

            # Enriquecimento de regime: ATR%, ADX, CHOP, Efficiency, BB Width etc.
            df = enriquecer_candles_contexto(df, context_cfg)

            # Armazena para gráficos e força relativa
            candles_dict[par] = df.reset_index(drop=True)

        except Exception as e:
            logging.error(f"Erro ao obter dados de {par}: {e}")
            ativos_df.at[idx, 'Último Setup Identificado'] = f"Erro: {e}"
            tempo_loop_total += time.time() - t_loop
            continue

        # Garante params otimizados ou padrão
        t_params = time.time()
        params = garantir_params(
            par,
            timeframe,
            df_hist=df.set_index('timestamp') if 'timestamp' in df.columns else None,
            objective=objective,
            auto_optimize=auto_optimize,
            use_optuna=optuna_flag,
            limit=limit
        )
        tempo_params += time.time() - t_params

        ativos_df.at[idx, '_ATR_PERIOD'] = params['atr_period']
        ativos_df.at[idx, '_K_SL'] = params['k_sl']
        ativos_df.at[idx, '_K_TP'] = params['k_tp']
        ativos_df.at[idx, '_PARAM_ORIGEM'] = params['origem']
        tempo_loop_total += time.time() - t_loop

    FORCA_RELATIVA_DF = calcular_forca_relativa(candles_dict, context_cfg)
    rs_map = {}
    if FORCA_RELATIVA_DF is not None and not FORCA_RELATIVA_DF.empty:
        rs_map = {str(r["Par"]).upper(): r for _, r in FORCA_RELATIVA_DF.iterrows()}

    # Segundo passo: setups + score/contexto
    for idx, ativo in ativos_df.iterrows():
        t_loop = time.time()

        par = str(ativo['Par']).upper().strip()
        timeframe = normalize_timeframe(ativo['Timeframe'])
        mercado = str(ativo.get('Mercado', context_cfg.category)).lower()
        df = candles_dict.get(par)

        if df is None or df.empty or len(df) < PERIODOS_MINIMO:
            if str(ativos_df.at[idx, 'Último Setup Identificado']) in ["", "nan"]:
                ativos_df.at[idx, 'Último Setup Identificado'] = "Sem dados"
            tempo_loop_total += time.time() - t_loop
            continue

        logging.debug(f"\n🔍 Verificando {par} ({timeframe} min - {mercado})")
        logging.debug("  🔍 Verificando integridade dos candles [-10] a [0]:")

        for i in range(-10, 0):
            candle = df.iloc[i]
            mme9 = df['MME9'].iloc[i] if 'MME9' in df.columns else None
            mma21 = df['MMA21'].iloc[i] if 'MMA21' in df.columns else None

            logging.debug(
                f"     idx={i:>2} | time={candle['timestamp']} | "
                f"o={candle['open']:.7f} h={candle['high']:.7f} l={candle['low']:.7f} c={candle['close']:.7f} "
                f"| m9={mme9:.7f} | m21={mma21:.7f}"
            )
            dados_integridade.append({
                'Par': par,
                'Timeframe': timeframe,
                'Mercado': mercado,
                'idx': i,
                'timestamp': candle['timestamp'],
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close'],
                'MME9': mme9,
                'MMA21': mma21,
                'Setup Identificado': ''
            })
        logging.debug("+" * 92)

        resultado_final = None
        status_do_setup = "Nenhum"
        swing_abs_setup = None
        swing_pct_setup = None

        t1 = time.time()
        for func in [setup_9_1, setup_9_2, setup_9_3, setup_9_4, setup_pc]:
            resultado = func(df, ativo=par)
            logging.debug(f"  ▶️Resultado de {func.__name__}: {resultado}")
            logging.debug("-" * 92)

            # BLOCO DE DISPARO
            if resultado and isinstance(resultado, dict) and resultado['status'].startswith("ARMAR"):
                tipo = str(resultado['tipo']).lower()
                coluna = str(resultado['coluna']).lower()
                gatilho = resultado['gatilho']
                # Mantém a lógica atual: candle [0] é o último candle da série.
                preco_atual = df[coluna].iloc[-1]

                if DEBUG_MODE:
                    logging.debug(f"  📍 candle[0] | Preço atual ({coluna}) = {preco_atual:.7f}")
                    logging.debug(f"  🔎 Gatilho atual = {gatilho:.7f}")

                rompeu = (tipo == 'compra' and preco_atual > gatilho) or (tipo == 'venda' and preco_atual < gatilho)

                if rompeu:
                    logging.debug("  ✅ DISPARO no candle [0] confirmado!")
                    resultado['status'] = resultado['status'].replace("ARMAR", "DISPARAR")

            # CONSTRÓI O RESULTADO FINAL
            if resultado is not None and isinstance(resultado, dict):
                precisao = f".{CASAS_DECIMAIS_GATILHO}f"
                gatilho_formatado = format(resultado['gatilho'], precisao)
                candle_m1 = df.iloc[-2]
                high_m1 = candle_m1['high']
                low_m1 = candle_m1['low']

                partes_status = resultado['status'].split()
                nome_setup = partes_status[2] if len(partes_status) >= 3 else ''
                direcao_swing = partes_status[1].upper() if len(partes_status) >= 2 else ''

                idx_inicio_swing, idx_fim_swing = obter_intervalo_swing_por_setup(
                    df,
                    nome_setup=nome_setup,
                    direcao=direcao_swing
                )

                swing_abs_setup = calcular_swing_absoluto_intervalo(
                    df,
                    idx_inicio=idx_inicio_swing,
                    idx_fim=idx_fim_swing,
                    direcao=direcao_swing
                )

                swing_pct_setup = calcular_swing_percentual_intervalo(
                    df,
                    idx_inicio=idx_inicio_swing,
                    idx_fim=idx_fim_swing,
                    direcao=direcao_swing
                )

                # Estima TP/SL para gravar TP_ESTIMADO_PCT/SL_ESTIMADO_PCT no score
                params = {
                    "atr_period": int(float(ativo.get('_ATR_PERIOD', ATR_PERIODO_SLTP) or ATR_PERIODO_SLTP)),
                    "k_sl": float(ativo.get('_K_SL', K_SL_PADRAO) or K_SL_PADRAO),
                    "k_tp": float(ativo.get('_K_TP', K_TP_PADRAO) or K_TP_PADRAO)
                }
                atr_series = compute_atr(df, period=params["atr_period"], method="wilder")
                atr_m1 = float(atr_series.iloc[-2]) if len(atr_series) >= 2 else None
                sl_est = tp_est = None
                if atr_m1 is not None:
                    if direcao_swing == 'COMPRA':
                        sl_est = float(resultado['gatilho']) - params["k_sl"] * atr_m1
                        tp_est = float(resultado['gatilho']) + params["k_tp"] * atr_m1
                    elif direcao_swing == 'VENDA':
                        sl_est = float(resultado['gatilho']) + params["k_sl"] * atr_m1
                        tp_est = float(resultado['gatilho']) - params["k_tp"] * atr_m1

                # Contexto profundo apenas para ativos com setup, para não pesar o loop.
                ticker_row = ticker_map.get(par, {})
                contexto = dict(contexto_base_por_par.get(par, {}))
                try:
                    contexto.update(capturar_contexto_profundo(bybit_client, par, context_cfg, ticker_row=ticker_row))
                except Exception as exc:
                    logging.warning(f"[CTX] Contexto profundo falhou para {par}: {exc}")

                aplicar_contexto_na_linha(
                    ativos_df, idx, df, nome_setup, direcao_swing, contexto, rs_map.get(par),
                    context_cfg, gatilho=resultado['gatilho'], tp=tp_est, sl=sl_est, swing_pct=swing_pct_setup
                )

                resultado_final = (
                    f"{resultado['status']} (gatilho: {gatilho_formatado} | "
                    f"h: {high_m1:.7f} | l: {low_m1:.7f}) "
                    f"({df['timestamp'].iloc[-1].strftime('%d/%m/%Y %H:%M:%S')})"
                )
                status_do_setup = resultado['status']
                break

        tempo_setups += time.time() - t1

        ativos_df.at[idx, 'SWING_ABS'] = swing_abs_setup
        ativos_df.at[idx, 'SWING_PCT'] = swing_pct_setup

        if len(dados_integridade) >= 6:
            dados_integridade[-6:][-1]['Setup Identificado'] = status_do_setup

        if resultado_final is None:
            resultado_final = f"Nenhum ({df['timestamp'].iloc[-1].strftime('%d/%m/%Y %H:%M:%S')})"

        logging.debug(f" >>>> {par}: {resultado_final}")
        ativos_df.at[idx, 'Último Setup Identificado'] = resultado_final

        if status_do_setup.startswith("ARMAR") or status_do_setup.startswith("DISPARAR"):
            mensagem = f"🚨 {par} | {resultado_final} | Score={ativos_df.at[idx, 'SCORE_TOTAL'] if 'SCORE_TOTAL' in ativos_df.columns else ''}"
            enviar_alerta_telegram(mensagem)

        logging.debug("=" * 92)
        tempo_loop_total += time.time() - t_loop

    # === SALVAMENTO E ENCERRAMENTO FINAL ===
    # Salva a planilha Excel original
    ARQUIVO_EXCEL_OPT = "ativos_opt_hr.xlsx"
    try:
#### >>Medição de Tempo do Script
        t_excel = time.time()
#### <<Medição de Tempo do Script

        gerar_excel_com_graficos(
            candles_dict,
            ativos_df,
            nome_arquivo=ARQUIVO_EXCEL_OPT,
            universo_bybit_df=UNIVERSO_BYBIT_DF,
            config_context=context_cfg,
            forca_relativa_df=FORCA_RELATIVA_DF,
        )

#### >>Medição de Tempo do Script
        tempo_excel = time.time() - t_excel
#### <<Medição de Tempo do Script

        logging.info(f"✅ Arquivo '{ARQUIVO_EXCEL_OPT}' salvo com sucesso com dados e gráficos.")
    except Exception as e:
        logging.error(f"❌ Erro ao gerar o arquivo '{ARQUIVO_EXCEL_OPT}': {e}")

    # Exporta os dados de integridade para CSV
    try:
        df_csv = pd.DataFrame(dados_integridade)
        df_csv.to_csv("dados_candles.csv", index=False)

        logging.debug("📁 Arquivo 'dados_candles.csv' salvo com os candles mais recentes.")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar CSV: {e}")

    # Chamada da função para envio ao Google Drive no Hostinger
    try:

#### >>Medição de Tempo do Script
        t_upload = time.time()
#### <<Medição de Tempo do Script

        upload_file_to_drive(ARQUIVO_EXCEL_OPT, os.environ.get("GDRIVE_FOLDER_ID"))

#### >>Medição de Tempo do Script
        tempo_upload = time.time() - t_upload
#### <<Medição de Tempo do Script

    except Exception as e:
        logging.error(f"❌ Erro ao enviar '{ARQUIVO_EXCEL_OPT}' para o Google Drive: {e}")

    logging.debug("-" * 60)

#### >>Medição de Tempo do Script
    tempo_total = time.time() - tempo_inicio_total
    logging.info(f"⏱ Tempo total: {tempo_total/60:.2f} minutos")
    logging.info(f"📥 Tempo download: {tempo_download/60:.2f} minutos")
    logging.info(f"🧠 Tempo setups: {tempo_setups/60:.2f} minutos")
    logging.info(f"📊 Tempo Excel: {tempo_excel/60:.2f} minutos")
    logging.info(f"⚙️ Tempo params/JSON: {tempo_params/60:.2f} minutos")
    logging.info(f"☁️ Tempo upload Drive: {tempo_upload/60:.2f} minutos")
    logging.info(f"🔁 Tempo loop total ativos: {tempo_loop_total/60:.2f} minutos")
#### <<Medição de Tempo do Script

    logging.info(f"🏁 Execução finalizada em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
