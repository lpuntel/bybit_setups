"""
Utilidades comuns para Bybit Spot Grid.
Somente dados públicos de mercado. Não envia ordens.
"""
from __future__ import annotations
import math
import requests
import pandas as pd

BASE_URL = "https://api.bybit.com"
CATEGORY = "spot"
_SESSION = requests.Session()

SUPPORTED_INTERVALS = {"1","3","5","15","30","60","120","240","360","720","D","W","M"}


def normalize_timeframe(tf) -> str:
    # Normaliza e valida os intervalos oficiais aceitos pela Bybit V5.
    s = str(tf).strip().upper()
    if s in {"D", "1D", "1440", "1444"}:
        out = "D"
    elif s in {"W", "1W"}:
        out = "W"
    elif s in {"M", "1M"}:
        out = "M"
    else:
        try:
            out = str(int(float(s)))
        except Exception:
            out = s

    if out not in SUPPORTED_INTERVALS:
        validos = "1,3,5,15,30,60,120,240,360,720,D,W,M"
        raise ValueError(
            f"Timeframe '{tf}' não é suportado pela Bybit V5. Use um de: {validos}"
        )
    return out

def _get(path: str, params: dict | None = None) -> dict:
    r = _SESSION.get(BASE_URL + path, params=params or {}, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit API: {data.get('retCode')} - {data.get('retMsg')}")
    return data


def get_kline(
    symbol: str,
    interval,
    limit: int = 300,
    start: int | None = None,
    end: int | None = None,
) -> pd.DataFrame:
    params = {
        "category": CATEGORY,
        "symbol": symbol.upper(),
        "interval": normalize_timeframe(interval),
        "limit": min(int(limit), 1000),
    }
    if start is not None:
        params["start"] = int(start)
    if end is not None:
        params["end"] = int(end)

    data = _get("/v5/market/kline", params)
    rows = data["result"]["list"]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=[
        "startTime", "open", "high", "low", "close", "volume", "turnover"
    ])
    for c in ["open", "high", "low", "close", "volume", "turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["timestamp"] = pd.to_datetime(pd.to_numeric(df["startTime"]), unit="ms", utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)

def get_ticker(symbol: str) -> dict:
    rows = _get("/v5/market/tickers", {
        "category": CATEGORY, "symbol": symbol.upper()
    })["result"]["list"]
    return rows[0] if rows else {}


def get_orderbook(symbol: str, limit: int = 50) -> dict:
    return _get("/v5/market/orderbook", {
        "category": CATEGORY, "symbol": symbol.upper(), "limit": int(limit)
    })["result"]


def get_spot_instruments() -> list[dict]:
    # Spot não usa paginação/cursor neste endpoint.
    return _get("/v5/market/instruments-info", {"category": CATEGORY})["result"]["list"]


def get_spot_tickers() -> list[dict]:
    return _get("/v5/market/tickers", {"category": CATEGORY})["result"]["list"]


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev = df["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def market_microstructure(symbol: str, depth_levels: int = 50) -> dict:
    t = get_ticker(symbol)
    ob = get_orderbook(symbol, max(20, min(int(depth_levels), 200)))
    bid = float(t.get("bid1Price") or 0)
    ask = float(t.get("ask1Price") or 0)
    last = float(t.get("lastPrice") or 0)
    mid = (bid + ask) / 2 if bid and ask else last
    spread_pct = ((ask - bid) / mid * 100) if mid else None  # pontos percentuais

    bid_cut = last * 0.99 if last else 0
    ask_cut = last * 1.01 if last else float("inf")
    bids = ob.get("b", [])
    asks = ob.get("a", [])
    depth_bid = sum(float(p) * float(q) for p, q in bids if float(p) >= bid_cut)
    depth_ask = sum(float(p) * float(q) for p, q in asks if float(p) <= ask_cut)
    depth_min = min(depth_bid, depth_ask) if depth_bid and depth_ask else 0.0

    return {
        "last": last,
        "turnover24h": float(t.get("turnover24h") or 0),
        "volume24h": float(t.get("volume24h") or 0),
        "spread_pct": spread_pct,
        "depth_bid_quote": depth_bid,
        "depth_ask_quote": depth_ask,
        "depth_min_quote": depth_min,
    }


def spot_grid_parameters(
    entry: float,
    atr: float,
    slope_pct: float | None = None,
    fee_side_pct: float = 0.10,
    min_net_grid_pct: float = 0.15,
    range_atr_down: float = 2.0,
    range_atr_up: float = 3.0,
    sl_buffer_atr: float = 0.7,
    tp_buffer_atr: float = 0.7,
    min_grids: int = 2,
    max_grids: int = 200,
    trailing_slope_pct: float = 0.75,
) -> dict:
    if entry <= 0 or atr <= 0:
        return {}

    atr_pct = atr / entry * 100.0
    lower = max(entry * 0.01, entry - range_atr_down * atr)
    upper = entry + range_atr_up * atr
    width_pct = (upper - lower) / entry * 100.0

    # Espaçamento precisa cobrir fee de compra + fee de venda + ganho líquido mínimo.
    min_spacing = max(0.01, 2 * fee_side_pct + min_net_grid_pct)
    grids = math.floor(width_pct / min_spacing)
    grids = max(int(min_grids), min(int(max_grids), grids))
    spacing_pct = width_pct / grids

    slope = float(slope_pct or 0)
    regime = (
        "ALTA" if slope > trailing_slope_pct
        else ("BAIXA" if slope < -trailing_slope_pct else "LATERAL")
    )
    trailing_up = regime == "ALTA"

    return {
        "ENTRY": entry,
        "LOWER": lower,
        "UPPER": upper,
        "GRIDS": grids,
        "GRID_SPACING_PCT": spacing_pct,
        "GRID_NET_EST_PCT": max(0.0, spacing_pct - 2 * fee_side_pct),
        "SL": max(entry * 0.001, lower - sl_buffer_atr * atr),
        "TP": upper + tp_buffer_atr * atr,
        "TS_RETRACAO_PCT": max(3.0, min(20.0, 2 * atr_pct)),
        "TRAILING_UP": trailing_up,
        "ESTRATEGIA_GRID": "TRAILING_UP" if trailing_up else "NORMAL",
        "REGIME_SPOT": regime,
        "ATR_PCT_SPOT": atr_pct,
    }
