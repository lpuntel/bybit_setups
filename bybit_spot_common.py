"""
Utilidades comuns para Bybit Spot Grid.
Somente dados públicos de mercado. Não envia ordens.
"""
from __future__ import annotations
import math, requests, pandas as pd

BASE_URL = "https://api.bybit.com"
CATEGORY = "spot"

def normalize_timeframe(tf) -> str:
    s = str(tf).strip().upper()
    if s in {"D","1D","1440","1444"}:
        return "D"
    try:
        return str(int(float(s)))
    except Exception:
        return s

def _get(path: str, params: dict) -> dict:
    r = requests.get(BASE_URL + path, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit API: {data.get('retCode')} - {data.get('retMsg')}")
    return data

def get_kline(symbol: str, interval, limit: int = 300) -> pd.DataFrame:
    data = _get("/v5/market/kline", {
        "category": CATEGORY, "symbol": symbol.upper(),
        "interval": normalize_timeframe(interval), "limit": min(int(limit),1000)
    })
    rows = data["result"]["list"]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["startTime","open","high","low","close","volume","turnover"])
    for c in ["open","high","low","close","volume","turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["timestamp"] = pd.to_datetime(pd.to_numeric(df["startTime"]), unit="ms", utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)

def get_ticker(symbol: str) -> dict:
    x = _get("/v5/market/tickers", {"category":CATEGORY,"symbol":symbol.upper()})["result"]["list"]
    return x[0] if x else {}

def get_orderbook(symbol: str, limit: int = 50) -> dict:
    return _get("/v5/market/orderbook", {"category":CATEGORY,"symbol":symbol.upper(),"limit":int(limit)})["result"]

def get_spot_instruments() -> list[dict]:
    return _get("/v5/market/instruments-info", {"category":CATEGORY,"limit":1000})["result"]["list"]

def get_spot_tickers() -> list[dict]:
    return _get("/v5/market/tickers", {"category":CATEGORY})["result"]["list"]

def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev = df["close"].shift(1)
    tr = pd.concat([(df["high"]-df["low"]).abs(),
                    (df["high"]-prev).abs(),
                    (df["low"]-prev).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def market_microstructure(symbol: str, depth_levels: int = 20) -> dict:
    t = get_ticker(symbol)
    ob = get_orderbook(symbol, max(20,depth_levels))
    bid = float(t.get("bid1Price") or 0)
    ask = float(t.get("ask1Price") or 0)
    last = float(t.get("lastPrice") or 0)
    mid = (bid+ask)/2 if bid and ask else last
    spread_pct = ((ask-bid)/mid*100) if mid else None
    bids, asks = ob.get("b",[])[:depth_levels], ob.get("a",[])[:depth_levels]
    depth_quote = sum(float(p)*float(q) for p,q in bids) + sum(float(p)*float(q) for p,q in asks)
    return {"last":last,
            "turnover24h":float(t.get("turnover24h") or 0),
            "spread_pct":spread_pct,
            "depth_quote":depth_quote}

def spot_grid_parameters(entry: float, atr: float, slope_pct: float|None=None,
                         fee_side_pct: float=0.10, min_net_grid_pct: float=0.15,
                         range_atr_down: float=2.0, range_atr_up: float=3.0,
                         sl_buffer_atr: float=0.7, tp_buffer_atr: float=0.7,
                         min_grids: int=5, max_grids: int=50) -> dict:
    if entry <= 0 or atr <= 0:
        return {}
    atr_pct = atr/entry*100
    lower = max(entry*0.01, entry-range_atr_down*atr)
    upper = entry+range_atr_up*atr
    width_pct = (upper-lower)/entry*100
    min_spacing = max(0.01, 2*fee_side_pct + min_net_grid_pct)
    grids = max(min_grids, min(max_grids, math.floor(width_pct/min_spacing)))
    spacing_pct = width_pct/grids
    slope = float(slope_pct or 0)
    regime = "ALTA" if slope > 0.75 else ("BAIXA" if slope < -0.75 else "LATERAL")
    trailing_up = regime == "ALTA"
    return {
        "ENTRY":entry, "LOWER":lower, "UPPER":upper, "GRIDS":grids,
        "GRID_SPACING_PCT":spacing_pct,
        "GRID_NET_EST_PCT":max(0,spacing_pct-2*fee_side_pct),
        "SL":max(entry*0.001, lower-sl_buffer_atr*atr),
        "TP":upper+tp_buffer_atr*atr,
        "TS_RETRACAO_PCT":max(3.0,min(20.0,2*atr_pct)),
        "TRAILING_UP":trailing_up,
        "ESTRATEGIA_GRID":"TRAILING_UP" if trailing_up else "NORMAL",
        "REGIME":regime, "ATR_PCT":atr_pct
    }
