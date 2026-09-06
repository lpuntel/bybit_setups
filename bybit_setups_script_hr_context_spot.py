"""
Bybit Spot Grid - scanner contextual.

Base conceitual: bybit_setups_script_hr_context.py (legado Futures), preservando:
- Larry Williams 9.1 a 9.4 e PC;
- ATR, MME9, MMA21, swing, slope e pavio/corpo;
- universo dinâmico, regime técnico, força relativa, book e score;
- Excel, Google Drive e Telegram.

Mudanças Spot:
- category=spot;
- sem funding, OI, basis, long/short ratio, leverage ou liquidação;
- sinais VENDA são proteção (AGUARDAR / SAIR_NAO_ABRIR), nunca short;
- otimização somente de sinais COMPRA;
- grid considera fee de compra + fee de venda;
- parâmetros compatíveis com limites atuais do Spot Grid Bot.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
import requests

from legacy_futures import bybit_setups_script_hr_context as legacy
from bybit_spot_common import (
    get_kline,
    get_orderbook,
    get_spot_instruments,
    get_spot_tickers,
    normalize_timeframe,
    spot_grid_parameters,
)
from optimizer_atr_sl_tp_spot import run_optimization_with_setups

BASE_DIR = Path(__file__).resolve().parent
ARQUIVO_EXCEL = BASE_DIR / "ativos_spot.xlsx"
ARQUIVO_SAIDA = BASE_DIR / "ativos_opt_hr_contexto_spot.xlsx"
CSV_CANDLES = BASE_DIR / "dados_candles_spot.csv"
DIRETORIO_OPT = BASE_DIR / "opt_params_spot"
DIRETORIO_OPT.mkdir(parents=True, exist_ok=True)
REOTIMIZAR_APOS_DIAS = 7

FUSO_BRASILIA = timezone(timedelta(hours=-3))
SETUPS = [legacy.setup_9_1, legacy.setup_9_2, legacy.setup_9_3, legacy.setup_9_4, legacy.setup_pc]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


@dataclass
class SpotContextConfig:
    # Universo
    modo_universo: str = "HIBRIDO"          # AUTO | HIBRIDO | MANUAL
    category: str = "spot"
    quote_coin: str = "USDT"
    historico_min_dias: int = 90
    default_timeframes: str = "120,240,360,D"
    kline_limit: int = 250
    excluir_stablecoins: bool = True
    stablecoins_excluir: str = "USDT,USDC,DAI,FDUSD,TUSD,PYUSD,USDE,USD1,USDS,EURC"
    excluir_st_tag: bool = True
    symbol_types_excluir: str = "xstocks"

    # Liquidez / execução (frações quando indicado)
    min_turnover24h_usdt: float = 1_000_000
    max_spread_pct: float = 0.003             # fração: 0,003 = 0,30%
    orderbook_limit: int = 200
    min_depth_1pct_usdt: float = 50_000
    max_slippage_pct: float = 0.003           # fração: 0,30%
    capturar_orderbook: bool = True

    # Regime técnico - mesmos conceitos do context Futures
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
    atr_close_min_pct: float = 2.0             # pontos percentuais
    atr_close_max_pct: float = 8.0             # pontos percentuais

    # Força relativa
    rs_short_window: int = 3
    rs_mid_window: int = 6
    rs_long_window: int = 12
    rs_min_long: float = 65.0

    # Score
    score_min_setup: float = 70.0

    # Spot Grid
    fee_side_pct: float = 0.10                # pontos percentuais: 0,10%
    min_net_grid_pct: float = 0.15
    range_atr_down: float = 2.0
    range_atr_up: float = 3.0
    sl_buffer_atr: float = 0.7
    tp_buffer_atr: float = 0.7
    min_grids: int = 2
    max_grids: int = 200
    slope_trailing_up_pct: float = 0.75

    # Rotina
    api_sleep_s: float = 0.08
    enviar_telegram: bool = False
    upload_drive: bool = True


def _to_float(value, default=np.nan):
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        if isinstance(value, str):
            value = value.strip().replace(" ", "").replace(",", ".")
        return float(value)
    except Exception:
        return default


def _to_int(value, default=0):
    try:
        return int(float(str(value).replace(",", ".")))
    except Exception:
        return default


def _to_bool(value, default=False):
    if isinstance(value, bool):
        return value
    txt = str(value).strip().upper()
    if txt in {"SIM", "TRUE", "1", "YES", "Y"}:
        return True
    if txt in {"NAO", "NÃO", "FALSE", "0", "NO", "N"}:
        return False
    return default


def _safe_div(a, b, default=np.nan):
    try:
        if b is None or pd.isna(b) or float(b) == 0:
            return default
        return float(a) / float(b)
    except Exception:
        return default


def _spread_to_fraction(v):
    """Aceita 0.003 (fração) ou 0.30 (0,30% em pontos percentuais)."""
    x = _to_float(v)
    if pd.isna(x):
        return x
    return x / 100.0 if abs(x) > 0.05 else x


def _split_timeframes(value, fallback=("240",)):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return list(fallback)
    if isinstance(value, (int, float)):
        return [normalize_timeframe(value)]
    parts = [p.strip() for p in str(value).replace(";", ",").split(",") if p.strip()]
    return [normalize_timeframe(x) for x in parts] or list(fallback)


def _days_since_ms(ms):
    ts = _to_int(ms, 0)
    if ts <= 0:
        return None
    return (int(time.time() * 1000) - ts) / 86_400_000


ALIASES = {
    "MODO_UNIVERSO": "modo_universo",
    "TIMEFRAMES_PADRAO": "default_timeframes",
    "KLINE_LIMIT": "kline_limit",
    "IDADE_MIN_DIAS": "historico_min_dias",
    "HISTORICO_MIN_DIAS": "historico_min_dias",
    "EXCLUIR_STABLECOINS": "excluir_stablecoins",
    "STABLECOINS_EXCLUIR": "stablecoins_excluir",
    "EXCLUIR_ST_TAG": "excluir_st_tag",
    "SYMBOL_TYPES_EXCLUIR": "symbol_types_excluir",
    "TURNOVER24H_MIN": "min_turnover24h_usdt",
    "MIN_TURNOVER": "min_turnover24h_usdt",
    "SPREAD_MAX_PCT": "max_spread_pct",
    "SPREAD_PCT_MAX": "max_spread_pct",
    "DEPTH_1PCT_MIN": "min_depth_1pct_usdt",
    "DEPTH_QUOTE_MIN": "min_depth_1pct_usdt",
    "MIN_DEPTH_QUOTE": "min_depth_1pct_usdt",
    "SLIPPAGE_MAX_PCT": "max_slippage_pct",
    "CAPTURAR_ORDERBOOK": "capturar_orderbook",
    "ADX_MIN_TENDENCIA": "adx_min_trend",
    "CHOP_MAX_TENDENCIA": "chop_max_trend",
    "EFFICIENCY_RATIO_MIN": "er_min_trend",
    "ATR_PERCENTIL_MIN": "atr_percentile_min",
    "ATR_PERCENTIL_MAX": "atr_percentile_max",
    "BB_WIDTH_PERCENTIL_COMPRESSAO": "bb_width_percentile_compression",
    "ATR_CLOSE_MIN_PCT": "atr_close_min_pct",
    "ATR_CLOSE_MAX_PCT": "atr_close_max_pct",
    "RANK_MIN_LONG": "rs_min_long",
    "SCORE_MIN_SETUP": "score_min_setup",
    "FEE_SIDE_PCT": "fee_side_pct",
    "MIN_NET_GRID_PCT": "min_net_grid_pct",
    "RANGE_ATR_DOWN": "range_atr_down",
    "RANGE_ATR_UP": "range_atr_up",
    "SL_BUFFER_ATR": "sl_buffer_atr",
    "TP_BUFFER_ATR": "tp_buffer_atr",
    "MIN_GRIDS": "min_grids",
    "MAX_GRIDS": "max_grids",
    "SLOPE_TRAILING_UP": "slope_trailing_up_pct",
    "SLOPE_MIN_TRAILING": "slope_trailing_up_pct",
    "ENVIAR_TELEGRAM": "enviar_telegram",
    "UPLOAD_DRIVE": "upload_drive",
}


def _set_cfg(cfg: SpotContextConfig, key, value):
    if key is None or value is None:
        return
    attr = ALIASES.get(str(key).strip().upper(), str(key).strip())
    valid = {f.name for f in fields(cfg)}
    if attr not in valid:
        return
    cur = getattr(cfg, attr)
    if attr in {"max_spread_pct", "max_slippage_pct"}:
        parsed = _spread_to_fraction(value)
    elif isinstance(cur, bool):
        parsed = _to_bool(value, cur)
    elif isinstance(cur, int) and not isinstance(cur, bool):
        parsed = _to_int(value, cur)
    elif isinstance(cur, float):
        parsed = _to_float(value, cur)
    else:
        parsed = str(value).strip()
    setattr(cfg, attr, parsed)


def ler_config_spot(path=ARQUIVO_EXCEL) -> SpotContextConfig:
    cfg = SpotContextConfig()
    try:
        xf = pd.ExcelFile(path)
        candidate_sheets = [
            "CONFIG_SPOT", "FILTROS_SPOT", "CONFIG_UNIVERSO", "CONFIG_REGIME",
            "CONFIG_FORCA_RELATIVA", "CONFIG_GRID",
        ]
        for sheet in candidate_sheets:
            if sheet not in xf.sheet_names:
                continue
            df = pd.read_excel(path, sheet_name=sheet)
            if df.empty:
                continue
            cols = {str(c).strip().lower(): c for c in df.columns}
            pcol = cols.get("parametro") or cols.get("parâmetro") or cols.get("filtro") or df.columns[0]
            vcol = cols.get("valor") or cols.get("valor inicial") or cols.get("value")
            if vcol is None and len(df.columns) > 1:
                vcol = df.columns[1]
            if vcol is None:
                continue
            for _, row in df.iterrows():
                _set_cfg(cfg, row.get(pcol), row.get(vcol))
    except Exception as exc:
        logging.warning("[SPOT] Não foi possível ler configurações: %s. Usando defaults.", exc)
    cfg.category = "spot"
    cfg.quote_coin = "USDT"
    return cfg


def ler_manual(path=ARQUIVO_EXCEL) -> pd.DataFrame:
    try:
        xf = pd.ExcelFile(path)
        sheet = "ATIVOS" if "ATIVOS" in xf.sheet_names else xf.sheet_names[0]
        df = pd.read_excel(path, sheet_name=sheet)
        df = df.loc[:, ~df.columns.duplicated(keep="first")]
        if "Mercado" not in df.columns:
            df["Mercado"] = "spot"
        else:
            df["Mercado"] = "spot"
        return df
    except Exception as exc:
        logging.warning("[SPOT] Lista manual indisponível: %s", exc)
        return pd.DataFrame(columns=["ATIVO", "Par", "Timeframe", "Mercado"])


# === SPOT_V21_UNIVERSE =======================================================
def _csv_upper_set(value) -> set[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return set()
    return {
        x.strip().upper()
        for x in str(value).replace(";", ",").split(",")
        if x.strip()
    }


def checar_historico_minimo_spot(symbol: str, dias: int):
    # Spot não fornece launchTime. Confirma se havia negociação há 'dias'.
    dias = max(0, int(dias or 0))
    if dias == 0:
        return True, None, "DESATIVADO"

    alvo = datetime.now(timezone.utc) - timedelta(days=dias)
    inicio = int((alvo - timedelta(days=3)).timestamp() * 1000)
    fim = int(alvo.timestamp() * 1000)

    try:
        hist = get_kline(symbol, "D", limit=5, start=inicio, end=fim)
        if hist is None or hist.empty:
            return False, None, "INSUFICIENTE"

        ts = pd.to_datetime(hist["timestamp"], utc=True, errors="coerce").dropna()
        ts = ts[ts <= pd.Timestamp(alvo)]
        if ts.empty:
            return False, None, "INSUFICIENTE"

        return True, ts.max().strftime("%Y-%m-%d"), "OK"
    except Exception as exc:
        logging.warning("[SPOT] Histórico mínimo %s: %s", symbol, exc)
        return False, None, "ERRO"


def montar_universo_spot(manual_df: pd.DataFrame, cfg: SpotContextConfig):
    mode = str(cfg.modo_universo).upper().strip()
    if mode not in {"AUTO", "HIBRIDO", "MANUAL"}:
        mode = "HIBRIDO"

    default_tfs = _split_timeframes(cfg.default_timeframes)

    stablecoins = _csv_upper_set(cfg.stablecoins_excluir)
    excluded_symbol_types = {
        x.lower() for x in _csv_upper_set(cfg.symbol_types_excluir)
    }

    instruments = get_spot_instruments()
    tickers = get_spot_tickers()
    tmap = {x.get("symbol"): x for x in tickers if x.get("symbol")}

    rows = []
    candidatos_historico = 0

    for inst in instruments:
        symbol = str(inst.get("symbol", "")).upper().strip()
        if not symbol:
            continue

        ticker = tmap.get(symbol, {})
        status = inst.get("status")
        base_coin = str(inst.get("baseCoin", "")).upper().strip()
        quote = str(inst.get("quoteCoin", "")).upper().strip()
        st_tag = str(inst.get("stTag", "0")).strip()
        symbol_type = str(inst.get("symbolType", "") or "").strip()

        last = _to_float(ticker.get("lastPrice"))
        bid = _to_float(ticker.get("bid1Price"))
        ask = _to_float(ticker.get("ask1Price"))
        spread = _safe_div(ask - bid, last) if last and bid and ask else np.nan
        turnover = _to_float(ticker.get("turnover24h"), 0.0)

        stablecoin_base = base_coin in stablecoins
        st_tag_ok = (not cfg.excluir_st_tag) or st_tag != "1"
        symbol_type_ok = (
            not symbol_type
            or symbol_type.lower() not in excluded_symbol_types
        )
        stable_ok = (not cfg.excluir_stablecoins) or not stablecoin_base

        structural_pre = (
            status == "Trading"
            and quote == cfg.quote_coin
            and stable_ok
            and st_tag_ok
            and symbol_type_ok
        )
        liquidity_ok = (
            turnover >= cfg.min_turnover24h_usdt
            and (pd.isna(spread) or spread <= cfg.max_spread_pct)
        )

        historico_ok = None
        historico_ref = None
        historico_status = "NAO_AVALIADO"

        if mode != "MANUAL" and structural_pre and liquidity_ok:
            candidatos_historico += 1
            historico_ok, historico_ref, historico_status = checar_historico_minimo_spot(
                symbol, cfg.historico_min_dias
            )
            if cfg.api_sleep_s:
                time.sleep(cfg.api_sleep_s)

        structural_ok = (
            structural_pre
            if mode == "MANUAL"
            else structural_pre and bool(historico_ok)
        )

        reasons = []
        if status != "Trading":
            reasons.append("status_not_trading")
        if quote != cfg.quote_coin:
            reasons.append("quote_not_usdt")
        if cfg.excluir_stablecoins and stablecoin_base:
            reasons.append("stablecoin_base")
        if cfg.excluir_st_tag and st_tag == "1":
            reasons.append("st_tag")
        if symbol_type and symbol_type.lower() in excluded_symbol_types:
            reasons.append("symbol_type_excluido")
        if turnover < cfg.min_turnover24h_usdt:
            reasons.append("turnover24h_baixo")
        if not pd.isna(spread) and spread > cfg.max_spread_pct:
            reasons.append("spread_alto")
        if mode != "MANUAL" and structural_pre and liquidity_ok and not historico_ok:
            reasons.append(
                "historico_erro"
                if historico_status == "ERRO"
                else "historico_insuficiente"
            )

        lot = inst.get("lotSizeFilter", {}) or {}
        price_filter = inst.get("priceFilter", {}) or {}

        rows.append({
            "Par": symbol,
            "Status": status,
            "BaseCoin": base_coin,
            "QuoteCoin": quote,
            "STTag": st_tag,
            "SymbolType": symbol_type,
            "Stablecoin_Base": stablecoin_base,
            "Historico_Min_Dias": cfg.historico_min_dias,
            "Historico_OK": historico_ok,
            "Historico_Referencia_UTC": historico_ref,
            "Historico_Status": historico_status,
            "TickSize": price_filter.get("tickSize"),
            "MinOrderQty": lot.get("minOrderQty"),
            "MinOrderAmt": lot.get("minOrderAmt"),
            "LastPrice": last,
            "Bid1": bid,
            "Ask1": ask,
            "Spread_Pct": spread,
            "Turnover24h": turnover,
            "Volume24h": _to_float(ticker.get("volume24h"), 0.0),
            "Price24hPcnt": _to_float(ticker.get("price24hPcnt")),
            "Estrutural_OK": structural_ok,
            "Liquidez_OK": liquidity_ok,
            "Elegivel_Universo": bool(structural_ok and liquidity_ok),
            "Bloqueio_Universo": ";".join(reasons),
        })

    universo = pd.DataFrame(rows)
    if not universo.empty:
        universo = universo.sort_values(
            ["Elegivel_Universo", "Turnover24h"], ascending=[False, False]
        ).reset_index(drop=True)

    if mode != "MANUAL":
        logging.info(
            "[SPOT] Histórico mínimo=%d dias avaliado em %d candidato(s).",
            cfg.historico_min_dias,
            candidatos_historico,
        )

    manual = manual_df.copy()
    if not manual.empty:
        if "ATIVO" in manual.columns:
            manual = manual[manual["ATIVO"].fillna(False).astype(bool)]
        manual["Par"] = manual["Par"].astype(str).str.upper().str.strip()
        manual["Mercado"] = "spot"
        manual["Timeframe"] = manual["Timeframe"].apply(normalize_timeframe)

    if mode == "MANUAL":
        return manual.reset_index(drop=True), universo, tmap

    auto_rows = []
    if not universo.empty:
        for _, u in universo[universo["Elegivel_Universo"] == True].iterrows():
            for tf in default_tfs:
                d = {
                    "ATIVO": True,
                    "Par": u["Par"],
                    "Timeframe": tf,
                    "Mercado": "spot",
                }
                d.update(u.to_dict())
                auto_rows.append(d)

    auto = pd.DataFrame(auto_rows)

    if mode == "AUTO" or manual.empty:
        scan = auto
    elif auto.empty:
        scan = manual
    else:
        scan = pd.concat([auto, manual], ignore_index=True, sort=False)

    if not scan.empty:
        scan["Mercado"] = "spot"
        scan["Timeframe"] = scan["Timeframe"].apply(normalize_timeframe)
        scan = scan.drop_duplicates(
            subset=["Par", "Timeframe"], keep="last"
        ).reset_index(drop=True)

    logging.info(
        "[SPOT] Universo elegível=%d | timeframes=%s | linhas para scan=%d",
        int(universo["Elegivel_Universo"].sum()) if not universo.empty else 0,
        ",".join(default_tfs),
        len(scan),
    )
    return scan, universo, tmap

def capturar_contexto_spot(symbol: str, cfg: SpotContextConfig, ticker_row=None) -> dict:
    ticker = ticker_row or {}
    last = _to_float(ticker.get("lastPrice"))
    bid = _to_float(ticker.get("bid1Price"))
    ask = _to_float(ticker.get("ask1Price"))
    ctx = {
        "Turnover24h": _to_float(ticker.get("turnover24h"), 0.0),
        "Volume24h": _to_float(ticker.get("volume24h"), 0.0),
        "Spread_Pct": _safe_div(ask - bid, last) if last and bid and ask else np.nan,
        "LastPrice": last,
        "Bid1": bid,
        "Ask1": ask,
    }
    if cfg.capturar_orderbook:
        try:
            book = get_orderbook(symbol, limit=min(cfg.orderbook_limit, 200))
            ctx.update(legacy.calculate_orderbook_metrics_context(book, last))
        except Exception as exc:
            ctx["OrderbookErro"] = str(exc)
    return ctx


def score_spot_candidate(direction: str, setup: str, last_row, context, rs_row, cfg: SpotContextConfig):
    direction = str(direction).upper()
    is_buy = direction == "COMPRA"

    hard_blocks = []
    soft_alerts = []

    turnover = _to_float(context.get("Turnover24h"), 0.0)
    spread = _to_float(context.get("Spread_Pct"))
    depth = _to_float(context.get("DepthMin1Pct"))
    slippage = _to_float(context.get("SlippageEstPct"))

    liq_parts = [
        100.0 if turnover >= cfg.min_turnover24h_usdt else max(0.0, turnover / cfg.min_turnover24h_usdt * 100),
        100.0 if pd.isna(spread) or spread <= cfg.max_spread_pct else max(0.0, 100 - (spread / cfg.max_spread_pct - 1) * 100),
        100.0 if pd.isna(depth) or depth >= cfg.min_depth_1pct_usdt else max(0.0, depth / cfg.min_depth_1pct_usdt * 100),
        100.0 if pd.isna(slippage) or slippage <= cfg.max_slippage_pct else max(0.0, 100 - (slippage / cfg.max_slippage_pct - 1) * 100),
    ]
    score_liq = float(np.nanmean(liq_parts))

    if turnover < cfg.min_turnover24h_usdt:
        hard_blocks.append("turnover24h_baixo")
    if not pd.isna(spread) and spread > cfg.max_spread_pct:
        hard_blocks.append("spread_alto")
    if not pd.isna(depth) and depth < cfg.min_depth_1pct_usdt:
        hard_blocks.append("depth_baixo")

    adx = _to_float(last_row.get("ADX"))
    chop = _to_float(last_row.get("CHOP"))
    er = _to_float(last_row.get("EFFICIENCY_RATIO"))
    atr_pct = _to_float(last_row.get("ATR_PCT")) * 100
    regime = str(last_row.get("REGIME", ""))

    regime_parts = [
        min(100, adx / cfg.adx_min_trend * 100) if not pd.isna(adx) else 50,
        100 if pd.isna(chop) or chop <= cfg.chop_max_trend else max(0, 100 - (chop - cfg.chop_max_trend) * 3),
        min(100, er / cfg.er_min_trend * 100) if not pd.isna(er) else 50,
    ]
    score_regime = float(np.nanmean(regime_parts))

    if not pd.isna(atr_pct) and atr_pct < cfg.atr_close_min_pct:
        hard_blocks.append("atr_close_baixo")
    if not pd.isna(atr_pct) and atr_pct > cfg.atr_close_max_pct:
        hard_blocks.append("atr_close_alto")

    if setup in {"9.2", "9.3", "9.4", "PC"} and regime != "TENDENCIA":
        soft_alerts.append(f"regime_{regime.lower()}_nao_ideal")

    rank = _to_float(rs_row.get("RANK_FORCA")) if rs_row is not None else np.nan
    score_strength = rank if not pd.isna(rank) else 50.0

    if is_buy and not pd.isna(rank) and rank < cfg.rs_min_long:
        soft_alerts.append("forca_relativa_insuficiente")

    setup_base = {"9.1": 60, "9.2": 75, "9.3": 75, "9.4": 70, "PC": 78}.get(setup, 65)

    score_total = (
        score_liq * 0.30
        + score_regime * 0.25
        + score_strength * 0.25
        + setup_base * 0.20
    )

    approved = (
        is_buy
        and score_total >= cfg.score_min_setup
        and not hard_blocks
    )

    if not is_buy:
        status_score = "NAO_COMPRA"
    elif hard_blocks:
        status_score = "BLOQUEADO_HARD"
    elif score_total < cfg.score_min_setup:
        status_score = "SCORE_INSUFICIENTE"
    else:
        status_score = "APROVADO"

    all_reasons = hard_blocks + soft_alerts

    return {
        "REGIME_TECNICO": regime,
        "SCORE_TOTAL": round(score_total, 2),
        "SCORE_LIQUIDEZ": round(score_liq, 2),
        "SCORE_REGIME": round(score_regime, 2),
        "SCORE_FORCA": round(score_strength, 2),
        "SCORE_SETUP_BASE": setup_base,
        "RANK_FORCA": rank,
        "APROVADO_SCORE": bool(approved),
        "STATUS_SCORE": status_score,
        "BLOQUEIO_HARD": ";".join(hard_blocks),
        "ALERTA_SOFT": ";".join(soft_alerts),
        "BLOQUEIO_MOTIVO": ";".join(all_reasons),
    }

def escolher_setup_spot(df, symbol):
    """Prioridade: DISPARAR COMPRA > ARMAR COMPRA > DISPARAR VENDA > ARMAR VENDA."""
    found = []
    for fn in SETUPS:
        try:
            out = fn(df.copy(), ativo=symbol)
        except Exception as exc:
            logging.debug("Setup %s falhou em %s: %s", fn.__name__, symbol, exc)
            continue
        if isinstance(out, dict):
            found.append(out)
    if not found:
        return None

    def priority(out):
        st = str(out.get("status", "")).upper()
        tipo = str(out.get("tipo", "")).upper()
        if tipo == "COMPRA" and st.startswith("DISPARAR"): return 0
        if tipo == "COMPRA" and st.startswith("ARMAR"): return 1
        if tipo == "VENDA" and st.startswith("DISPARAR"): return 2
        return 3
    return sorted(found, key=priority)[0]


def caminho_json(par, timeframe, objective="mar"):
    return DIRETORIO_OPT / f"opt_{par}_{normalize_timeframe(timeframe)}m_{objective}.json"


def carregar_params(par, timeframe, objective="mar"):
    p = caminho_json(par, timeframe, objective)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        bp = data.get("best_params", data)
        return {
            "atr_period": int(float(bp.get("atr_period", 14))),
            "k_sl": float(bp.get("k_sl", 1.5)),
            "k_tp": float(bp.get("k_tp", 2.5)),
            "origem": "otimizado_spot",
            "generated_at": data.get("generated_at"),
        }
    except Exception as exc:
        logging.warning("[OPT-SPOT] Falha lendo %s: %s", p, exc)
        return None


def params_validos(params, days=REOTIMIZAR_APOS_DIAS):
    if not params or not params.get("generated_at"):
        return False
    try:
        dt = datetime.strptime(params["generated_at"], "%Y-%m-%d %H:%M:%S")
        return datetime.now() - dt <= timedelta(days=days)
    except Exception:
        return False


def garantir_params_spot(par, timeframe, df, cfg, objective="mar", auto_optimize=False, tick_size=0.01):
    params = carregar_params(par, timeframe, objective)
    if params and params_validos(params):
        return params
    if auto_optimize:
        try:
            work = df.set_index("timestamp") if "timestamp" in df.columns else df
            best, score = run_optimization_with_setups(
                work,
                setup_funcs=SETUPS,
                objective=objective,
                commission_bps_per_side=cfg.fee_side_pct * 100.0,
                slippage_ticks=0.0,
                tick_size=float(tick_size or 0.01),
            )
            payload = {
                "symbol": par,
                "interval": normalize_timeframe(timeframe),
                "objective": objective,
                "market": "spot",
                "best_params": best,
                "best_score": score,
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            caminho_json(par, timeframe, objective).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return {
                "atr_period": int(best.get("atr_period", 14)),
                "k_sl": float(best.get("k_sl", 1.5)),
                "k_tp": float(best.get("k_tp", 2.5)),
                "origem": "otimizado_spot",
            }
        except Exception as exc:
            logging.warning("[OPT-SPOT] Falha otimizando %s %s: %s", par, timeframe, exc)
    if params:
        return params
    return {"atr_period": 14, "k_sl": 1.5, "k_tp": 2.5, "origem": "padrao_spot"}


def enviar_telegram(message: str, cfg: SpotContextConfig):
    if not cfg.enviar_telegram:
        return
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logging.warning("[TG] TELEGRAM_BOT_TOKEN/CHAT_ID ausentes no .env")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": message}, timeout=10,
        ).raise_for_status()
    except Exception as exc:
        logging.warning("[TG] Falha: %s", exc)


def validar_parametros_bybit(grid: dict, current_price: float, status: str) -> tuple[bool, str]:
    if not grid or not current_price:
        return False, "sem_parametros"
    entry, lower, upper = grid["ENTRY"], grid["LOWER"], grid["UPPER"]
    sl, tp, grids = grid["SL"], grid["TP"], int(grid["GRIDS"])
    reasons = []
    # Entry Price do Spot Grid não pode ser maior que o preço de mercado.
    if status.startswith("DISPARAR") and entry > current_price * 1.000001:
        reasons.append("entry_acima_mercado")
    if lower < current_price * 0.30 or lower > current_price * 1.20:
        reasons.append("lower_fora_limite")
    if upper < current_price * 0.80 or upper > current_price * 3.00:
        reasons.append("upper_fora_limite")
    if not (2 <= grids <= 200): reasons.append("grids_fora_limite")
    if not (sl < lower and sl < entry): reasons.append("sl_invalido")
    if not (tp > upper and tp > entry): reasons.append("tp_invalido")
    return len(reasons) == 0, ";".join(reasons)


def gerar_excel(resultados, universo, forca, cfg):
    out = pd.DataFrame(resultados)
    with pd.ExcelWriter(ARQUIVO_SAIDA, engine="xlsxwriter") as writer:
        out.to_excel(writer, sheet_name="Setups Spot", index=False)
        if universo is not None and not universo.empty:
            universo.to_excel(writer, sheet_name="UNIVERSO_SPOT", index=False)
        if forca is not None and not forca.empty:
            forca.to_excel(writer, sheet_name="FORCA_RELATIVA", index=False)
        pd.DataFrame([{"Parametro": k, "Valor": v} for k, v in asdict(cfg).items()]).to_excel(
            writer, sheet_name="CONFIG_CONTEXT", index=False
        )

        wb = writer.book
        header = wb.add_format({"bold": True, "bg_color": "#0F766E", "font_color": "#FFFFFF", "border": 1})
        pct = wb.add_format({"num_format": "0.0000%"})
        dec = wb.add_format({"num_format": "#,##0.00000000"})
        num = wb.add_format({"num_format": "#,##0.00"})
        for name, ws in writer.sheets.items():
            if name == "CONFIG_CONTEXT":
                ws.set_column(0, 0, 30); ws.set_column(1, 1, 22)
                continue
            df_sheet = out if name == "Setups Spot" else (universo if name == "UNIVERSO_SPOT" else forca)
            if df_sheet is None:
                continue
            for c, col in enumerate(df_sheet.columns):
                ws.write(0, c, col, header)
                width = min(max(len(str(col)) + 2, 12), 28)
                fmt = None
                if col in {"SPREAD_PCT", "SLIPPAGE_EST_PCT", "ATR_PCT", "RET_3C", "RET_6C", "RET_12C"}:
                    fmt = pct
                elif col in {"GATILHO", "PRECO_ATUAL", "LOWER", "UPPER", "SL", "TP", "ATR_M1"}:
                    fmt = dec
                elif col in {"SCORE_TOTAL", "SCORE_LIQUIDEZ", "SCORE_REGIME", "SCORE_FORCA", "RANK_FORCA"}:
                    fmt = num
                ws.set_column(c, c, width, fmt)
            ws.freeze_panes(1, 0)
            if len(df_sheet.columns):
                ws.autofilter(0, 0, max(1, len(df_sheet)), len(df_sheet.columns) - 1)


def run_scan(args):
    cfg = ler_config_spot()
    if args.modo_universo:
        cfg.modo_universo = args.modo_universo
    if args.sem_orderbook:
        cfg.capturar_orderbook = False

    manual = ler_manual()
    ativos, universo, ticker_map = montar_universo_spot(manual, cfg)
    if ativos.empty:
        raise RuntimeError("Universo Spot vazio.")

    logging.info("[SPOT] %d par/timeframe para análise | modo=%s", len(ativos), cfg.modo_universo)
    candle_map = {}
    raw_rows = []
    limit = min(1000, max(legacy.PERIODOS_MINIMO + 10, cfg.kline_limit, cfg.percentile_lookback + 50))

    # 1) Candles + enriquecimento técnico
    for idx, row in ativos.iterrows():
        par = str(row["Par"]).upper().strip()
        tf = normalize_timeframe(row["Timeframe"])
        try:
            df = get_kline(par, tf, limit=limit)
            if df.empty or len(df) < legacy.PERIODOS_MINIMO:
                continue
            df["MME9"] = df["close"].ewm(span=9).mean()
            df["MMA21"] = df["close"].rolling(21).mean()
            df = legacy.enriquecer_candles_contexto(df, cfg)
            candle_map[legacy.candle_key(par, tf)] = df.reset_index(drop=True)
        except Exception as exc:
            logging.warning("[SPOT] Candles %s %s: %s", par, tf, exc)
        if cfg.api_sleep_s:
            time.sleep(cfg.api_sleep_s)

    forca = legacy.calcular_forca_relativa(candle_map, cfg)
    rs_map = {}
    if forca is not None and not forca.empty:
        for _, r in forca.iterrows():
            rs_map[legacy.candle_key(r["Par"], r.get("Timeframe", ""))] = r

    # 2) Setup + score + grid
    for _, row in ativos.iterrows():
        par = str(row["Par"]).upper().strip()
        tf = normalize_timeframe(row["Timeframe"])
        key = legacy.candle_key(par, tf)
        df = candle_map.get(key)
        if df is None or df.empty:
            continue

        signal = escolher_setup_spot(df, par)
        if not signal:
            continue

        status = str(signal.get("status", ""))
        direction = str(signal.get("tipo", "")).upper()
        trigger = float(signal.get("gatilho"))
        current = float(df.iloc[-1]["close"])
        parts = status.split()
        setup = parts[2] if len(parts) >= 3 else ""
        action = (
            "GRID" if direction == "COMPRA" and status.startswith("DISPARAR")
            else "AGUARDAR_GATILHO" if direction == "COMPRA"
            else "SAIR_NAO_ABRIR" if status.startswith("DISPARAR")
            else "AGUARDAR"
        )

        tick_size = _to_float(row.get("TickSize"), 0.01)
        params = garantir_params_spot(
            par, tf, df, cfg, objective=args.objective,
            auto_optimize=args.auto_optimize, tick_size=tick_size,
        )
        atr_series = legacy.compute_atr(df, period=params["atr_period"], method="wilder")
        atr_m1 = float(atr_series.iloc[-2])
        slope = legacy.calcular_slope_mme9(df, periodos=legacy.SLOPE_MME9_PERIODOS)

        swing_i0, swing_i1 = legacy.obter_intervalo_swing_por_setup(df, setup, direction)
        swing_abs = legacy.calcular_swing_absoluto_intervalo(df, swing_i0, swing_i1, direction)
        swing_pct = legacy.calcular_swing_percentual_intervalo(df, swing_i0, swing_i1, direction)
        wick_body = legacy.calcular_razao_pavio_corpo(df, lookback=legacy.LOOKBACK_PAVIO_CORPO)

        ticker = ticker_map.get(par, {})
        context = capturar_contexto_spot(par, cfg, ticker)
        last_closed = df.iloc[-2]
        score = score_spot_candidate(direction, setup, last_closed, context, rs_map.get(key), cfg)

        grid = {}
        if direction == "COMPRA":
            grid = spot_grid_parameters(
                entry=trigger,
                atr=atr_m1,
                slope_pct=slope,
                fee_side_pct=cfg.fee_side_pct,
                min_net_grid_pct=cfg.min_net_grid_pct,
                range_atr_down=cfg.range_atr_down,
                range_atr_up=cfg.range_atr_up,
                sl_buffer_atr=cfg.sl_buffer_atr,
                tp_buffer_atr=cfg.tp_buffer_atr,
                min_grids=cfg.min_grids,
                max_grids=cfg.max_grids,
                trailing_slope_pct=cfg.slope_trailing_up_pct,
            )
        valid, invalid_reason = validar_parametros_bybit(grid, current, status) if grid else (False, "nao_aplicavel")

        result = {
            "Par": par,
            "Timeframe": tf,
            "Mercado": "spot",
            "Time Stamp": df.iloc[-1]["timestamp"].astimezone(FUSO_BRASILIA).strftime("%d/%m/%Y %H:%M"),
            "Setup": setup,
            "SINAL_ORIGINAL": direction,
            "ARMAR/DISPARAR": parts[0] if parts else "",
            "DECISAO_SPOT": action,
            "GATILHO": trigger,
            "PRECO_ATUAL": current,
            "DIST_GATILHO_PCT": (current / trigger - 1) * 100 if trigger else np.nan,
            "ATR_PERIOD": params["atr_period"],
            "PARAM_ORIGEM": params["origem"],
            "ATR_M1": atr_m1,
            "K_SL": params["k_sl"],
            "K_TP": params["k_tp"],
            "SWING_ABS": swing_abs,
            "SWING_PCT": swing_pct,
            "SLOPE_MME9_PCT": slope,
            "RAZAO_PAVIO_CORPO": wick_body,
            "TURNOVER24H": context.get("Turnover24h"),
            "VOLUME24H": context.get("Volume24h"),
            "SPREAD_PCT": context.get("Spread_Pct"),
            "DEPTH_BID_1PCT": context.get("DepthBid1Pct"),
            "DEPTH_ASK_1PCT": context.get("DepthAsk1Pct"),
            "DEPTH_1PCT": context.get("DepthMin1Pct"),
            "BOOK_IMBALANCE_1PCT": context.get("BookImbalance1Pct"),
            "SLIPPAGE_EST_PCT": context.get("SlippageEstPct"),
            "ATR_PCT": last_closed.get("ATR_PCT"),
            "ATR_PERCENTIL": last_closed.get("ATR_PERCENTIL"),
            "ADX": last_closed.get("ADX"),
            "CHOP": last_closed.get("CHOP"),
            "EFFICIENCY_RATIO": last_closed.get("EFFICIENCY_RATIO"),
            "BB_WIDTH_PERCENTIL": last_closed.get("BB_WIDTH_PERCENTIL"),
            "CLOSE_POSITION": last_closed.get("CLOSE_POSITION"),
            **score,
            **grid,
            "PARAMETROS_BYBIT_VALIDOS": valid,
            "PARAMETROS_BYBIT_MOTIVO": invalid_reason,
            "Último Setup Identificado": f"{status} (gatilho: {trigger:.7f})",
        }
        raw_rows.append(result)

        if action == "GRID" and score["APROVADO_SCORE"]:
            enviar_telegram(
                f"SPOT GRID | {par} {tf}\n{status}\nScore={score['SCORE_TOTAL']:.1f}\n"
                f"Range={grid.get('LOWER')} - {grid.get('UPPER')} | Grids={grid.get('GRIDS')}",
                cfg,
            )

    gerar_excel(raw_rows, universo, forca, cfg)
    pd.DataFrame([
        {"Par": k[0], "Timeframe": k[1], **row}
        for k, df in candle_map.items()
        for row in df.tail(11).to_dict("records")
    ]).to_csv(CSV_CANDLES, index=False)

    if cfg.upload_drive:
        try:
            legacy.upload_file_to_drive(str(ARQUIVO_SAIDA), os.getenv("GDRIVE_FOLDER_ID"))
        except Exception as exc:
            logging.warning("[DRIVE] Upload não concluído: %s", exc)

    logging.info("[SPOT] Finalizado: %s | setups=%d", ARQUIVO_SAIDA.name, len(raw_rows))
    return 0


def test_api():
    from bybit_spot_common import get_ticker
    ticker = get_ticker("BTCUSDT")
    candles = get_kline("BTCUSDT", "240", 3)
    print("category=spot")
    print("BTCUSDT lastPrice=", ticker.get("lastPrice"))
    print("candles_240=", len(candles))
    if not candles.empty:
        print(candles[["timestamp", "open", "high", "low", "close"]].tail(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Bybit Spot Grid - scanner contextual")
    sub = p.add_subparsers(dest="mode")
    sub.add_parser("test-api", help="Teste mínimo da API Spot com BTCUSDT")
    scan = sub.add_parser("scan", help="Executa o scanner Spot contextual")
    scan.add_argument("--modo-universo", choices=["AUTO", "HIBRIDO", "MANUAL"], default=None)
    scan.add_argument("--auto-optimize", action="store_true")
    scan.add_argument("--objective", choices=["net", "mar", "sharpe", "pf"], default="mar")
    scan.add_argument("--sem-orderbook", action="store_true")
    args = p.parse_args()

    if args.mode == "test-api":
        raise SystemExit(test_api())
    if args.mode in {None, "scan"}:
        if args.mode is None:
            # argparse não cria os atributos do subparser quando ele é omitido.
            args.modo_universo = None
            args.auto_optimize = False
            args.objective = "mar"
            args.sem_orderbook = False
        raise SystemExit(run_scan(args))
