"""
Otimizador do modelo Spot Grid LOW_SETUP_TRAILING_V1.

Backtest/proxy de pesquisa. Não envia ordens.
- não otimiza K_SL / K_TP;
- LOW_SETUP permanece estrutural;
- usa spot_grid_parameters();
- avalia somente sinais de COMPRA;
- otimiza parâmetros que realmente entram no modelo Spot Grid.

O resultado é um proxy de envelope (TP/SL + atividade potencial de grid),
não uma reconstrução contábil perfeita do P&L do bot da exchange.
"""

from __future__ import annotations
import math
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from bybit_spot_common import spot_grid_parameters
from legacy_futures import bybit_setups_script_hr_context as legacy

GRID_OPT_SCHEMA_VERSION = 2
GRID_OPT_MODEL = "LOW_SETUP_TRAILING_V1"

SPOT_COMMISSION_BPS_PER_SIDE = 10.0
SPOT_SLIPPAGE_TICKS = 0.5
SPOT_TICK_SIZE_DEFAULT = 0.01

ATR_PERIOD_GRID = [7, 10, 14, 21]
RANGE_ATR_UP_GRID = [2.5, 3.0, 3.5]
MIN_NET_GRID_PCT_GRID = [0.10, 0.15, 0.20, 0.25]
TRAILING_SLOPE_PCT_GRID = [0.50, 0.75, 1.00]
TRAILING_UP_STEPS_GRID = [2, 3, 4]
TP_EXTRA_GRIDS_GRID = [1, 2]

MIN_BARS_SIGNAL = 30
DEFAULT_MAX_BARS = 30


def _finite(x, default=0.0):
    try:
        x = float(x)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _compute_atr(df: pd.DataFrame, period: int) -> pd.Series:
    h = pd.to_numeric(df["high"], errors="coerce")
    l = pd.to_numeric(df["low"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")
    prev = c.shift(1)
    tr = pd.concat(
        [(h - l).abs(), (h - prev).abs(), (l - prev).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / int(period), adjust=False).mean()


def _call_setup(fn: Callable, sub: pd.DataFrame):
    try:
        return fn(sub.copy(), ativo="")
    except TypeError:
        return fn(sub.copy())


def _find_low_setup(sub: pd.DataFrame, trigger: float):
    closed = sub.iloc[:-1]
    if closed.empty:
        return None
    tol = max(abs(trigger) * 1e-10, 1e-12)
    highs = pd.to_numeric(closed["high"], errors="coerce")
    matches = closed.loc[(highs - trigger).abs() <= tol]
    if matches.empty:
        return None
    try:
        low = float(matches.iloc[-1]["low"])
        return low if low > 0 and low < trigger else None
    except Exception:
        return None


def _setup_name(status: str) -> str:
    parts = str(status).split()
    return parts[2] if len(parts) >= 3 else ""


def precompute_grid_events(
    df: pd.DataFrame,
    setup_funcs: List[Callable[[pd.DataFrame], Optional[Dict]]],
) -> List[Dict]:
    work = df.copy()
    if not isinstance(work.index, pd.DatetimeIndex):
        if "timestamp" not in work.columns:
            raise ValueError("DataFrame precisa de timestamp ou DatetimeIndex.")
        work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True)
        work = work.set_index("timestamp")

    events: List[Dict] = []
    seen = set()

    for i in range(MIN_BARS_SIGNAL, len(work)):
        sub = work.iloc[: i + 1].copy()
        sub["timestamp"] = sub.index

        found = []
        for fn in setup_funcs:
            try:
                out = _call_setup(fn, sub)
            except Exception:
                continue
            if not isinstance(out, dict):
                continue
            if str(out.get("tipo", "")).strip().upper() != "COMPRA":
                continue

            status = str(out.get("status", "")).upper()
            try:
                trigger = float(out.get("gatilho"))
            except Exception:
                continue
            if trigger <= 0:
                continue

            if status.startswith("ARMAR"):
                try:
                    if float(sub.iloc[-1]["high"]) < trigger:
                        continue
                except Exception:
                    continue
                priority = 1
            elif status.startswith("DISPARAR"):
                priority = 0
            else:
                continue

            found.append((priority, out, trigger))

        if not found:
            continue

        _, out, trigger = sorted(found, key=lambda x: x[0])[0]
        low_setup = _find_low_setup(sub, trigger)
        if low_setup is None:
            continue

        try:
            slope = float(
                legacy.calcular_slope_mme9(
                    sub,
                    periodos=getattr(legacy, "SLOPE_MME9_PERIODOS", 3),
                )
            )
        except Exception:
            slope = 0.0

        setup = _setup_name(out.get("status", ""))
        key = (i, setup, round(trigger, 12), round(low_setup, 12))
        if key in seen:
            continue
        seen.add(key)

        events.append({
            "signal_idx": i,
            "ts": work.index[i],
            "setup": setup,
            "trigger": trigger,
            "low_setup": low_setup,
            "slope_pct": slope,
        })

    return events


def _simulate_one(
    work: pd.DataFrame,
    event: Dict,
    atr_series: pd.Series,
    params: Dict,
    fee_side_pct: float,
    slippage_ticks: float,
    tick_size: float,
) -> Optional[Dict]:
    i = int(event["signal_idx"])
    atr_idx = max(0, i - 1)

    try:
        atr = float(atr_series.iloc[atr_idx])
    except Exception:
        return None
    if not math.isfinite(atr) or atr <= 0:
        return None

    grid = spot_grid_parameters(
        entry=float(event["trigger"]),
        atr=atr,
        slope_pct=float(event.get("slope_pct") or 0.0),
        fee_side_pct=fee_side_pct,
        min_net_grid_pct=float(params["min_net_grid_pct"]),
        range_atr_down=2.0,
        range_atr_up=float(params["range_atr_up"]),
        min_grids=2,
        max_grids=200,
        trailing_slope_pct=float(params["trailing_slope_pct"]),
        low_setup=float(event["low_setup"]),
        tick_size=tick_size,
        sl_buffer_ticks=int(params["sl_buffer_ticks"]),
        trailing_up_steps=int(params["trailing_up_steps"]),
        tp_extra_grids=int(params["tp_extra_grids"]),
    )
    if not grid:
        return None

    entry = float(grid["ENTRY"])
    sl = float(grid["SL"])
    tp = float(grid["TP"])
    lower = float(grid["LOWER"])
    interval = float(grid["GRID_INTERVAL_PRICE"])
    max_bars = int(params.get("max_bars", DEFAULT_MAX_BARS))

    start = i + 1
    if start >= len(work):
        return None
    end = min(len(work) - 1, i + max_bars)

    exit_price = float(work.iloc[end]["close"])
    exit_reason = "TIME"
    activity = 0.0

    for j in range(start, end + 1):
        bar = work.iloc[j]
        high = float(bar["high"])
        low = float(bar["low"])

        if interval > 0:
            lo_clip = max(low, lower)
            hi_clip = min(high, tp)
            if hi_clip > lo_clip:
                activity += max(0.0, (hi_clip - lo_clip) / interval)

        hit_sl = low <= sl
        hit_tp = high >= tp

        # Pior caso se ambos ocorrerem no mesmo candle.
        if hit_sl:
            exit_price = sl
            exit_reason = "SL"
            break
        if hit_tp:
            exit_price = tp
            exit_reason = "TP"
            break

    gross_pct = (exit_price / entry - 1.0) * 100.0
    fee_pct = 2.0 * fee_side_pct
    slip_pct = (
        2.0 * float(slippage_ticks) * float(tick_size) / entry * 100.0
        if entry > 0 else 0.0
    )

    return {
        "net_proxy_pct": gross_pct - fee_pct - slip_pct,
        "exit_reason": exit_reason,
        "grid_activity": activity,
        "grids": float(grid.get("GRIDS") or 0),
        "trailing_up": bool(grid.get("TRAILING_UP")),
    }


def _metrics(results: List[Dict]) -> Dict[str, float]:
    if not results:
        return {
            "trades": 0.0, "net": 0.0, "winrate": 0.0,
            "pf": 0.0, "sharpe": 0.0, "maxdd": 0.0,
            "mar": 0.0, "expectancy": 0.0, "grid_activity": 0.0,
            "tp_rate": 0.0, "sl_rate": 0.0,
        }

    pnls = np.array([r["net_proxy_pct"] for r in results], dtype=float)
    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]

    net = float(pnls.sum())
    expectancy = float(pnls.mean())
    winrate = float((pnls > 0).mean())
    gross_win = float(wins.sum()) if len(wins) else 0.0
    gross_loss = abs(float(losses.sum())) if len(losses) else 0.0
    pf = gross_win / gross_loss if gross_loss > 0 else (99.0 if gross_win > 0 else 0.0)

    eq = pnls.cumsum()
    peak = np.maximum.accumulate(np.insert(eq, 0, 0.0))[1:]
    dd = eq - peak
    maxdd = float(dd.min()) if len(dd) else 0.0
    mar = net / abs(maxdd) if maxdd < 0 else (99.0 if net > 0 else 0.0)

    if len(pnls) >= 2:
        sd = float(pnls.std(ddof=1))
        sharpe = float(pnls.mean() / sd * math.sqrt(len(pnls))) if sd > 0 else 0.0
    else:
        sharpe = 0.0

    return {
        "trades": float(len(results)),
        "net": _finite(net),
        "winrate": _finite(winrate),
        "pf": _finite(pf),
        "sharpe": _finite(sharpe),
        "maxdd": _finite(maxdd),
        "mar": _finite(mar),
        "expectancy": _finite(expectancy),
        "grid_activity": _finite(np.mean([r["grid_activity"] for r in results])),
        "tp_rate": _finite(np.mean([r["exit_reason"] == "TP" for r in results])),
        "sl_rate": _finite(np.mean([r["exit_reason"] == "SL" for r in results])),
    }


def _evaluate_params(
    work: pd.DataFrame,
    events: List[Dict],
    params: Dict,
    commission_bps_per_side: float,
    slippage_ticks: float,
    tick_size: float,
) -> Dict[str, float]:
    atr_series = _compute_atr(work, int(params["atr_period"]))
    fee_side_pct = float(commission_bps_per_side) / 100.0
    results = []
    for event in events:
        r = _simulate_one(
            work, event, atr_series, params, fee_side_pct,
            slippage_ticks, tick_size,
        )
        if r is not None:
            results.append(r)
    return _metrics(results)


def _ranking(metrics: Dict[str, float], objective: str):
    score = _finite(metrics.get(objective), -1e12)
    trades = _finite(metrics.get("trades"), 0.0)
    reliability = min(1.0, trades / 5.0)
    adjusted = score * reliability if score >= 0 else score / max(reliability, 0.2)
    return (
        adjusted,
        _finite(metrics.get("expectancy"), -1e12),
        _finite(metrics.get("grid_activity"), 0.0),
        trades,
    )


def run_optimization_with_setups(
    df: pd.DataFrame,
    setup_funcs: List[Callable[[pd.DataFrame], Optional[Dict]]],
    objective: str = "mar",
    use_optuna: bool = False,
    commission_bps_per_side: float = SPOT_COMMISSION_BPS_PER_SIDE,
    slippage_ticks: float = SPOT_SLIPPAGE_TICKS,
    tick_size: float = SPOT_TICK_SIZE_DEFAULT,
    defaults: Optional[Dict] = None,
) -> Tuple[Dict, Dict[str, float]]:
    if objective not in {"net", "mar", "sharpe", "pf"}:
        raise ValueError("objective deve ser net, mar, sharpe ou pf")

    work = df.copy()
    if not isinstance(work.index, pd.DatetimeIndex):
        if "timestamp" not in work.columns:
            raise ValueError("DataFrame precisa de timestamp ou DatetimeIndex.")
        work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True)
        work = work.set_index("timestamp")

    defaults = dict(defaults or {})
    best = {
        "schema_version": GRID_OPT_SCHEMA_VERSION,
        "optimizer_model": GRID_OPT_MODEL,
        "atr_period": int(defaults.get("atr_period", 14)),
        "range_atr_up": float(defaults.get("range_atr_up", 3.0)),
        "min_net_grid_pct": float(defaults.get("min_net_grid_pct", 0.15)),
        "trailing_slope_pct": float(defaults.get("trailing_slope_pct", 0.75)),
        "trailing_up_steps": int(defaults.get("trailing_up_steps", 3)),
        "tp_extra_grids": int(defaults.get("tp_extra_grids", 1)),
        "sl_buffer_ticks": int(defaults.get("sl_buffer_ticks", 2)),
        "max_bars": int(defaults.get("max_bars", DEFAULT_MAX_BARS)),
    }

    events = precompute_grid_events(work, setup_funcs)
    if not events:
        m = _metrics([])
        m["events_detected"] = 0.0
        return best, m

    def choose(candidates, current_best):
        winner = dict(current_best)
        winner_metrics = _evaluate_params(
            work, events, winner,
            commission_bps_per_side, slippage_ticks, tick_size,
        )
        winner_rank = _ranking(winner_metrics, objective)

        for cand in candidates:
            m = _evaluate_params(
                work, events, cand,
                commission_bps_per_side, slippage_ticks, tick_size,
            )
            rank = _ranking(m, objective)
            if rank > winner_rank:
                winner = dict(cand)
                winner_metrics = m
                winner_rank = rank
        return winner, winner_metrics

    stage1 = []
    for atr_period in ATR_PERIOD_GRID:
        for range_up in RANGE_ATR_UP_GRID:
            c = dict(best)
            c["atr_period"] = atr_period
            c["range_atr_up"] = range_up
            stage1.append(c)
    best, best_metrics = choose(stage1, best)

    stage2 = []
    for min_net in MIN_NET_GRID_PCT_GRID:
        c = dict(best)
        c["min_net_grid_pct"] = min_net
        stage2.append(c)
    best, best_metrics = choose(stage2, best)

    stage3 = []
    for slope in TRAILING_SLOPE_PCT_GRID:
        for steps in TRAILING_UP_STEPS_GRID:
            for extra in TP_EXTRA_GRIDS_GRID:
                c = dict(best)
                c["trailing_slope_pct"] = slope
                c["trailing_up_steps"] = steps
                c["tp_extra_grids"] = extra
                stage3.append(c)
    best, best_metrics = choose(stage3, best)

    best["schema_version"] = GRID_OPT_SCHEMA_VERSION
    best["optimizer_model"] = GRID_OPT_MODEL
    best_metrics = {k: _finite(v) for k, v in best_metrics.items()}
    best_metrics["events_detected"] = float(len(events))
    best_metrics["evaluations"] = float(len(stage1) + len(stage2) + len(stage3))
    best_metrics["proxy_model"] = GRID_OPT_MODEL
    return best, best_metrics
