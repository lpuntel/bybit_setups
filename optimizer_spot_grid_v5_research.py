"""
Spot Grid Optimizer v5 RESEARCH.

Arquivo de pesquisa isolado: NÃO é importado pelo scanner/monitor de produção.

Objetivo desta versão:
- registrar o setup quando ele é ARMADO, antes de o gatilho ser necessariamente tocado;
- acompanhar o evento até a ativação;
- rejeitar LOW_SETUP rompido antes da ativação;
- rejeitar candle intrabar ambíguo (LOW_SETUP e gatilho no mesmo candle);
- respeitar faixa de ativação [gatilho, gatilho + tolerância];
- permitir ausência do setup atual;
- bloquear sinal VENDA ou mudança explícita para outro setup de COMPRA;
- atualizar gatilho/LOW_SETUP quando o mesmo setup de COMPRA muda;
- aplicar filtro ATR na ativação;
- recalcular slope na ativação;
- reutilizar o simulador de inventário/ciclos da v4.

É um backtest comparativo. Não envia ordens.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from legacy_futures import bybit_setups_script_hr_context as legacy
from optimizer_atr_sl_tp_spot import (
    _compute_atr,
    _simulate_one,
    _metrics,
)

RESEARCH_MODEL = "GRID_ACTIVATION_REVALIDATION_V5"
BASE_GRID_MODEL = "GRID_INVENTORY_TRAILING_V4"

ATR_PERIOD_GRID = [7, 10, 14, 21]
RANGE_ATR_UP_GRID = [2.5, 3.0, 3.5]
MIN_NET_GRID_PCT_GRID = [0.10, 0.15, 0.20, 0.25]
TRAILING_SLOPE_PCT_GRID = [0.50, 0.75, 1.00]
TRAILING_UP_STEPS_GRID = [2, 3, 4]
TP_EXTRA_GRIDS_GRID = [1, 2]

MIN_BARS_SIGNAL = 30


def _finite(x, default=0.0):
    try:
        x = float(x)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _call_setup(fn: Callable, sub: pd.DataFrame, symbol: str = ""):
    try:
        return fn(sub.copy(), ativo=symbol)
    except TypeError:
        return fn(sub.copy())


def _priority(out: Dict) -> int:
    st = str(out.get("status", "")).upper()
    tipo = str(out.get("tipo", "")).upper()
    if tipo == "COMPRA" and st.startswith("DISPARAR"):
        return 0
    if tipo == "COMPRA" and st.startswith("ARMAR"):
        return 1
    if tipo == "VENDA" and st.startswith("DISPARAR"):
        return 2
    return 3


def _choose_setup(
    sub: pd.DataFrame,
    setup_funcs: List[Callable],
    symbol: str = "",
) -> Optional[Dict]:
    found = []
    for fn in setup_funcs:
        try:
            out = _call_setup(fn, sub, symbol)
        except Exception:
            continue
        if isinstance(out, dict):
            found.append(out)
    if not found:
        return None
    return sorted(found, key=_priority)[0]


def _setup_name(status: str) -> str:
    parts = str(status).split()
    return parts[2] if len(parts) >= 3 else ""


def _find_low_setup_info(
    sub: pd.DataFrame,
    trigger: float,
) -> Tuple[Optional[float], Optional[str]]:
    closed = sub.iloc[:-1]
    if closed.empty:
        return None, None

    try:
        trigger = float(trigger)
    except Exception:
        return None, None

    tol = max(abs(trigger) * 1e-10, 1e-12)
    highs = pd.to_numeric(closed["high"], errors="coerce")
    matches = closed.loc[(highs - trigger).abs() <= tol]

    if matches.empty:
        return None, None

    candle = matches.iloc[-1]

    try:
        low = float(candle["low"])
    except Exception:
        return None, None

    if not math.isfinite(low) or low <= 0 or low >= trigger:
        return None, None

    try:
        candle_key = pd.Timestamp(matches.index[-1]).isoformat()
    except Exception:
        candle_key = str(matches.index[-1])

    return low, candle_key


def precompute_armed_events(
    df: pd.DataFrame,
    setup_funcs: List[Callable],
    symbol: str = "",
) -> List[Dict]:
    """
    Registra o primeiro aparecimento de um evento COMPRA.

    Diferença para a v4:
    ARMAR não precisa já ter tocado o gatilho. O evento é salvo quando
    aparece e a ativação é tratada posteriormente pela máquina de estados.
    """
    work = df.copy()

    if not isinstance(work.index, pd.DatetimeIndex):
        if "timestamp" not in work.columns:
            raise ValueError("DataFrame precisa de timestamp ou DatetimeIndex.")
        work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True)
        work = work.set_index("timestamp")

    events = []
    seen = set()

    for i in range(MIN_BARS_SIGNAL, len(work)):
        sub = work.iloc[: i + 1].copy()
        sub["timestamp"] = sub.index

        signal = _choose_setup(sub, setup_funcs, symbol)
        if not isinstance(signal, dict):
            continue

        direction = str(signal.get("tipo", "")).upper().strip()
        status = str(signal.get("status", "")).upper().strip()

        if direction != "COMPRA":
            continue
        if not (status.startswith("ARMAR") or status.startswith("DISPARAR")):
            continue

        try:
            trigger = float(signal.get("gatilho"))
        except Exception:
            continue

        if not math.isfinite(trigger) or trigger <= 0:
            continue

        low_setup, candle_key = _find_low_setup_info(sub, trigger)
        if low_setup is None or candle_key is None:
            continue

        setup = _setup_name(status)
        key = (
            setup,
            candle_key,
            round(trigger, 12),
            round(low_setup, 12),
        )

        if key in seen:
            continue
        seen.add(key)

        events.append({
            "signal_idx": i,
            "armed_idx": i,
            "armed_ts": work.index[i],
            "setup": setup,
            "trigger": trigger,
            "low_setup": low_setup,
            "setup_candle_key": candle_key,
            "armed_status": status,
        })

    return events


def _recalc_slope(sub: pd.DataFrame) -> float:
    try:
        return float(
            legacy.calcular_slope_mme9(
                sub,
                periodos=getattr(legacy, "SLOPE_MME9_PERIODOS", 3),
            )
        )
    except Exception:
        return 0.0


def prepare_activated_events(
    df: pd.DataFrame,
    setup_funcs: List[Callable],
    symbol: str = "",
    *,
    wait_bars: int = 30,
    activation_tolerance_pct: float = 1.0,
    atr_filter_period: int = 14,
    atr_close_min_pct: float = 1.5,
    atr_close_max_pct: float = 8.0,
) -> Tuple[List[Dict], Dict[str, float]]:
    """
    Máquina de estados histórica aproximando o monitor de produção.

    Regras principais:
    - evento nasce em ARMAR/DISPARAR COMPRA;
    - ausência posterior do setup NÃO bloqueia;
    - VENDA bloqueia;
    - outro setup COMPRA bloqueia;
    - mesmo setup COMPRA pode atualizar gatilho e LOW_SETUP;
    - LOW_SETUP rompido antes da entrada bloqueia;
    - candle com LOW_SETUP e gatilho simultaneamente é rejeitado como ambíguo;
    - ativação precisa tocar [gatilho, gatilho + tolerância];
    - ATR é validado no candle fechado anterior à ativação.
    """
    work = df.copy()

    if not isinstance(work.index, pd.DatetimeIndex):
        if "timestamp" not in work.columns:
            raise ValueError("DataFrame precisa de timestamp ou DatetimeIndex.")
        work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True)
        work = work.set_index("timestamp")

    raw_events = precompute_armed_events(work, setup_funcs, symbol)
    atr_filter = _compute_atr(work, int(atr_filter_period))

    activated = []
    stats = Counter()

    wait_bars = max(0, int(wait_bars))
    tolerance = max(0.0, float(activation_tolerance_pct)) / 100.0

    for e0 in raw_events:
        i = int(e0["armed_idx"])

        effective_trigger = float(e0["trigger"])
        effective_low = float(e0["low_setup"])
        effective_candle_key = e0.get("setup_candle_key")

        end_wait = min(len(work) - 1, i + wait_bars)
        finished = False

        for j in range(i, end_wait + 1):
            sub = work.iloc[: j + 1].copy()
            sub["timestamp"] = sub.index

            # Colunas simples que também existem no scanner.
            if "MME9" not in sub.columns:
                sub["MME9"] = sub["close"].ewm(span=9).mean()
            if "MMA21" not in sub.columns:
                sub["MMA21"] = sub["close"].rolling(21).mean()

            signal_now = _choose_setup(sub, setup_funcs, symbol)

            if isinstance(signal_now, dict):
                direction_now = str(
                    signal_now.get("tipo", "")
                ).upper().strip()
                status_now = str(
                    signal_now.get("status", "")
                ).upper().strip()
                setup_now = _setup_name(status_now)

                if direction_now == "VENDA":
                    stats["BLOCK_SINAL_VENDA"] += 1
                    finished = True
                    break

                if (
                    direction_now == "COMPRA"
                    and setup_now
                    and setup_now != e0["setup"]
                ):
                    stats["BLOCK_SETUP_MUDOU"] += 1
                    finished = True
                    break

                if (
                    direction_now == "COMPRA"
                    and setup_now == e0["setup"]
                ):
                    try:
                        trigger_now = float(signal_now.get("gatilho"))
                    except Exception:
                        trigger_now = None

                    if (
                        trigger_now is not None
                        and math.isfinite(trigger_now)
                        and trigger_now > 0
                    ):
                        low_now, candle_key_now = _find_low_setup_info(
                            sub, trigger_now
                        )

                        if low_now is not None:
                            if abs(
                                trigger_now - effective_trigger
                            ) > max(
                                abs(effective_trigger) * 1e-10,
                                1e-12,
                            ):
                                stats["GATILHO_ATUALIZADO"] += 1

                            effective_trigger = trigger_now
                            effective_low = low_now
                            effective_candle_key = candle_key_now

            # Sem setup atual: carrega o evento original/adaptado,
            # exatamente como o monitor permite.
            row = work.iloc[j]

            try:
                lo = float(row["low"])
                hi = float(row["high"])
                close = float(row["close"])
            except Exception:
                continue

            upper_activation = effective_trigger * (1.0 + tolerance)

            # No primeiro candle, usa o fechamento como proxy do preço
            # observado pelo scanner/monitor naquele instante.
            if j == i:
                if close <= effective_low:
                    stats["LOW_ROMPIDO_NO_SCAN"] += 1
                    finished = True
                    break

                touched_band = (
                    effective_trigger <= close <= upper_activation
                )
                ambiguous = False

            else:
                touched_low = lo <= effective_low
                touched_band = (
                    hi >= effective_trigger
                    and lo <= upper_activation
                )
                ambiguous = touched_low and touched_band

                if ambiguous:
                    stats["AMBIGUO_LOW_E_GATILHO"] += 1
                    finished = True
                    break

                if touched_low:
                    stats["LOW_ANTES_GATILHO"] += 1
                    finished = True
                    break

            if not touched_band:
                continue

            # Filtro ATR no último candle fechado anterior à ativação.
            atr_idx = max(0, j - 1)

            try:
                atr_value = float(atr_filter.iloc[atr_idx])
                close_prev = float(work.iloc[atr_idx]["close"])
                atr_pct = (
                    atr_value / close_prev * 100.0
                    if close_prev > 0 else float("nan")
                )
            except Exception:
                atr_pct = float("nan")

            if not math.isfinite(atr_pct):
                stats["BLOCK_ATR_INDISPONIVEL"] += 1
                finished = True
                break

            if atr_pct < float(atr_close_min_pct):
                stats["BLOCK_ATR_BAIXO"] += 1
                finished = True
                break

            if atr_pct > float(atr_close_max_pct):
                stats["BLOCK_ATR_ALTO"] += 1
                finished = True
                break

            e = dict(e0)
            e["signal_idx"] = j
            e["activation_idx"] = j
            e["activation_ts"] = work.index[j]
            e["trigger"] = effective_trigger
            e["low_setup"] = effective_low
            e["setup_candle_key"] = effective_candle_key
            e["slope_pct"] = _recalc_slope(sub)
            e["activation_atr_pct"] = atr_pct
            e["activation_type"] = (
                "IMEDIATA" if j == i else "POSTERIOR"
            )

            activated.append(e)
            stats[
                "ENTRADA_IMEDIATA"
                if j == i
                else "ENTRADA_POSTERIOR"
            ] += 1
            stats["SIMULADO_ELEGIVEL"] += 1

            finished = True
            break

        if not finished:
            stats["SEM_ENTRADA"] += 1

    out_stats = {k: float(v) for k, v in stats.items()}
    out_stats["raw_events_detected"] = float(len(raw_events))
    out_stats["activated_events"] = float(len(activated))

    return activated, out_stats


def _ranking(metrics: Dict[str, float], objective: str):
    score = _finite(metrics.get(objective), -1e12)
    trades = _finite(metrics.get("trades"), 0.0)
    reliability = min(1.0, trades / 10.0)

    adjusted = (
        score * reliability
        if score >= 0
        else score / max(reliability, 0.2)
    )

    return (
        adjusted,
        _finite(metrics.get("expectancy"), -1e12),
        _finite(metrics.get("pf"), 0.0),
        trades,
    )


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
            work,
            event,
            atr_series,
            params,
            fee_side_pct,
            slippage_ticks,
            tick_size,
        )
        if r is not None:
            results.append(r)

    return _metrics(results)


def run_optimization_with_setups(
    df: pd.DataFrame,
    setup_funcs: List[Callable],
    objective: str = "net",
    use_optuna: bool = False,
    commission_bps_per_side: float = 10.0,
    slippage_ticks: float = 0.5,
    tick_size: float = 0.01,
    defaults: Optional[Dict] = None,
    symbol: str = "",
) -> Tuple[Dict, Dict[str, float]]:
    """
    Otimização experimental v5.

    Parâmetros de ativação/filtro são fixos durante a busca.
    A busca continua somente nos parâmetros estruturais do grid.
    """
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
        "research_model": RESEARCH_MODEL,
        "base_grid_model": BASE_GRID_MODEL,
        "atr_period": int(defaults.get("atr_period", 14)),
        "range_atr_up": float(defaults.get("range_atr_up", 3.0)),
        "min_net_grid_pct": float(
            defaults.get("min_net_grid_pct", 0.15)
        ),
        "trailing_slope_pct": float(
            defaults.get("trailing_slope_pct", 0.75)
        ),
        "trailing_up_steps": int(
            defaults.get("trailing_up_steps", 3)
        ),
        "tp_extra_grids": int(
            defaults.get("tp_extra_grids", 1)
        ),
        "sl_buffer_ticks": int(
            defaults.get("sl_buffer_ticks", 2)
        ),
        "max_bars": int(defaults.get("max_bars", 30)),
        "wait_bars": int(
            defaults.get(
                "wait_bars",
                defaults.get("max_bars", 30),
            )
        ),
        "activation_tolerance_pct": float(
            defaults.get("activation_tolerance_pct", 1.0)
        ),
        "activation_atr_period": int(
            defaults.get("activation_atr_period", 14)
        ),
        "atr_close_min_pct": float(
            defaults.get("atr_close_min_pct", 1.5)
        ),
        "atr_close_max_pct": float(
            defaults.get("atr_close_max_pct", 8.0)
        ),
    }

    events, activation_stats = prepare_activated_events(
        work,
        setup_funcs,
        symbol=symbol,
        wait_bars=best["wait_bars"],
        activation_tolerance_pct=best[
            "activation_tolerance_pct"
        ],
        atr_filter_period=best["activation_atr_period"],
        atr_close_min_pct=best["atr_close_min_pct"],
        atr_close_max_pct=best["atr_close_max_pct"],
    )

    if not events:
        m = _metrics([])
        m.update(activation_stats)
        m["research_model"] = RESEARCH_MODEL
        m["base_grid_model"] = BASE_GRID_MODEL
        return best, m

    def choose(candidates, current_best):
        winner = dict(current_best)

        winner_metrics = _evaluate_params(
            work,
            events,
            winner,
            commission_bps_per_side,
            slippage_ticks,
            tick_size,
        )
        winner_rank = _ranking(winner_metrics, objective)

        for cand in candidates:
            m = _evaluate_params(
                work,
                events,
                cand,
                commission_bps_per_side,
                slippage_ticks,
                tick_size,
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

    best_metrics = {
        k: _finite(v)
        for k, v in best_metrics.items()
    }

    best_metrics.update(activation_stats)
    best_metrics["events_detected"] = float(len(events))
    best_metrics["evaluations"] = float(
        len(stage1) + len(stage2) + len(stage3)
    )
    best_metrics["research_model"] = RESEARCH_MODEL
    best_metrics["base_grid_model"] = BASE_GRID_MODEL

    return best, best_metrics
