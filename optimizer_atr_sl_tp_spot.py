"""
Otimizador ATR/SL/TP para Bybit Spot.

Reutiliza o motor de backtest consolidado em optimizer_atr_sl_tp.py, mas:
- aceita somente sinais de COMPRA (long);
- nunca converte sinais VENDA em short;
- usa custo Spot por lado configurável (default 10 bps = 0,10% por execução).
"""
from __future__ import annotations
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
import optimizer_atr_sl_tp as base

SPOT_COMMISSION_BPS_PER_SIDE = 10.0
SPOT_SLIPPAGE_TICKS = 0.5
SPOT_TICK_SIZE_DEFAULT = 0.01


def _buy_only(fn: Callable) -> Callable:
    """Mantém o setup original, mas descarta qualquer sinal que não seja COMPRA."""
    def wrapped(df, ativo=""):
        try:
            out = fn(df, ativo=ativo)
        except TypeError:
            out = fn(df)
        if isinstance(out, dict) and str(out.get("tipo", "")).lower() == "compra":
            out = dict(out)
            out["tipo"] = "compra"
            if "coluna" in out:
                out["coluna"] = str(out["coluna"]).lower()
            return out
        return None
    wrapped.__name__ = f"{getattr(fn, '__name__', 'setup')}_spot_buy_only"
    return wrapped


def run_optimization_with_setups(
    df: pd.DataFrame,
    setup_funcs: List[Callable[[pd.DataFrame], Optional[Dict]]],
    objective: str = "mar",
    use_optuna: bool = False,
    commission_bps_per_side: float = SPOT_COMMISSION_BPS_PER_SIDE,
    slippage_ticks: float = SPOT_SLIPPAGE_TICKS,
    tick_size: float = SPOT_TICK_SIZE_DEFAULT,
) -> Tuple[Dict, Dict[str, float]]:
    """Otimiza somente operações compradas e inclui custos Spot realistas no backtest."""
    work = df if isinstance(df.index, pd.DatetimeIndex) else (
        df.set_index("timestamp") if "timestamp" in df.columns else df.copy()
    )

    spot_funcs = [_buy_only(fn) for fn in setup_funcs]
    pre_sigs_all = base.precompute_signals_from_lwpc_full(work, spot_funcs)
    pre_sigs_all = [s for s in pre_sigs_all if s.get("side") == "long"]

    def signal_func(d: pd.DataFrame, ctx: Dict) -> List[Dict]:
        idx = d.index if isinstance(d.index, pd.DatetimeIndex) else pd.to_datetime(d["timestamp"])
        lo, hi = idx.min(), idx.max()
        return [s for s in pre_sigs_all if lo <= s["ts"] <= hi]

    costs = base.Costs(
        commission_bps=float(commission_bps_per_side),
        slippage_ticks=float(slippage_ticks),
        tick_size=float(tick_size),
    )

    return base.optimize_vol_sl_tp(
        work,
        signal_func=signal_func,
        costs=costs,
        objective=objective,
        use_optuna=use_optuna,
    )
