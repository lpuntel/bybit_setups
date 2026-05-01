"""
===============================================================================
 MINI RESUMO — COMO O OTIMIZADOR DEFINE ATR_PERIOD, K_SL E K_TP
===============================================================================

1) GERAÇÃO DOS SINAIS (SETUPS → INTENÇÕES DE TRADE)
   ------------------------------------------------
   - Para cada candle, os setups (9.1 a 9.4 + PC) são executados com df completo
     até aquele ponto.
   - Sempre que um setup retorna:
         DISPARAR COMPRA / DISPARAR VENDA
     é criado um sinal de trade no timestamp daquele candle.

   - Também é permitido:
         ARMAR + rompimento no candle [0]
     → Isso vira entrada (sinal de trade) no otimizador.

   - Os sinais começam somente após um "warmup" mínimo de candles (MIN_BARS_SIGNAL)
     para garantir que MME9, MMA21 e ATR estejam estabilizados.


2) SIMULAÇÃO DE TRADE (ATR, SL, TP)
   ---------------------------------
   Para cada sinal:
   - O otimizador escolhe o candle de entrada de acordo com:
         entry_reference = 'after_signal'  → entrada no próximo candle.
   - Calcula o ATR no candle de entrada com o período atual da grade.
   - Define SL/TP usando multiplicadores:
         SL = entry_price ± k_sl * ATR
         TP = entry_price ± k_tp * ATR
     (sinal invertido para venda, como esperado)

   - O trade é simulado candle a candle, até:
         • SL ser atingido
         • TP ser atingido
         • timeout opcional
         • ou o fim do histórico


3) AVALIAÇÃO DE PERFORMANCE (MAR E NÚMERO DE TRADES)
   --------------------------------------------------
   - Para cada combinação de parâmetros testada no grid:
         ATR_PERIOD_GRID × K_SL_GRID × K_TP_GRID × TRAILING_GRID
     o otimizador simula TODOS os trades gerados no histórico inteiro.

   - As métricas são calculadas por:
         evaluate(df, trades, costs)
     que devolve:
         net, winrate, pf, sharpe, maxdd, mar, expectancy, trades

   - O parâmetro "objective" (por padrão = 'mar') define o critério
     principal de seleção.  
     Em caso de empate, é usado também o número de trades.


4) ESCOLHA DO MELHOR CONJUNTO
   ---------------------------
   - A melhor combinação é aquela com:
         maior valor de objective  (ex: MAR mais alto)
         e, em caso de empate, maior número de trades.

   - O resultado final é salvo como JSON em opt_params/:
         opt_<PAR>_<TF>_<objective>.json
     contendo:
         atr_period, k_sl, k_tp, trailing, e métricas finais.


5) USO NA PLANILHA (scan normal)
   ------------------------------
   - No modo "scan", o script lê o JSON otimizado (se existir) ou usa
     parâmetros padrão.
   - SL e TP são recalculados sempre que há ARMAR ou DISPARAR:
         ATR_M1    = ATR no candle [-1]
         SL / TP   = gatilho ± multiplicadores
   - A planilha ativos_opt.xlsx reflete:
         PARAM_ORIGEM = 'otimizado' ou 'padrao'
         ATR_PERIOD, K_SL, K_TP, ATR_M1, SL, TP.

===============================================================================
"""

from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Literal, Optional, Tuple

import numpy as np
import pandas as pd

# ============================================================================
# CONFIGURAÇÃO GLOBAL DE OTIMIZAÇÃO (FÁCIL DE BRINCAR NOS TESTES)
# ============================================================================

# Grade de parâmetros que o grid search vai testar
ATR_PERIOD_GRID = [5, 7, 10, 14, 21, 28] #Grid Moderado
K_SL_GRID       = [0.8, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5]
K_TP_GRID       = [1.2, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0]
TRAILING_GRID   = [False, True]

# Nº mínimo de candles antes de começar a gerar sinais (warmup das médias)
MIN_BARS_SIGNAL = 30

# -------------------------
# Utilities
# -------------------------

def compute_atr(df: pd.DataFrame, period: int = 14, method: Literal["wilder","ema","sma"] = "wilder") -> pd.Series:
    """Compute ATR on OHLCV dataframe with columns ['open','high','low','close'].
    Returns a Series aligned with df.index.
    """
    h, l, c = df['high'], df['low'], df['close']
    prev_c = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    if method == "wilder":
        # Wilder's smoothing (RMA)
        atr = tr.ewm(alpha=1/period, adjust=False).mean()
    elif method == "ema":
        atr = tr.ewm(span=period, adjust=False).mean()
    else:
        atr = tr.rolling(period, min_periods=1).mean()
    return atr

@dataclass
class Costs:
    commission_bps: float = 0.0      # round-turn bps (e.g., 2.0 = 0.02%)
    slippage_ticks: float = 0.0      # ticks per side
    tick_size: float = 0.01          # instrument tick

    
    def round_turn_cost(self, price: float) -> float:
        return price * (self.commission_bps / 10000.0)

    def slip(self, side: Literal['long','short']) -> float:
        # simple symmetric slippage model
        return self.slippage_ticks * self.tick_size

# Custos padrão do backtest (pode ajustar se quiser ser mais conservador)
DEFAULT_COSTS = Costs(
    commission_bps=2.0,   # 2 bps round-turn
    slippage_ticks=0.5,
    tick_size=0.01,
)

@dataclass
class Trade:
    ts: pd.Timestamp
    side: Literal['long','short']
    entry_idx: int
    entry_price: float
    size: float
    k_sl: float
    k_tp: float
    atr_at_entry: float
    sl_price: float
    tp_price: float
    exit_idx: Optional[int] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None

@dataclass
class StrategyParams:
    atr_period: int = 14
    atr_method: Literal["wilder","ema","sma"] = "wilder"
    k_sl: float = 1.5
    k_tp: float = 2.5
    trailing: bool = False
    breakeven_after_rr: Optional[float] = None  # e.g., 1.0 -> move SL to BE after 1R
    partial_tp_rr: Optional[float] = None       # take partial at xR
    partial_frac: float = 0.5
    risk_model: Literal['fixed_usd','fixed_qty','fractional_equity'] = 'fixed_qty'
    fixed_qty: float = 1.0
    fixed_risk_usd: float = 100.0
    fractional_risk: float = 0.01               # 1% of equity
    entry_reference: Literal['candle_-1','after_signal'] = 'candle_-1'
    max_bars_in_trade: Optional[int] = None

# -------------------------
# Backtest engine (event-driven, bar-resolution)
# -------------------------

def generate_trades(
    df: pd.DataFrame,
    signals: List[Dict],
    params: StrategyParams,
    costs: Costs,
    initial_equity: float = 10_000.0,
) -> List[Trade]:
    """Create Trade objects from signal intents with ATR-based SL/TP.
    signals: list of dicts with keys {"ts", "side", optional "entry_price", optional "size"}
    """
    atr = compute_atr(df, params.atr_period, params.atr_method)
    index_map = {ts: i for i, ts in enumerate(df.index)}

    equity = initial_equity
    trades: List[Trade] = []

    for s in signals:
        ts = s['ts']
        side = s['side']
        if ts not in index_map:
            # snap to next available index
            next_idx = df.index.searchsorted(ts)
            if next_idx >= len(df):
                continue
            entry_idx = int(next_idx)
        else:
            entry_idx = index_map[ts]

        if params.entry_reference == 'candle_-1':
            entry_idx = max(0, entry_idx)
        else:  # 'after_signal' -> enter at next bar open
            entry_idx = min(entry_idx + 1, len(df) - 1)

        entry_row = df.iloc[entry_idx]
        atr_val = float(atr.iloc[entry_idx])
        if np.isnan(atr_val) or atr_val == 0:
            continue

        # position sizing by risk
        if params.risk_model == 'fixed_qty':
            qty = s.get('size', params.fixed_qty)
        elif params.risk_model == 'fixed_usd':
            # risk per trade in USD divided by SL distance
            sl_dist = params.k_sl * atr_val
            if sl_dist <= 0:
                continue
            qty = max(0.0, params.fixed_risk_usd / sl_dist)
        else:  # 'fractional_equity'
            sl_dist = params.k_sl * atr_val
            risk_usd = equity * params.fractional_risk
            qty = max(0.0, risk_usd / sl_dist) if sl_dist > 0 else 0.0

        # entry price and SL/TP
        entry_price = float(s.get('entry_price', entry_row['open']))
        slip = costs.slip(side)
        entry_price = entry_price + (slip if side == 'long' else -slip)

        if side == 'long':
            sl_price = entry_price - params.k_sl * atr_val
            tp_price = entry_price + params.k_tp * atr_val
        else:
            sl_price = entry_price + params.k_sl * atr_val
            tp_price = entry_price - params.k_tp * atr_val

        tr = Trade(
            ts=df.index[entry_idx],
            side=side,
            entry_idx=entry_idx,
            entry_price=entry_price,
            size=qty,
            k_sl=params.k_sl,
            k_tp=params.k_tp,
            atr_at_entry=atr_val,
            sl_price=sl_price,
            tp_price=tp_price,
        )
        trades.append(tr)

    return trades


def simulate(df: pd.DataFrame, trades: List[Trade], params: StrategyParams, costs: Costs) -> List[Trade]:
    """Simulate bar-by-bar exits for each Trade (SL/TP/trailing/timeout)."""
    highs = df['high'].values
    lows = df['low'].values
    opens = df['open'].values

    for tr in trades:
        max_favor = 0.0  # track favorable excursion for trailing/BE
        start = tr.entry_idx + 1  # process bars after entry
        for i in range(start, len(df)):
            bar_h, bar_l, bar_o = highs[i], lows[i], opens[i]

            # optional trailing or BE shift
            if params.trailing or params.breakeven_after_rr is not None:
                r = abs(tr.entry_price - tr.sl_price)
                if r > 0:
                    fe = (bar_h - tr.entry_price) if tr.side == 'long' else (tr.entry_price - bar_l)
                    max_favor = max(max_favor, fe)
                    # Break-even move
                    if params.breakeven_after_rr is not None and max_favor >= params.breakeven_after_rr * r:
                        tr.sl_price = tr.entry_price
                    # Simple chandelier-like trailing: move SL with max_favor - k_sl*ATR (approx)
                    if params.trailing:
                        atr_here = abs(tr.entry_price - tr.sl_price) / params.k_sl
                        if tr.side == 'long':
                            new_sl = bar_h - params.k_sl * atr_here
                            tr.sl_price = max(tr.sl_price, new_sl)
                        else:
                            new_sl = bar_l + params.k_sl * atr_here
                            tr.sl_price = min(tr.sl_price, new_sl)

            hit_tp = hit_sl = False
            if tr.side == 'long':
                # SL/TP with intra-bar priority: assume worst-case (SL first) unless configured differently
                if bar_l <= tr.sl_price:
                    tr.exit_idx, tr.exit_price, tr.exit_reason = i, tr.sl_price, 'SL'
                    hit_sl = True
                if not hit_sl and bar_h >= tr.tp_price:
                    tr.exit_idx, tr.exit_price, tr.exit_reason = i, tr.tp_price, 'TP'
                    hit_tp = True
            else:
                if bar_h >= tr.sl_price:
                    tr.exit_idx, tr.exit_price, tr.exit_reason = i, tr.sl_price, 'SL'
                    hit_sl = True
                if not hit_sl and bar_l <= tr.tp_price:
                    tr.exit_idx, tr.exit_price, tr.exit_reason = i, tr.tp_price, 'TP'
                    hit_tp = True

            if tr.exit_idx is not None:
                # apply round-turn costs at exit
                fee = costs.round_turn_cost(tr.entry_price) + costs.round_turn_cost(tr.exit_price)
                if tr.exit_reason == 'SL' and costs.slippage_ticks:
                    # add extra slip on stop
                    tr.exit_price += (costs.slip(tr.side) if tr.side == 'long' else -costs.slip(tr.side))
                # we keep fee implicit; P&L computation handles it
                break

            if params.max_bars_in_trade is not None and (i - tr.entry_idx) >= params.max_bars_in_trade:
                tr.exit_idx, tr.exit_price, tr.exit_reason = i, bar_o, 'TIME'
                break

        if tr.exit_idx is None:
            # close at last bar close
            last_close = float(df['close'].iloc[-1])
            tr.exit_idx, tr.exit_price, tr.exit_reason = len(df) - 1, last_close, 'EOD'

    return trades

# -------------------------
# Metrics
# -------------------------

def trade_pnl(tr: Trade, costs: Costs) -> float:
    gross = (tr.exit_price - tr.entry_price) * tr.size if tr.side == 'long' else (tr.entry_price - tr.exit_price) * tr.size
    fee = costs.round_turn_cost(tr.entry_price) + costs.round_turn_cost(tr.exit_price)
    return gross - fee


def evaluate(df: pd.DataFrame, trades: List[Trade], costs: Costs) -> Dict[str, float]:
    pnls = np.array([trade_pnl(t, costs) for t in trades], dtype=float)
    if len(pnls) == 0:
        return {k: 0.0 for k in ['trades','net','winrate','pf','sharpe','maxdd','mar','expectancy']}

    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    net = pnls.sum()
    winrate = (len(wins) / len(pnls)) if len(pnls) else 0.0
    pf = (wins.sum() / abs(losses.sum())) if losses.sum() != 0 else math.inf
    expectancy = pnls.mean()

    # Equity curve (assume sequential)
    eq = pnls.cumsum()
    peak = np.maximum.accumulate(eq)
    dd = eq - peak
    maxdd = dd.min() if len(dd) else 0.0

    # Dailyized Sharpe (assume df has a consistent timeframe; infer bars per day)
    # Approx bars/day via median of per-day counts
    days = pd.Series(df.index.date)
    bpd = days.value_counts().median() if len(days) else 1
    # Map trade PnL to bar returns evenly (rough approx)
    if len(eq) >= 2:
        ret = np.diff(np.insert(eq, 0, 0))
        sd = ret.std(ddof=1)
        sharpe = (ret.mean() / sd * math.sqrt(bpd*252)) if sd > 0 else 0.0
    else:
        sharpe = 0.0

    # MAR (Net / |MaxDD|)
    mar = (net / abs(maxdd)) if maxdd != 0 else math.inf

    return {
        'trades': float(len(pnls)),
        'net': float(net),
        'winrate': float(winrate),
        'pf': float(pf),
        'sharpe': float(sharpe),
        'maxdd': float(maxdd),
        'mar': float(mar),
        'expectancy': float(expectancy),
    }

# -------------------------
# Walk-forward / CV
# -------------------------

def time_series_splits(n: int, n_splits: int = 4, min_train_frac: float = 0.5) -> List[Tuple[slice, slice]]:
    """Generate expanding window splits.
    Returns list of (train_slice, test_slice) over index positions [0..n-1].
    """
    splits = []
    min_train = int(n * min_train_frac)
    step = (n - min_train) // n_splits if n_splits > 0 else 0
    for i in range(n_splits):
        train_end = min_train + i * step
        test_end = min(n, train_end + step)
        if test_end <= train_end:
            break
        splits.append((slice(0, train_end), slice(train_end, test_end)))
    return splits

# -------------------------
# Optimizers
# -------------------------

def grid_search(
    df: pd.DataFrame,
    signal_func: Callable[[pd.DataFrame, Dict], List[Dict]],
    grid: Dict[str, Iterable],
    costs: Costs,
    objective: Literal['net','mar','sharpe','pf'] = 'mar',
    cv_splits: int = 1,  # ignorado na versão simples
    precomputed_signals: Optional[List[Dict]] = None,
) -> Tuple[Dict, Dict[str,float]]:
    """
    Versão simplificada:
    - NÃO usa cross-validation.
    - Usa o histórico inteiro de df de uma vez.
    - Usa sinais pré-computados (precomputed_signals) se fornecidos,
      senão chama signal_func(df, {"phase": "all"}).
    - Escolhe o melhor conjunto com base em (objective, trades), isto é:
      primeiro maior MAR (por exemplo), depois maior nº de trades.
    """
    # Garantir índice datetime
    work = df if isinstance(df.index, pd.DatetimeIndex) else (
        df.set_index('timestamp') if 'timestamp' in df.columns else df.copy()
    )

    # Sinais pré-computados
    if precomputed_signals is None:
        pre_sigs = signal_func(work, {"phase": "all"})
    else:
        pre_sigs = precomputed_signals

    best_params: Optional[Dict] = None
    best_score_tuple = (-np.inf, -np.inf)  # (score, trades)
    best_metrics: Dict[str, float] = {}

    combos_total = (
        len(list(grid.get('atr_period', [14]))) *
        len(list(grid.get('k_sl', [1.5]))) *
        len(list(grid.get('k_tp', [2.0, 3.0]))) *
        len(list(grid.get('trailing', [False])))
    )
    checked = 0

    for atr_period in grid.get('atr_period', [14]):
        for k_sl in grid.get('k_sl', [1.5]):
            for k_tp in grid.get('k_tp', [2.0, 3.0]):
                for trailing in grid.get('trailing', [False]):
                    params = StrategyParams(
                        atr_period=atr_period,
                        k_sl=k_sl,
                        k_tp=k_tp,
                        trailing=trailing,
                        entry_reference='after_signal',  # entra no próximo candle após o sinal
                    )

                    trades = generate_trades(work, pre_sigs, params, costs)
                    trades = simulate(work, trades, params, costs)
                    m = evaluate(work, trades, costs)

                    score = float(m.get(objective, 0.0))
                    trades_count = float(m.get('trades', 0.0))

                    cand = (score, trades_count)
                    if cand > best_score_tuple:
                        best_score_tuple = cand
                        best_params = params.__dict__.copy()
                        best_metrics = {objective: score, 'trades': trades_count}

                    checked += 1
                    if checked % 50 == 0:
                        print(f"[GRID] {checked}/{combos_total} testadas... melhor={best_metrics}")

    if best_params is None:
        best_params = StrategyParams().__dict__.copy()
        best_metrics = {objective: 0.0, 'trades': 0.0}

    return best_params, best_metrics


def optimize_vol_sl_tp(
    df: pd.DataFrame,
    signal_func: Callable[[pd.DataFrame, Dict], List[Dict]],
    costs: Costs = DEFAULT_COSTS,
    objective: Literal['net','mar','sharpe','pf'] = 'mar',
    use_optuna: bool = False,
) -> Tuple[Dict, Dict[str,float]]:
    """
    Versão simplificada:
    - Ignora Optuna (mesmo se use_optuna=True).
    - Faz um único grid search simples usando TODO o histórico.
    - Usa sinais pré-computados para reduzir custo.
    """

    if use_optuna:
        print("[OPT] Optuna foi solicitado, mas está desativado nesta versão simplificada. Usando grid search simples.")

    grid = {
        'atr_period': ATR_PERIOD_GRID,
        'k_sl':       K_SL_GRID,
        'k_tp':       K_TP_GRID,
        'trailing':   TRAILING_GRID,
    }

    # Garantir índice datetime
    work = df if isinstance(df.index, pd.DatetimeIndex) else (
        df.set_index('timestamp') if 'timestamp' in df.columns else df.copy()
    )

    # Sinais pré-computados uma única vez no histórico inteiro
    pre_sigs = signal_func(work, {"phase": "all_precomputed"})

    # Se não há nenhum sinal, não há o que otimizar → devolve defaults
    if not pre_sigs:
        # Você pode ajustar esses defaults se quiser
        default = StrategyParams()
        metrics = {objective: 0.0, 'trades': 0.0}
        return default.__dict__.copy(), metrics

    return grid_search(
        work,
        signal_func,
        grid,
        costs,
        objective,
        cv_splits=1,
        precomputed_signals=pre_sigs,
    )

# -------------------------
# Minimal example signal function
# -------------------------

def example_signal_func(df: pd.DataFrame, ctx: Dict) -> List[Dict]:
    """Toy signals: go long when close crosses above MME9; short when crosses below.
    Replace this with your setups (9.1–9.4, PC) to emit DISPARAR events.
    """
    ema = df['close'].ewm(span=9, adjust=False).mean()
    above = (df['close'] > ema) & (df['close'].shift(1) <= ema.shift(1))
    below = (df['close'] < ema) & (df['close'].shift(1) >= ema.shift(1))
    sigs: List[Dict] = []
    for i, ts in enumerate(df.index):
        if above.iloc[i]:
            sigs.append({"ts": ts, "side": "long"})
        elif below.iloc[i]:
            sigs.append({"ts": ts, "side": "short"})
    return sigs

# -------------------------
# Example usage (commented)
# -------------------------
"""
# df = load_your_candles(...)
# best_params, best_score = optimize_vol_sl_tp(
#     df,
#     signal_func=your_setups_signal_func,   # adapt to your pipeline
#     costs=Costs(commission_bps=2.0, slippage_ticks=0.5, tick_size=0.5),
#     objective='mar',
#     use_optuna=False,
# )
# print(best_params, best_score)
"""

# ============================================================================
# Integration helper for bybit_setups_script.py (setups 9.1–9.4 + PC)
# ============================================================================

def setups_signal_func_from_lwpc(
    df: pd.DataFrame,
    ctx: Dict,
    setup_funcs: List[Callable[[pd.DataFrame], Optional[Dict]]],
    allow_armar_break: bool = True,
) -> List[Dict]:
    """
    Converts your existing setup detectors (which expect df including candle[0])
    into a signal function for the optimizer. It steps through history and, for
    each bar i, calls the setup functions on df[:i+1] so they see a proper
    candle[0]. If any returns DISPARAR, we emit a trade intent at timestamp i.

    If allow_armar_break=True, we also allow entries when the setup returns
    ARMAR at candle[-1] and the current candle[0] breaks the gatilho, exactly
    como no seu script de produção.

    Expected setup return dict (your current format):
      {
        'status': 'DISPARAR COMPRA ...' | 'DISPARAR VENDA ...' | 'ARMAR ...',
        'gatilho': float,
        'tipo': 'compra'|'venda',
        'coluna': 'high'|'low'
      }
    Only DISPARAR events (or ARMAR with rompimento em [0]) become entries.
    """
    sigs: List[Dict] = []
    if df.empty:
        return sigs

    # Ensure datetime index
    work = df.copy()
    if not isinstance(work.index, pd.DatetimeIndex):
        if 'timestamp' in work.columns:
            work = work.set_index('timestamp')
        else:
            # create synthetic index if needed
            work.index = pd.to_datetime(work.index)

    # step through bars, letting each slice end at i (candle[0])
    for i in range(MIN_BARS_SIGNAL, len(work)):

        sub = work.iloc[: i + 1].copy()
        # re-add 'timestamp' column because your setups access it later for display
        sub['timestamp'] = sub.index
        intent = None
        for fn in setup_funcs:
            try:
                out = fn(sub)
            except TypeError:
                out = fn(sub, ativo="")
            if not out or not isinstance(out, dict):
                continue
            status = str(out.get('status',''))
            if status.startswith('DISPARAR'):
                intent = out
                break
            if allow_armar_break and status.startswith('ARMAR'):
                tipo = out.get('tipo')
                coluna = out.get('coluna')
                gatilho = float(out.get('gatilho', np.nan))
                if coluna in ('high','low') and not np.isnan(gatilho):
                    # candle[0] is the last row of sub
                    c0 = sub.iloc[-1]
                    preco_atual = float(c0[coluna])
                    rompeu = (tipo == 'compra' and preco_atual > gatilho) or (tipo == 'venda' and preco_atual < gatilho)
                    if rompeu:
                        intent = {**out, 'status': status.replace('ARMAR','DISPARAR')}
                        break
        if intent:
            side = 'long' if intent.get('tipo') == 'compra' else 'short'
            sigs.append({
                'ts': work.index[i],
                'side': side,
                # Optionally could pass entry_price=intent['gatilho']
            })
    return sigs

    # Ensure datetime index
    work = df.copy()
    if not isinstance(work.index, pd.DatetimeIndex):
        if 'timestamp' in work.columns:
            work = work.set_index('timestamp')
        else:
            # create synthetic index if needed
            work.index = pd.to_datetime(work.index)

    # step through bars, letting each slice end at i (candle[0])
    for i in range(max(30, 50), len(work)):  # warmup safety for MAs
        sub = work.iloc[: i + 1].copy()
        # re-add 'timestamp' column because your setups access it later for display
        sub['timestamp'] = sub.index
        intent = None
        for fn in setup_funcs:
            try:
                out = fn(sub)
            except TypeError:
                out = fn(sub, ativo="")
            if out and isinstance(out, dict) and str(out.get('status','')).startswith('DISPARAR'):
                intent = out
                break
        if intent:
            side = 'long' if intent.get('tipo') == 'compra' else 'short'
            sigs.append({
                'ts': work.index[i],
                'side': side,
                # Entry price optional; optimizer can enter next bar open.
                # 'entry_price': float(intent.get('gatilho', work['open'].iloc[i])),
            })
    return sigs

def precompute_signals_from_lwpc_full(
    df: pd.DataFrame,
    setup_funcs: List[Callable[[pd.DataFrame], Optional[Dict]]],
) -> List[Dict]:
    """
    Pré-calcula TODOS os sinais (DISPARAR ou ARMAR+rompimento no [0]) varrendo a série uma vez.
    Reaproveitaremos esse resultado em todo o grid/CV, reduzindo o custo de O(n²) -> O(n).
    """
    # Reuso da sua função atual (que entende candle[0]), mas uma única passada:
    return setups_signal_func_from_lwpc(df, {}, setup_funcs, allow_armar_break=True)

def run_optimization_with_setups(
    df: pd.DataFrame,
    setup_funcs: List[Callable[[pd.DataFrame], Optional[Dict]]],
    objective: str = 'mar',
    use_optuna: bool = False,
) -> Tuple[Dict, Dict[str,float]]:
    """
    Glue: pré-calcula sinais uma vez e passa uma signal_func que apenas FATIA
    por período, ao invés de recalcular tudo para cada split/combinação.
    """
    # Garante índice datetime
    work = df if isinstance(df.index, pd.DatetimeIndex) else (
        df.set_index('timestamp') if 'timestamp' in df.columns else df.copy()
    )

    # 1x só: pré-calcular sinais no histórico inteiro
    pre_sigs_all = precompute_signals_from_lwpc_full(work, setup_funcs)

    # Função de sinais “barata”: apenas filtra pelo intervalo
    def signal_func(d: pd.DataFrame, ctx: Dict) -> List[Dict]:
        idx = d.index if isinstance(d.index, pd.DatetimeIndex) else pd.to_datetime(d['timestamp'])
        lo, hi = idx.min(), idx.max()
        return [s for s in pre_sigs_all if lo <= s['ts'] <= hi]

    costs = DEFAULT_COSTS
    return optimize_vol_sl_tp(
        work,
        signal_func=signal_func,
        costs=costs,
        objective=objective,
        use_optuna=use_optuna,
    )
