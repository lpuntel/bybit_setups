# Monitor Spot Grid v2.8
# Não envia ordens. Consome o resultado do scanner contextual Spot.

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from bybit_spot_common import get_ticker, get_kline, normalize_timeframe, spot_grid_parameters
from bybit_setups_script_hr_context_spot import (
    capturar_contexto_spot,
    escolher_setup_spot,
    ler_config_spot,
    validar_parametros_bybit,
    legacy,
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

SCAN_FILE = BASE_DIR / "ativos_opt_hr_contexto_spot.xlsx"
STATE_FILE = BASE_DIR / "monitor_gatilho_grid_spot_state.json"
SHEET = "Setups Spot"

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def _bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return False
    return str(v).strip().upper() in {"TRUE", "1", "SIM", "YES", "Y"}


def _float(v, default=None):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return default
        return float(v)
    except Exception:
        return default


def _fmt_price(v):
    x = _float(v)
    if x is None:
        return "-"
    if abs(x) >= 1000:
        return f"{x:,.2f}"
    if abs(x) >= 1:
        return f"{x:.6f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def _fmt_int(v):
    x = _float(v)
    return "-" if x is None else str(int(round(x)))


def _fmt_pct_value(v, decimals=3):
    x = _float(v)
    return "-" if x is None else f"{x:.{decimals}f}%"


def _fmt_pct_fraction(v, decimals=3):
    x = _float(v)
    return "-" if x is None else f"{x * 100:.{decimals}f}%"


def _fmt_depth(v):
    x = _float(v)
    return "-" if x is None else f"{x:,.0f} USDT"


def _fmt_yesno(v):
    return "SIM" if _bool(v) else "NÃO"


def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(STATE_FILE)


def candidate_key(row):
    gat = _float(row.get("GATILHO"))
    gat_key = f"{gat:.12g}" if gat is not None else ""
    return "|".join([
        str(row.get("Par", "")).upper(),
        str(row.get("Timeframe", "")),
        str(row.get("Setup", "")),
        gat_key,
    ])


def send(text, dry_run=False):
    if dry_run:
        print("\n--- TELEGRAM DRY-RUN ---")
        print(text)
        print("--- FIM ---")
        return True

    if not TOKEN or not CHAT_ID:
        print("[TG] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID ausentes; mensagem não enviada.")
        print(text)
        return False

    r = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "text": text},
        timeout=20,
    )
    r.raise_for_status()
    return True


def load_candidates():
    if not SCAN_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {SCAN_FILE}")

    df = pd.read_excel(SCAN_FILE, sheet_name=SHEET)

    required = {
        "Par", "Timeframe", "Setup", "SINAL_ORIGINAL",
        "DECISAO_SPOT", "GATILHO", "APROVADO_SCORE",
        "PARAMETROS_BYBIT_VALIDOS",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"Colunas ausentes em {SHEET}: {missing}")

    mask = (
        df["SINAL_ORIGINAL"].astype(str).str.upper().eq("COMPRA")
        & df["APROVADO_SCORE"].apply(_bool)
        & df["DECISAO_SPOT"].isin(["GRID", "AGUARDAR_GATILHO"])
    )

    return df.loc[mask].copy()


def grid_dict(row):
    keys = [
        "ENTRY", "LOWER", "UPPER", "GRIDS", "GRID_SPACING_PCT",
        "GRID_NET_EST_PCT", "SL", "TP", "TS_RETRACAO_PCT",
        "TRAILING_UP", "ESTRATEGIA_GRID", "REGIME_SPOT", "ATR_PCT_SPOT",
    ]
    return {k: row.get(k) for k in keys if k in row.index}


def hard_revalidation(par, cfg, ticker):
    ctx = capturar_contexto_spot(par, cfg, ticker)
    reasons = []

    turnover = _float(ctx.get("Turnover24h"), 0.0)
    spread = _float(ctx.get("Spread_Pct"))
    depth = _float(ctx.get("DepthMin1Pct"))

    if turnover < cfg.min_turnover24h_usdt:
        reasons.append("turnover24h_baixo")
    if spread is not None and spread > cfg.max_spread_pct:
        reasons.append("spread_alto")
    if depth is not None and depth < cfg.min_depth_1pct_usdt:
        reasons.append("depth_baixo")

    return len(reasons) == 0, reasons, ctx


def technical_revalidation(row, cfg, current_price=None):
    # Revalida sem exigir persistência do evento de setup em candles seguintes.
    par = str(row["Par"]).strip().upper()
    tf = normalize_timeframe(row["Timeframe"])
    expected_setup = str(row.get("Setup", "")).strip()
    original_trigger = _float(row.get("GATILHO"))
    decision = str(row.get("DECISAO_SPOT", "")).strip().upper()

    limit = min(
        1000,
        max(
            legacy.PERIODOS_MINIMO + 10,
            cfg.kline_limit,
            cfg.percentile_lookback + 50,
        ),
    )

    try:
        df = get_kline(par, tf, limit=limit)
    except Exception as exc:
        return False, [f"candles_erro:{type(exc).__name__}"], {}

    if df is None or df.empty or len(df) < legacy.PERIODOS_MINIMO:
        return False, ["candles_insuficientes"], {}

    try:
        df["MME9"] = df["close"].ewm(span=9).mean()
        df["MMA21"] = df["close"].rolling(21).mean()
        df = legacy.enriquecer_candles_contexto(df, cfg)
        signal = escolher_setup_spot(df, par)
    except Exception as exc:
        return False, [f"tecnico_erro:{type(exc).__name__}"], {}

    if current_price is None:
        current_price = _float(df.iloc[-1].get("close"))
    else:
        current_price = _float(current_price)

    status = ""
    direction = ""
    setup_now = ""
    trigger_now = None

    if signal:
        status = str(signal.get("status", "")).upper().strip()
        direction = str(signal.get("tipo", "")).upper().strip()
        parts = status.split()
        setup_now = parts[2] if len(parts) >= 3 else ""
        trigger_now = _float(signal.get("gatilho"))

    reasons = []
    warnings = []

    if not signal:
        warnings.append("sem_evento_setup_atual")
    else:
        if direction == "VENDA":
            reasons.append(f"sinal_contrario:{status or 'VENDA'}")
        elif direction == "COMPRA" and setup_now and setup_now != expected_setup:
            reasons.append(f"setup_mudou:{expected_setup}->{setup_now}")

    last_closed = df.iloc[-2]
    atr_pct_real = _float(last_closed.get("ATR_PCT"))
    atr_pct_real = atr_pct_real * 100.0 if atr_pct_real is not None else None

    atr_period = int(_float(row.get("ATR_PERIOD"), 14) or 14)
    atr_series = legacy.compute_atr(df, period=atr_period, method="wilder")
    atr_m1 = _float(atr_series.iloc[-2])

    slope_now = _float(
        legacy.calcular_slope_mme9(
            df,
            periodos=legacy.SLOPE_MME9_PERIODOS,
        )
    )

    if atr_pct_real is None:
        reasons.append("atr_indisponivel")
    else:
        if atr_pct_real < cfg.atr_close_min_pct:
            reasons.append("atr_close_baixo")
        if atr_pct_real > cfg.atr_close_max_pct:
            reasons.append("atr_close_alto")

    if atr_m1 is None or atr_m1 <= 0:
        reasons.append("atr_absoluto_invalido")

    if original_trigger is None or original_trigger <= 0:
        reasons.append("gatilho_original_invalido")

    effective_trigger = original_trigger

    if (
        signal
        and direction == "COMPRA"
        and setup_now == expected_setup
        and trigger_now is not None
        and trigger_now > 0
    ):
        effective_trigger = trigger_now

        if status.startswith("ARMAR") and current_price is not None:
            if current_price < trigger_now:
                if not (
                    decision == "AGUARDAR_GATILHO"
                    and original_trigger is not None
                    and current_price < original_trigger
                ):
                    reasons.append("novo_gatilho_nao_atingido")

    # Se existe um evento atual do MESMO setup, o gatilho atual passa a
    # prevalecer sobre o gatilho que estava na planilha do scan.
    # Isso evita manter AGUARDANDO quando o setup atual já está DISPARAR.
    waiting_effective = (
        decision == "AGUARDAR_GATILHO"
        and current_price is not None
        and effective_trigger is not None
        and current_price < effective_trigger
    )

    if not waiting_effective:
        if (
            current_price is not None
            and effective_trigger is not None
            and current_price < effective_trigger
        ):
            reasons.append("preco_abaixo_gatilho_efetivo")

    grid_now = {}

    if waiting_effective and not reasons:
        technical_state = "AGUARDANDO_GATILHO"
    elif not reasons:
        technical_state = (
            "VALIDADO_EVENTO_ATUAL"
            if signal
            else "VALIDADO_SEM_EVENTO_ATUAL"
        )

        grid_now = spot_grid_parameters(
            entry=effective_trigger,
            atr=atr_m1,
            slope_pct=slope_now or 0.0,
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
    else:
        technical_state = "INVALIDADO"

    info = {
        "technical_state": technical_state,
        "setup_atual": setup_now,
        "status_atual": status,
        "direcao_atual": direction,
        "gatilho_original": original_trigger,
        "gatilho_evento_atual": trigger_now,
        "gatilho_atual": effective_trigger,
        "preco_atual": current_price,
        "atr_pct_atual": atr_pct_real,
        "atr_m1_atual": atr_m1,
        "slope_atual": slope_now,
        "grid_atual": grid_now,
        "warnings": warnings,
        "candle_fechado": str(last_closed.get("timestamp", "")),
    }

    return len(reasons) == 0, reasons, info


def technical_check_all(verbose=True):
    cfg = ler_config_spot()
    candidates = load_candidates()

    ok_count = 0
    blocked_count = 0
    waiting_count = 0

    print("=" * 110)
    print("REVALIDAÇÃO TÉCNICA ATUAL")
    print("=" * 110)

    for _, row in candidates.iterrows():
        par = str(row["Par"]).strip().upper()

        try:
            ticker = get_ticker(par)
            current_price = _float(ticker.get("lastPrice"))
        except Exception:
            current_price = None

        ok, reasons, info = technical_revalidation(
            row,
            cfg,
            current_price=current_price,
        )

        state = info.get("technical_state", "INVALIDADO")

        if ok:
            ok_count += 1
            if state == "AGUARDANDO_GATILHO":
                waiting_count += 1
            status_txt = state
        else:
            blocked_count += 1
            status_txt = ";".join(reasons)

        warnings = ",".join(info.get("warnings", [])) or "-"

        if verbose:
            print(
                f"{str(row['Par']):<14} "
                f"{str(row['Timeframe']):>4} "
                f"{str(row['Setup']):>4} | "
                f"{status_txt:<38} | "
                f"evento={info.get('status_atual') or '-':<20} "
                f"ATR={_fmt_pct_value(info.get('atr_pct_atual'), 2):>8} "
                f"Gat={_fmt_price(info.get('gatilho_atual')):>12} "
                f"Grids={_fmt_int((info.get('grid_atual') or {}).get('GRIDS')):>3} "
                f"Aviso={warnings}"
            )

    print("-" * 110)
    print(
        f"TOTAL={len(candidates)} | "
        f"TECNICO_OK={ok_count} | "
        f"AGUARDANDO={waiting_count} | "
        f"TECNICO_BLOQUEADO={blocked_count}"
    )


def build_ready_message(row, last, ctx, grid, technical_info):
    score = _float(row.get("SCORE_TOTAL"), 0.0)
    trigger_now = technical_info.get("gatilho_atual")
    atr_pct_now = technical_info.get("atr_pct_atual")

    return (
        f"GRID PRONTO | {row['Par']} {row['Timeframe']} | Setup {row['Setup']}\n"
        f"Preço: {_fmt_price(last)} | Gatilho atual: {_fmt_price(trigger_now)}\n"
        f"Score scan: {score:.2f} | ATR atual: {_fmt_pct_value(atr_pct_now, 2)}\n"
        f"Faixa atual: {_fmt_price(grid.get('LOWER'))} - {_fmt_price(grid.get('UPPER'))}\n"
        f"Grids: {_fmt_int(grid.get('GRIDS'))} | "
        f"Líq/grid est.: {_fmt_pct_value(grid.get('GRID_NET_EST_PCT'))}\n"
        f"TP: {_fmt_price(grid.get('TP'))} | SL: {_fmt_price(grid.get('SL'))}\n"
        f"TS retração: {_fmt_pct_value(grid.get('TS_RETRACAO_PCT'), 2)} | "
        f"Trailing Up: {_fmt_yesno(grid.get('TRAILING_UP'))}\n"
        f"Depth 1%: {_fmt_depth(ctx.get('DepthMin1Pct'))} | "
        f"Spread: {_fmt_pct_fraction(ctx.get('Spread_Pct'))}"
    )


def build_near_message(row, last, dist_pct):
    return (
        f"APROXIMANDO GATILHO | {row['Par']} {row['Timeframe']} | Setup {row['Setup']}\n"
        f"Preço: {_fmt_price(last)}\n"
        f"Gatilho: {_fmt_price(row.get('GATILHO'))}\n"
        f"Distância: {dist_pct:.3f}%\n"
        f"Score: {_float(row.get('SCORE_TOTAL'), 0.0):.2f}\n"
        f"Faixa prevista: {_fmt_price(row.get('LOWER'))} - {_fmt_price(row.get('UPPER'))}\n"
        f"Grids: {_fmt_int(row.get('GRIDS'))}"
    )


def prime_state(tolerance_pct=1.0):
    candidates = load_candidates()
    state = {}
    ready_count = 0
    near_count = 0

    for _, row in candidates.iterrows():
        key = candidate_key(row)
        decision = str(row["DECISAO_SPOT"]).strip().upper()
        rec = {
            "prealert_sent": False,
            "ready_sent": False,
            "last_decision": decision,
        }

        par = str(row["Par"]).strip().upper()
        ticker = get_ticker(par)
        last = _float(ticker.get("lastPrice"), 0.0)
        gat = _float(row.get("GATILHO"), 0.0)

        if decision == "GRID" and _bool(row.get("PARAMETROS_BYBIT_VALIDOS")):
            rec["ready_sent"] = True
            rec["ready_price"] = last
            rec["ready_at"] = pd.Timestamp.utcnow().isoformat()
            ready_count += 1

        elif decision == "AGUARDAR_GATILHO" and last and gat:
            dist_abs = abs(last / gat - 1.0) * 100.0

            if last >= gat:
                rec["ready_sent"] = True
                rec["ready_price"] = last
                rec["ready_at"] = pd.Timestamp.utcnow().isoformat()
                ready_count += 1
            elif dist_abs <= tolerance_pct:
                rec["prealert_sent"] = True
                near_count += 1

        state[key] = rec

    save_state(state)
    print(
        f"[PRIME] estado inicializado sem Telegram | "
        f"candidatos={len(candidates)} "
        f"já_prontos={ready_count} "
        f"já_próximos={near_count}"
    )


def once(tolerance_pct=1.0, dry_run=False, verbose=True):
    cfg = ler_config_spot()
    candidates = load_candidates()
    state = load_state()

    active_keys = set()
    changed = False

    stats = {
        "candidatos": len(candidates),
        "grid": 0,
        "aguardando": 0,
        "prealertas": 0,
        "prontos": 0,
        "bloqueados_revalidacao": 0,
        "bloqueados_tecnico": 0,
        "bybit_invalidos": 0,
    }

    for _, row in candidates.iterrows():
        par = str(row["Par"]).strip().upper()
        decision = str(row["DECISAO_SPOT"]).strip().upper()
        key = candidate_key(row)
        active_keys.add(key)

        rec = state.setdefault(key, {
            "prealert_sent": False,
            "ready_sent": False,
            "last_decision": decision,
        })
        rec["last_decision"] = decision

        ticker = get_ticker(par)
        last = _float(ticker.get("lastPrice"), 0.0)
        gat = _float(row.get("GATILHO"), 0.0)

        if not last or not gat:
            continue

        if decision == "AGUARDAR_GATILHO":
            stats["aguardando"] += 1

            dist_signed = (last / gat - 1.0) * 100.0
            dist_abs = abs(dist_signed)

            if (
                last < gat
                and dist_abs <= tolerance_pct
                and not rec.get("prealert_sent", False)
            ):
                if send(build_near_message(row, last, dist_abs), dry_run=dry_run):
                    rec["prealert_sent"] = True
                    stats["prealertas"] += 1
                    changed = True

            if last < gat or rec.get("ready_sent", False):
                continue

        elif decision == "GRID":
            stats["grid"] += 1

            if rec.get("ready_sent", False):
                continue

            if not _bool(row.get("PARAMETROS_BYBIT_VALIDOS")):
                stats["bybit_invalidos"] += 1
                continue
        else:
            continue

        hard_ok, hard_reasons, ctx = hard_revalidation(par, cfg, ticker)
        if not hard_ok:
            stats["bloqueados_revalidacao"] += 1
            if verbose:
                print(
                    f"[REVALIDAÇÃO] {par} {row['Timeframe']} bloqueado: "
                    + ";".join(hard_reasons)
                )
            continue

        technical_ok, technical_reasons, technical_info = technical_revalidation(
            row,
            cfg,
            current_price=last,
        )
        if not technical_ok:
            stats["bloqueados_tecnico"] += 1
            if verbose:
                print(
                    f"[TÉCNICO] {par} {row['Timeframe']} bloqueado: "
                    + ";".join(technical_reasons)
                )
            continue

        grid = technical_info.get("grid_atual") or {}
        bybit_ok, bybit_reason = validar_parametros_bybit(
            grid,
            last,
            "DISPARAR COMPRA",
        )
        if not bybit_ok:
            stats["bybit_invalidos"] += 1
            if verbose:
                print(
                    f"[BYBIT] {par} {row['Timeframe']} inválido: {bybit_reason}"
                )
            continue

        if send(
            build_ready_message(row, last, ctx, grid, technical_info),
            dry_run=dry_run,
        ):
            rec["ready_sent"] = True
            rec["ready_price"] = last
            rec["ready_at"] = pd.Timestamp.utcnow().isoformat()
            stats["prontos"] += 1
            changed = True

    stale = [k for k in state if k not in active_keys]
    for k in stale:
        del state[k]
        changed = True

    if changed and not dry_run:
        save_state(state)

    if verbose:
        print(
            "[MONITOR] "
            f"candidatos={stats['candidatos']} "
            f"grid={stats['grid']} "
            f"aguardando={stats['aguardando']} "
            f"prealertas={stats['prealertas']} "
            f"prontos={stats['prontos']} "
            f"revalidacao_bloq={stats['bloqueados_revalidacao']} "
            f"tecnico_bloq={stats['bloqueados_tecnico']} "
            f"bybit_invalidos={stats['bybit_invalidos']}"
        )

    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Monitor Spot Grid v2.8")
    p.add_argument("--once", action="store_true")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--tolerance-pct", type=float, default=1.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--prime-state", action="store_true")
    p.add_argument("--technical-check", action="store_true")
    p.add_argument("--quiet", action="store_true")
    a = p.parse_args()

    if a.technical_check:
        technical_check_all(verbose=not a.quiet)
    elif a.prime_state:
        prime_state(tolerance_pct=a.tolerance_pct)
    elif a.once:
        once(
            tolerance_pct=a.tolerance_pct,
            dry_run=a.dry_run,
            verbose=not a.quiet,
        )
    else:
        while True:
            try:
                once(
                    tolerance_pct=a.tolerance_pct,
                    dry_run=a.dry_run,
                    verbose=not a.quiet,
                )
            except Exception as exc:
                print(f"[MONITOR] ERRO: {exc}")
            time.sleep(max(60, a.interval))
