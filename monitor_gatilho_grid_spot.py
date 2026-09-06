# Monitor Spot Grid v2.3
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

from bybit_spot_common import get_ticker
from bybit_setups_script_hr_context_spot import (
    capturar_contexto_spot,
    ler_config_spot,
    validar_parametros_bybit,
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
    return "|".join([
        str(row.get("Par", "")).upper(),
        str(row.get("Timeframe", "")),
        str(row.get("Setup", "")),
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


def build_ready_message(row, last, ctx):
    score = _float(row.get("SCORE_TOTAL"), 0.0)
    return (
        f"GRID PRONTO | {row['Par']} {row['Timeframe']} | Setup {row['Setup']}\n"
        f"Preço: {_fmt_price(last)} | Gatilho: {_fmt_price(row.get('GATILHO'))}\n"
        f"Score: {score:.2f}\n"
        f"Faixa: {_fmt_price(row.get('LOWER'))} - {_fmt_price(row.get('UPPER'))}\n"
        f"Grids: {row.get('GRIDS', '')} | Líq/grid est.: {row.get('GRID_NET_EST_PCT', '')}%\n"
        f"TP: {_fmt_price(row.get('TP'))} | SL: {_fmt_price(row.get('SL'))}\n"
        f"TS retração: {row.get('TS_RETRACAO_PCT', '')}% | Trailing Up: {row.get('TRAILING_UP', '')}\n"
        f"Depth 1%: {ctx.get('DepthMin1Pct', '')} | Spread: {ctx.get('Spread_Pct', '')}"
    )


def build_near_message(row, last, dist_pct):
    return (
        f"APROXIMANDO GATILHO | {row['Par']} {row['Timeframe']} | Setup {row['Setup']}\n"
        f"Preço: {_fmt_price(last)}\n"
        f"Gatilho: {_fmt_price(row.get('GATILHO'))}\n"
        f"Distância: {dist_pct:.3f}%\n"
        f"Score: {_float(row.get('SCORE_TOTAL'), 0.0):.2f}\n"
        f"Faixa prevista: {_fmt_price(row.get('LOWER'))} - {_fmt_price(row.get('UPPER'))}\n"
        f"Grids: {row.get('GRIDS', '')}"
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

        grid = grid_dict(row)
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

        if send(build_ready_message(row, last, ctx), dry_run=dry_run):
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
            f"bybit_invalidos={stats['bybit_invalidos']}"
        )

    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Monitor Spot Grid v2.3")
    p.add_argument("--once", action="store_true")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--tolerance-pct", type=float, default=1.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--quiet", action="store_true")
    a = p.parse_args()

    if a.once:
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
