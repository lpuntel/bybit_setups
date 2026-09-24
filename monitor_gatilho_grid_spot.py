# Monitor Spot Grid v3.3.0
# Não envia ordens. Consome o resultado do scanner contextual Spot.

from __future__ import annotations

import argparse
import hashlib
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
    localizar_candle_setup_compra,
    ler_config_spot,
    validar_parametros_bybit,
    legacy,
)
from spot_research_log import (
    log_monitor_observation,
    log_monitor_event,
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

SCAN_FILE = BASE_DIR / "ativos_opt_hr_contexto_spot.xlsx"
STATE_FILE = BASE_DIR / "monitor_gatilho_grid_spot_state.json"
CALLBACK_STATE_FILE = BASE_DIR / "monitor_gatilho_grid_spot_callback_state.json"
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


def _fmt_setup_datetime(v):
    """Formata o timestamp do candle do setup em UTC-3 para leitura rápida no Telegram."""
    if v is None:
        return "-"
    try:
        ts = pd.to_datetime(v, utc=True)
        if pd.isna(ts):
            return "-"
        ts = ts - pd.Timedelta(hours=3)
        return ts.strftime("%d/%m/%Y %H:%M UTC-3")
    except Exception:
        s = str(v).strip()
        return s if s else "-"


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
    low = _float(row.get("LOW_SETUP"))
    gat_key = f"{gat:.12g}" if gat is not None else ""
    low_key = f"{low:.12g}" if low is not None else ""
    return "|".join([
        str(row.get("Par", "")).upper(),
        str(row.get("Timeframe", "")),
        str(row.get("Setup", "")),
        gat_key,
        low_key,
    ])


def _callback_token(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _load_callback_cursor() -> int | None:
    if not CALLBACK_STATE_FILE.exists():
        return None
    try:
        data = json.loads(CALLBACK_STATE_FILE.read_text(encoding="utf-8"))
        value = data.get("next_update_id")
        return int(value) if value is not None else None
    except Exception:
        return None


def _save_callback_cursor(next_update_id: int):
    tmp = CALLBACK_STATE_FILE.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"next_update_id": int(next_update_id)}, indent=2),
        encoding="utf-8",
    )
    tmp.replace(CALLBACK_STATE_FILE)


def send(text, dry_run=False, reply_markup=None, return_result=False):
    if dry_run:
        print("\n--- TELEGRAM DRY-RUN ---")
        print(text)
        if reply_markup:
            print("[BOTÃO] ✅ TRATADO")
        print("--- FIM ---")
        if return_result:
            return {"message_id": None}
        return True

    if not TOKEN or not CHAT_ID:
        print("[TG] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID ausentes; mensagem não enviada.")
        print(text)
        return False

    data = {"chat_id": CHAT_ID, "text": text}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    r = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data=data,
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    result = payload.get("result") or {}

    if return_result:
        return result
    return True


def _answer_callback(callback_query_id: str, text: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/answerCallbackQuery",
            data={
                "callback_query_id": callback_query_id,
                "text": text,
                "show_alert": "false",
            },
            timeout=20,
        ).raise_for_status()
    except Exception as exc:
        print(f"[TG CALLBACK] Falha ao responder callback: {exc}")


def _delete_telegram_message(chat_id, message_id) -> tuple[bool, str | None]:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/deleteMessage",
            data={"chat_id": chat_id, "message_id": message_id},
            timeout=20,
        )
        if r.ok:
            return True, None
        return False, r.text[:500]
    except Exception as exc:
        return False, str(exc)


def process_telegram_callbacks(verbose=True):
    """
    Processa somente callbacks do botão TRATADO.
    Não executa nem altera ordens; apenas registra o alerta como tratado
    e tenta apagar a própria mensagem enviada pelo bot.
    """
    if not TOKEN or not CHAT_ID:
        return 0

    params = {
        "timeout": 0,
        "allowed_updates": json.dumps(["callback_query"]),
    }
    offset = _load_callback_cursor()
    if offset is not None:
        params["offset"] = offset

    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TOKEN}/getUpdates",
            params=params,
            timeout=20,
        )
        r.raise_for_status()
        updates = (r.json() or {}).get("result") or []
    except Exception as exc:
        if verbose:
            print(f"[TG CALLBACK] ERRO getUpdates: {exc}")
        return 0

    if not updates:
        return 0

    state = load_state()
    changed = False
    handled_count = 0
    next_update_id = offset

    for update in updates:
        update_id = update.get("update_id")
        if update_id is not None:
            candidate_next = int(update_id) + 1
            next_update_id = (
                candidate_next
                if next_update_id is None
                else max(next_update_id, candidate_next)
            )

        callback = update.get("callback_query") or {}
        callback_id = str(callback.get("id") or "")
        data = str(callback.get("data") or "")
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        message_id = message.get("message_id")

        if not data.startswith("handled:"):
            if callback_id:
                _answer_callback(callback_id, "Ação não reconhecida.")
            continue

        if str(chat_id) != str(CHAT_ID):
            if callback_id:
                _answer_callback(callback_id, "Ação não autorizada.")
            continue

        token = data.split(":", 1)[1].strip()
        matched_key = None
        matched_rec = None

        for key, rec in state.items():
            if not isinstance(rec, dict):
                continue
            if (
                rec.get("telegram_callback_token") == token
                and str(rec.get("telegram_message_id")) == str(message_id)
            ):
                matched_key = key
                matched_rec = rec
                break

        if matched_rec is None:
            if callback_id:
                _answer_callback(callback_id, "Alerta antigo ou não localizado.")
            continue

        if matched_rec.get("handled", False):
            if callback_id:
                _answer_callback(callback_id, "Este alerta já foi tratado.")
            continue

        matched_rec["handled"] = True
        matched_rec["handled_at"] = pd.Timestamp.utcnow().isoformat()
        matched_rec["handled_source"] = "telegram_button"

        deleted, delete_error = _delete_telegram_message(chat_id, message_id)
        matched_rec["telegram_message_deleted"] = bool(deleted)
        if delete_error:
            matched_rec["telegram_delete_error"] = delete_error
        else:
            matched_rec.pop("telegram_delete_error", None)

        state[matched_key] = matched_rec
        changed = True
        handled_count += 1

        log_monitor_event(
            "GRID_READY_HANDLED",
            {
                "Par": matched_rec.get("par"),
                "Timeframe": matched_rec.get("timeframe"),
                "Setup": matched_rec.get("setup"),
            },
            current_price=matched_rec.get("ready_price"),
            extra={
                "candidate_key": matched_key,
                "telegram_message_id": message_id,
                "telegram_deleted": bool(deleted),
                "delete_error": delete_error,
            },
        )

        if callback_id:
            _answer_callback(
                callback_id,
                "Tratado e removido." if deleted else "Tratado; não consegui apagar a mensagem.",
            )

        if verbose:
            status = "apagada" if deleted else "mantida"
            print(
                f"[TG CALLBACK] {matched_rec.get('par')} "
                f"{matched_rec.get('timeframe')} marcado TRATADO | mensagem {status}"
            )

    if changed:
        save_state(state)

    if next_update_id is not None:
        _save_callback_cursor(next_update_id)

    return handled_count


def _operational_capacity(cfg) -> int:
    capital = _float(getattr(cfg, "capital_disponivel_usdt", 0.0), 0.0) or 0.0
    reference = _float(getattr(cfg, "capital_referencia_usdt", 0.0), 0.0) or 0.0
    if capital <= 0 or reference <= 0:
        return 0
    return max(0, int(capital // reference))


def _priority_sort_key(item):
    row = item["row"]
    return (
        -(_float(row.get("SCORE_TOTAL"), 0.0) or 0.0),
        -(_float(row.get("SCORE_LIQUIDEZ"), 0.0) or 0.0),
        -(_float(row.get("SCORE_FORCA"), 0.0) or 0.0),
        str(row.get("Par", "")),
        str(row.get("Timeframe", "")),
    )


def load_candidates():
    if not SCAN_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {SCAN_FILE}")

    df = pd.read_excel(SCAN_FILE, sheet_name=SHEET)

    required = {
        "Par", "Timeframe", "Setup", "SINAL_ORIGINAL",
        "DECISAO_SPOT", "GATILHO", "LOW_SETUP", "TICK_SIZE",
        "APROVADO_SCORE", "PARAMETROS_BYBIT_VALIDOS",
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
        "GRID_NET_EST_PCT", "GRID_INTERVAL_PRICE", "SL", "TP",
        "TS_RETRACAO_PCT", "TRAILING_UP", "TRAILING_UP_LIMIT",
        "TRAILING_UP_STEPS", "TP_EXTRA_GRIDS", "LOW_SETUP",
        "ESTRATEGIA_GRID", "REGIME_SPOT", "ATR_PCT_SPOT",
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
    par = str(row["Par"]).strip().upper()
    tf = normalize_timeframe(row["Timeframe"])
    expected_setup = str(row.get("Setup", "")).strip()
    original_trigger = _float(row.get("GATILHO"))
    original_low_setup = _float(row.get("LOW_SETUP"))
    tick_size = _float(row.get("TICK_SIZE"))
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
        reasons.append("setup_expirado")
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
    if original_low_setup is None or original_low_setup <= 0:
        reasons.append("low_setup_indisponivel")
    if tick_size is None or tick_size <= 0:
        reasons.append("tick_size_indisponivel")

    effective_trigger = original_trigger
    effective_low_setup = original_low_setup
    effective_candle_setup_ts = str(row.get("CANDLE_SETUP_TS", "") or "")

    if (
        signal
        and direction == "COMPRA"
        and setup_now == expected_setup
        and trigger_now is not None
        and trigger_now > 0
    ):
        effective_trigger = trigger_now

        low_now, candle_ts_now = localizar_candle_setup_compra(df, trigger_now)
        if low_now is not None:
            effective_low_setup = low_now
            effective_candle_setup_ts = candle_ts_now
        else:
            reasons.append("low_setup_atual_nao_localizado")

        if (
            status.startswith("ARMAR")
            and current_price is not None
            and current_price < trigger_now
        ):
            warnings.append("novo_gatilho_aguardando")

    if (
        effective_low_setup is not None
        and effective_trigger is not None
        and effective_low_setup >= effective_trigger
    ):
        reasons.append("low_setup_acima_gatilho")

    if (
        current_price is not None
        and effective_low_setup is not None
        and current_price <= effective_low_setup
    ):
        reasons.append("low_setup_rompido")

    waiting_effective = (
        current_price is not None
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
            low_setup=effective_low_setup,
            tick_size=tick_size,
            sl_buffer_ticks=cfg.sl_buffer_ticks,
            trailing_up_steps=cfg.trailing_up_steps,
            tp_extra_grids=cfg.tp_extra_grids,
        )

        if not grid_now:
            reasons.append("grid_atual_invalido")
            technical_state = "INVALIDADO"

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
        "low_setup_atual": effective_low_setup,
        "candle_setup_ts_atual": effective_candle_setup_ts,
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
                f"Low={_fmt_price(info.get('low_setup_atual')):>12} "
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
    trigger_now = technical_info.get("gatilho_atual")
    candle_setup_ts = technical_info.get("candle_setup_ts_atual") or "-"
    trailing = _bool(grid.get("TRAILING_UP"))
    trailing_limit = grid.get("TRAILING_UP_LIMIT")

    trailing_line = (
        f"Trailing Up: SIM | Limite: {_fmt_price(trailing_limit)} | "
        f"Reserva: {_fmt_int(grid.get('TRAILING_UP_STEPS'))} deslocamentos"
        if trailing
        else "Trailing Up: NÃO"
    )

    priority_rank = technical_info.get("priority_rank")
    priority_capacity = technical_info.get("priority_capacity")
    score_total = _float(row.get("SCORE_TOTAL"))
    priority_line = (
        f"Prioridade: {priority_rank}/{priority_capacity} | "
        f"Score: {score_total:.1f}\n"
        if priority_rank is not None and priority_capacity is not None and score_total is not None
        else ""
    )

    return (
        f"GRID PRONTO | {row['Par']} {row['Timeframe']} | Setup {row['Setup']}\n"
        f"{priority_line}"
        f"Setup em: {_fmt_setup_datetime(candle_setup_ts)}\n"
        f"\nFAIXA INICIAL / GRID\n"
        f"Lower: {_fmt_price(grid.get('LOWER'))} | Upper: {_fmt_price(grid.get('UPPER'))}\n"
        f"Grids: {_fmt_int(grid.get('GRIDS'))} | "
        f"Intervalo: {_fmt_price(grid.get('GRID_INTERVAL_PRICE'))} | "
        f"Líq/grid est.: {_fmt_pct_value(grid.get('GRID_NET_EST_PCT'))}\n"
        f"\nTS retração: {_fmt_pct_value(grid.get('TS_RETRACAO_PCT'), 2)}\n"
        f"\nGatilho: {_fmt_price(trigger_now)} | Preço Atual = {_fmt_price(last)}\n"
        f"\n{trailing_line}\n"
        f"\nSL: {_fmt_price(grid.get('SL'))}\n"
        f"\nTP: {_fmt_price(grid.get('TP'))}"
    )

def build_near_message(row, last, dist_pct, technical_info=None):
    technical_info = technical_info or {}
    candle_setup_ts = (
        technical_info.get("candle_setup_ts_atual")
        or row.get("CANDLE_SETUP_TS")
        or "-"
    )
    return (
        f"APROXIMANDO GATILHO | {row['Par']} {row['Timeframe']} | Setup {row['Setup']}\n"
        f"Setup em: {_fmt_setup_datetime(candle_setup_ts)}\n"
        f"Preço: {_fmt_price(last)}\n"
        f"Gatilho: {_fmt_price(row.get('GATILHO'))}\n"
        f"Distância: {dist_pct:.3f}%\n"
        f"Score: {_float(row.get('SCORE_TOTAL'), 0.0):.2f}\n"
        f"Faixa prevista: {_fmt_price(row.get('LOWER'))} - {_fmt_price(row.get('UPPER'))}\n"
        f"Grids: {_fmt_int(row.get('GRIDS'))}"
    )



def _activation_row_snapshot(row, effective_trigger=None, effective_low=None, effective_setup_ts=None):
    """Snapshot mínimo e JSON-serializável para revalidar o setup depois."""
    return {
        "Par": str(row.get("Par", "")).strip().upper(),
        "Timeframe": str(row.get("Timeframe", "")),
        "Setup": str(row.get("Setup", "")),
        "GATILHO": _float(effective_trigger, _float(row.get("GATILHO"))),
        "LOW_SETUP": _float(effective_low, _float(row.get("LOW_SETUP"))),
        "TICK_SIZE": _float(row.get("TICK_SIZE")),
        "DECISAO_SPOT": str(row.get("DECISAO_SPOT", "")),
        "ATR_PERIOD": int(_float(row.get("ATR_PERIOD"), 14) or 14),
        "CANDLE_SETUP_TS": str(
            effective_setup_ts
            or row.get("CANDLE_SETUP_TS", "")
            or ""
        ),
    }


def _activation_touch_since(par, trigger, started_at, current_price=None):
    """Detecta retorno ao gatilho depois do GRID PRONTO."""
    trigger = _float(trigger)
    if trigger is None or trigger <= 0 or not started_at:
        return False, None, None, None

    try:
        start = pd.to_datetime(started_at, utc=True)
        if pd.isna(start):
            return False, None, None, None
    except Exception:
        return False, None, None, None

    now = pd.Timestamp.now(tz="UTC")
    current_price = _float(current_price)

    if current_price is not None and current_price <= trigger:
        return True, current_price, now.isoformat(), "ticker"

    age_minutes = max(0.0, (now - start).total_seconds() / 60.0)
    interval = "1" if age_minutes <= 900 else "5"
    step_minutes = int(interval)
    limit = min(1000, max(5, int(age_minutes / step_minutes) + 5))

    try:
        df = get_kline(par, interval, limit=limit)
    except Exception:
        return False, None, None, None

    if (
        df is None
        or df.empty
        or "timestamp" not in df.columns
        or "low" not in df.columns
    ):
        return False, None, None, None

    try:
        ts = pd.to_datetime(df["timestamp"], utc=True)
        lows = pd.to_numeric(df["low"], errors="coerce")

        # Conservador: ignora o candle parcial iniciado antes do GRID PRONTO,
        # pois a mínima dele pode ter ocorrido antes do início do watch.
        cutoff = start.ceil(f"{step_minutes}min")
        hit = df.loc[(ts >= cutoff) & (lows <= trigger)].copy()

        if hit.empty:
            return False, None, None, None

        hit["_ts_utc"] = pd.to_datetime(hit["timestamp"], utc=True)
        first = hit.sort_values("_ts_utc").iloc[0]

        return (
            True,
            _float(first.get("low")),
            pd.to_datetime(first["_ts_utc"], utc=True).isoformat(),
            f"kline_{interval}m",
        )
    except Exception:
        return False, None, None, None


def _start_activation_watch(rec, row, effective_trigger, technical_info=None):
    technical_info = technical_info or {}
    now_iso = pd.Timestamp.now(tz="UTC").isoformat()

    effective_low = technical_info.get("low_setup_atual")
    effective_setup_ts = technical_info.get("candle_setup_ts_atual")

    rec["activation_state"] = "AGUARDANDO_ATIVACAO"
    rec["activation_trigger"] = _float(effective_trigger)
    rec["activation_watch_started_at"] = now_iso
    rec["activation_par"] = str(row.get("Par", "")).strip().upper()
    rec["activation_timeframe"] = str(row.get("Timeframe", ""))
    rec["activation_setup"] = str(row.get("Setup", ""))
    rec["activation_setup_ts"] = str(
        effective_setup_ts
        or row.get("CANDLE_SETUP_TS", "")
        or ""
    )
    rec["activation_touch_price"] = None
    rec["activation_touch_at"] = None
    rec["activation_row"] = _activation_row_snapshot(
        row,
        effective_trigger=effective_trigger,
        effective_low=effective_low,
        effective_setup_ts=effective_setup_ts,
    )

    return now_iso


def build_bot_trigger_touched_message(rec, touch_price, touch_at):
    return (
        f"GATILHO DO ROBÔ ATINGIDO | "
        f"{rec.get('activation_par', '-')} "
        f"{rec.get('activation_timeframe', '-')} | "
        f"Setup {rec.get('activation_setup', '-')}\n"
        f"Setup em: {_fmt_setup_datetime(rec.get('activation_setup_ts'))}\n"
        f"GRID PRONTO em: "
        f"{_fmt_setup_datetime(rec.get('activation_watch_started_at'))}\n"
        f"\nGatilho do bot: {_fmt_price(rec.get('activation_trigger'))}\n"
        f"Preço/mínima observada: {_fmt_price(touch_price)}\n"
        f"Toque detectado em: {_fmt_setup_datetime(touch_at)}\n"
        f"\nStatus: preço retornou ao gatilho após o GRID PRONTO.\n"
        f"Provável ativação do bot; confirmar na Bybit."
    )


def build_bot_not_triggered_message(rec):
    return (
        f"ROBÔ NÃO FOI ACIONADO PELO PREÇO | "
        f"{rec.get('activation_par', '-')} "
        f"{rec.get('activation_timeframe', '-')} | "
        f"Setup {rec.get('activation_setup', '-')}\n"
        f"Setup em: {_fmt_setup_datetime(rec.get('activation_setup_ts'))}\n"
        f"GRID PRONTO em: "
        f"{_fmt_setup_datetime(rec.get('activation_watch_started_at'))}\n"
        f"\nGatilho do bot: {_fmt_price(rec.get('activation_trigger'))}\n"
        f"\nO setup deixou de estar tecnicamente válido sem o preço "
        f"retornar ao gatilho após o GRID PRONTO.\n"
        f"Se o bot ainda estiver aguardando na Bybit, revise/encerre "
        f"para liberar o capital reservado."
    )


def process_activation_watch(
    row,
    rec,
    cfg,
    current_price,
    dry_run=False,
    verbose=True,
):
    if rec.get("activation_state") != "AGUARDANDO_ATIVACAO":
        return False, None

    par = str(
        rec.get("activation_par") or row.get("Par", "")
    ).strip().upper()

    trigger = _float(rec.get("activation_trigger"))
    started_at = rec.get("activation_watch_started_at")

    touched, touch_price, touch_at, touch_source = _activation_touch_since(
        par,
        trigger,
        started_at,
        current_price=current_price,
    )

    if touched:
        if send(
            build_bot_trigger_touched_message(
                rec,
                touch_price,
                touch_at,
            ),
            dry_run=dry_run,
        ):
            rec["activation_state"] = "GATILHO_ATINGIDO"
            rec["activation_touch_price"] = touch_price
            rec["activation_touch_at"] = touch_at
            rec["activation_touch_source"] = touch_source

            log_monitor_event(
                "BOT_TRIGGER_TOUCHED",
                row,
                current_price=current_price,
                extra={
                    "activation_trigger": trigger,
                    "touch_price": touch_price,
                    "touch_at": touch_at,
                    "touch_source": touch_source,
                    "watch_started_at": started_at,
                },
            )

            if verbose:
                print(
                    f"[BOT] {par} {row.get('Timeframe')} "
                    f"gatilho atingido após GRID PRONTO"
                )

            return True, "GATILHO_ATINGIDO"

        return False, None

    technical_ok, technical_reasons, technical_info = technical_revalidation(
        row,
        cfg,
        current_price=current_price,
    )

    terminal_setup = (
        "setup_expirado" in technical_reasons
        or any(
            str(r).startswith("setup_mudou:")
            for r in technical_reasons
        )
        or any(
            str(r).startswith("sinal_contrario:")
            for r in technical_reasons
        )
    )

    if not technical_ok and terminal_setup:
        if send(
            build_bot_not_triggered_message(rec),
            dry_run=dry_run,
        ):
            rec["activation_state"] = "SETUP_ENCERRADO_SEM_ATIVACAO"
            rec["activation_ended_at"] = (
                pd.Timestamp.now(tz="UTC").isoformat()
            )
            rec["activation_end_reasons"] = list(technical_reasons)

            log_monitor_event(
                "BOT_SETUP_ENDED_UNTRIGGERED",
                row,
                current_price=current_price,
                reasons=technical_reasons,
                info=technical_info,
                extra={
                    "activation_trigger": trigger,
                    "watch_started_at": started_at,
                },
            )

            if verbose:
                print(
                    f"[BOT] {par} {row.get('Timeframe')} "
                    f"setup encerrado sem retorno ao gatilho"
                )

            return True, "SETUP_ENCERRADO_SEM_ATIVACAO"

    return False, None



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
            "overshoot_logged": False,
            "expired_logged": False,
            "last_decision": decision,
            "activation_state": "NAO_INICIADO",
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

        if rec.get("ready_sent", False):
            rec["activation_state"] = "LEGACY_READY"

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
        "aguardando_efetivo": 0,
        "prealertas": 0,
        "prealertas_bloq_tecnico": 0,
        "prontos": 0,
        "bloqueados_revalidacao": 0,
        "bloqueados_tecnico": 0,
        "setups_expirados": 0,
        "gatilho_ultrapassado": 0,
        "bybit_invalidos": 0,
        "gatilhos_bot_atingidos": 0,
        "setups_sem_ativacao": 0,
        "prontos_deferidos_prioridade": 0,
        "slots_capital": _operational_capacity(cfg),
        "slots_ocupados": 0,
    }

    ready_queue = []

    for _, row in candidates.iterrows():
        par = str(row["Par"]).strip().upper()
        decision = str(row["DECISAO_SPOT"]).strip().upper()
        key = candidate_key(row)
        active_keys.add(key)

        rec = state.setdefault(key, {
            "prealert_sent": False,
            "ready_sent": False,
            "overshoot_logged": False,
            "expired_logged": False,
            "last_decision": decision,
            "activation_state": "NAO_INICIADO",
        })
        rec["last_decision"] = decision
        rec["par"] = par
        rec["timeframe"] = str(row.get("Timeframe", ""))
        rec["setup"] = str(row.get("Setup", ""))

        if "priority_slot_active" not in rec:
            rec["priority_slot_active"] = False
            changed = True
        if "priority_deferred_logged" not in rec:
            rec["priority_deferred_logged"] = False
            changed = True
        if "dynamic_waiting" not in rec:
            rec["dynamic_waiting"] = False
            changed = True
        if "handled" not in rec:
            rec["handled"] = False
            changed = True

        score_now = _float(row.get("SCORE_TOTAL"), 0.0) or 0.0
        if rec.get("priority_slot_active", False):
            if rec.get("priority_score") != score_now:
                rec["priority_score"] = score_now
                changed = True

        if "activation_state" not in rec:
            rec["activation_state"] = (
                "LEGACY_READY"
                if rec.get("ready_sent", False)
                else "NAO_INICIADO"
            )
            changed = True

        ticker = get_ticker(par)
        last = _float(ticker.get("lastPrice"), 0.0)
        gat = _float(row.get("GATILHO"), 0.0)

        log_monitor_observation(
            row, last, decision, rec
        )

        if not last or not gat:
            continue

        if (
            decision == "AGUARDAR_GATILHO"
            and last < gat
            and rec.get("priority_slot_active", False)
        ):
            rec["priority_slot_active"] = False
            rec["priority_left_reason"] = "voltou_abaixo_gatilho"
            changed = True

        if (
            not getattr(cfg, "bot_watch_automatico", False)
            and rec.get("activation_state") == "AGUARDANDO_ATIVACAO"
        ):
            rec["activation_state"] = "LEGACY_READY"
            changed = True

        if rec.get("activation_state") == "AGUARDANDO_ATIVACAO":
            if decision == "GRID":
                stats["grid"] += 1
            elif decision == "AGUARDAR_GATILHO":
                stats["aguardando"] += 1

            lifecycle_changed, lifecycle_event = process_activation_watch(
                row,
                rec,
                cfg,
                current_price=last,
                dry_run=dry_run,
                verbose=verbose,
            )

            if lifecycle_changed:
                changed = True

            if lifecycle_event == "GATILHO_ATINGIDO":
                stats["gatilhos_bot_atingidos"] += 1
            elif lifecycle_event == "SETUP_ENCERRADO_SEM_ATIVACAO":
                stats["setups_sem_ativacao"] += 1

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
                pre_ok, pre_reasons, pre_info = technical_revalidation(
                    row,
                    cfg,
                    current_price=last,
                )

                if not pre_ok:
                    stats["prealertas_bloq_tecnico"] += 1
                    if "setup_expirado" in pre_reasons:
                        stats["setups_expirados"] += 1
                        if not rec.get("expired_logged", False):
                            log_monitor_event(
                                "SETUP_EXPIRED",
                                row,
                                current_price=last,
                                reasons=["setup_expirado"],
                                info=pre_info,
                                extra={"motivo": "sem_evento_setup_atual"},
                            )
                            rec["expired_logged"] = True
                            changed = True
                    log_monitor_event(
                        "PREALERT_BLOCKED_TECHNICAL",
                        row,
                        current_price=last,
                        reasons=pre_reasons,
                        info=pre_info,
                    )
                    if verbose:
                        print(
                            f"[PRÉ-ALERTA TÉCNICO] {par} {row['Timeframe']} bloqueado: "
                            + ";".join(pre_reasons)
                        )
                else:
                    if rec.get("expired_logged", False):
                        rec["expired_logged"] = False
                        changed = True

                    if getattr(cfg, "prealert_telegram", False):
                        if send(
                            build_near_message(row, last, dist_abs, pre_info),
                            dry_run=dry_run,
                        ):
                            rec["prealert_sent"] = True
                            stats["prealertas"] += 1
                            changed = True
                            log_monitor_event(
                                "PREALERT_SENT",
                                row,
                                current_price=last,
                                info=pre_info,
                                extra={"distance_pct": dist_abs},
                            )
                    else:
                        rec["prealert_sent"] = True
                        changed = True
                        log_monitor_event(
                            "PREALERT_SUPPRESSED",
                            row,
                            current_price=last,
                            info=pre_info,
                            extra={"distance_pct": dist_abs},
                        )

            if last < gat:
                continue

        elif decision == "GRID":
            stats["grid"] += 1

            if not _bool(row.get("PARAMETROS_BYBIT_VALIDOS")):
                scan_reason = str(
                    row.get("PARAMETROS_BYBIT_MOTIVO", "")
                ).strip()
                scan_reasons = {
                    r.strip()
                    for r in scan_reason.split(";")
                    if r.strip()
                }

                # "entry_acima_mercado" é condição temporal:
                # o setup já disparou, mas o preço recuou abaixo do gatilho.
                # O monitor deve revalidar ao vivo e, se o setup continuar
                # válido, tratá-lo como aguardando gatilho.
                if scan_reasons != {"entry_acima_mercado"}:
                    stats["bybit_invalidos"] += 1
                    log_monitor_event(
                        "SCAN_PARAMS_INVALID",
                        row,
                        current_price=last,
                        reasons=[scan_reason],
                    )
                    continue
        else:
            continue

        hard_ok, hard_reasons, ctx = hard_revalidation(par, cfg, ticker)
        if not hard_ok:
            stats["bloqueados_revalidacao"] += 1
            log_monitor_event(
                "HARD_REVALIDATION_BLOCKED",
                row,
                current_price=last,
                reasons=hard_reasons,
                extra={
                    "spread_pct": ctx.get("Spread_Pct"),
                    "depth_1pct": ctx.get("DepthMin1Pct"),
                    "turnover24h": ctx.get("Turnover24h"),
                },
            )
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
            if "setup_expirado" in technical_reasons:
                stats["setups_expirados"] += 1
                if not rec.get("expired_logged", False):
                    log_monitor_event(
                        "SETUP_EXPIRED",
                        row,
                        current_price=last,
                        reasons=["setup_expirado"],
                        info=technical_info,
                        extra={"motivo": "sem_evento_setup_atual"},
                    )
                    rec["expired_logged"] = True
                    changed = True
            log_monitor_event(
                "TECHNICAL_BLOCKED",
                row,
                current_price=last,
                reasons=technical_reasons,
                info=technical_info,
            )
            if verbose:
                print(
                    f"[TÉCNICO] {par} {row['Timeframe']} bloqueado: "
                    + ";".join(technical_reasons)
                )
            continue

        if rec.get("expired_logged", False):
            rec["expired_logged"] = False
            changed = True

        if technical_info.get("technical_state") == "AGUARDANDO_GATILHO":
            stats["aguardando_efetivo"] += 1

            if rec.get("priority_slot_active", False):
                rec["priority_slot_active"] = False
                rec["priority_left_reason"] = "preco_abaixo_gatilho_efetivo"
                changed = True

            if not rec.get("dynamic_waiting", False):
                rec["dynamic_waiting"] = True
                changed = True
                log_monitor_event(
                    "GRID_DYNAMIC_WAITING",
                    row,
                    current_price=last,
                    info=technical_info,
                    extra={
                        "gatilho_efetivo": technical_info.get("gatilho_atual"),
                        "motivo": "preco_abaixo_gatilho_efetivo",
                    },
                )

            continue

        if rec.get("dynamic_waiting", False):
            rec["dynamic_waiting"] = False
            changed = True

        effective_trigger = _float(
            technical_info.get("gatilho_atual"),
            gat,
        )

        overshoot_active = False
        if effective_trigger and last > effective_trigger:
            overshoot_pct = (last / effective_trigger - 1.0) * 100.0
            if overshoot_pct > tolerance_pct:
                overshoot_active = True
                stats["gatilho_ultrapassado"] += 1

                # Registra somente a transição para "ultrapassado".
                if not rec.get("overshoot_logged", False):
                    if verbose:
                        print(
                            f"[GATILHO ULTRAPASSADO] {par} {row['Timeframe']} "
                            f"preço={_fmt_price(last)} "
                            f"gatilho={_fmt_price(effective_trigger)} "
                            f"dist={overshoot_pct:.3f}% "
                            f"> limite={tolerance_pct:.3f}%"
                        )
                    rec["overshoot_logged"] = True
                    changed = True
                    log_monitor_event(
                        "TRIGGER_OVERSHOOT",
                        row,
                        current_price=last,
                        info=technical_info,
                        extra={
                            "overshoot_pct": overshoot_pct,
                            "tolerance_pct": tolerance_pct,
                        },
                    )

                continue

        # Se voltou para dentro da faixa, rearma o log.
        if not overshoot_active and rec.get("overshoot_logged", False):
            rec["overshoot_logged"] = False
            changed = True

        grid = technical_info.get("grid_atual") or {}
        bybit_ok, bybit_reason = validar_parametros_bybit(
            grid,
            last,
            "DISPARAR COMPRA",
        )
        if not bybit_ok:
            stats["bybit_invalidos"] += 1
            log_monitor_event(
                "GRID_PARAMS_INVALID_AFTER_REVALIDATION",
                row,
                current_price=last,
                reasons=[bybit_reason],
                info=technical_info,
            )
            if verbose:
                print(
                    f"[BYBIT] {par} {row['Timeframe']} inválido: {bybit_reason}"
                )
            continue

        ready_queue.append({
            "key": key,
            "row": row.copy(),
            "rec": rec,
            "last": last,
            "ctx": ctx,
            "grid": grid,
            "technical_info": technical_info,
            "effective_trigger": effective_trigger,
        })

    capacity = _operational_capacity(cfg)
    max_per_par = max(1, int(getattr(cfg, "max_por_par", 1) or 1))

    ranked_ready = sorted(ready_queue, key=_priority_sort_key)
    desired_items = []
    desired_par_counts = {}

    for item in ranked_ready:
        if len(desired_items) >= capacity:
            break
        par = str(item["row"].get("Par", "")).upper()
        if desired_par_counts.get(par, 0) >= max_per_par:
            continue
        desired_items.append(item)
        desired_par_counts[par] = desired_par_counts.get(par, 0) + 1

    desired_keys = {item["key"] for item in desired_items}
    rank_by_key = {
        item["key"]: idx
        for idx, item in enumerate(desired_items, start=1)
    }

    # A shortlist representa os melhores GRID_READY tecnicamente válidos
    # neste ciclo. ready_sent apenas controla repetição de Telegram.
    for k, rec in state.items():
        if rec.get("priority_slot_active", False) and k not in desired_keys:
            rec["priority_slot_active"] = False
            rec["priority_left_reason"] = "fora_shortlist_atual"
            changed = True

    for item in ranked_ready:
        key = item["key"]
        row = item["row"]
        rec = item["rec"]
        par = str(row.get("Par", "")).upper()
        score = _float(row.get("SCORE_TOTAL"), 0.0) or 0.0

        if key not in desired_keys:
            stats["prontos_deferidos_prioridade"] += 1
            if not rec.get("priority_deferred_logged", False):
                rec["priority_deferred_logged"] = True
                changed = True
                log_monitor_event(
                    "GRID_READY_DEFERRED_PRIORITY",
                    row,
                    current_price=item["last"],
                    info=item["technical_info"],
                    extra={
                        "score_total": score,
                        "capacity": capacity,
                        "shortlist_ativa": len(desired_items),
                        "max_por_par": max_per_par,
                    },
                )
            continue

        rank = rank_by_key[key]
        item["technical_info"]["priority_rank"] = rank
        item["technical_info"]["priority_capacity"] = capacity

        was_active = rec.get("priority_slot_active", False)
        already_sent = rec.get("ready_sent", False)

        rec["priority_deferred_logged"] = False
        rec["priority_score"] = score
        rec["priority_rank"] = rank

        if already_sent:
            if not was_active:
                rec["priority_slot_active"] = True
                rec["priority_selected_at"] = pd.Timestamp.utcnow().isoformat()
                rec["priority_left_reason"] = None
                changed = True
                log_monitor_event(
                    "GRID_SHORTLIST_PROMOTED_EXISTING",
                    row,
                    current_price=item["last"],
                    info=item["technical_info"],
                    extra={
                        "score_total": score,
                        "rank": rank,
                        "capacity": capacity,
                    },
                )
            continue

        callback_token = _callback_token(key)
        reply_markup = {
            "inline_keyboard": [[
                {
                    "text": "✅ TRATADO",
                    "callback_data": f"handled:{callback_token}",
                }
            ]]
        }

        send_result = send(
            build_ready_message(
                row,
                item["last"],
                item["ctx"],
                item["grid"],
                item["technical_info"],
            ),
            dry_run=dry_run,
            reply_markup=reply_markup,
            return_result=True,
        )

        if send_result:
            rec["ready_sent"] = True
            rec["handled"] = False
            rec["handled_at"] = None
            rec["telegram_callback_token"] = callback_token
            rec["telegram_message_id"] = send_result.get("message_id")
            rec["ready_price"] = item["last"]
            rec["ready_at"] = pd.Timestamp.utcnow().isoformat()
            rec["priority_slot_active"] = True
            rec["priority_selected_at"] = pd.Timestamp.utcnow().isoformat()
            rec["priority_left_reason"] = None

            if getattr(cfg, "bot_watch_automatico", False):
                _start_activation_watch(
                    rec,
                    row,
                    item["effective_trigger"],
                    technical_info=item["technical_info"],
                )
            else:
                rec["activation_state"] = "NAO_INICIADO"

            stats["prontos"] += 1
            changed = True

            log_monitor_event(
                "GRID_READY",
                row,
                current_price=item["last"],
                info=item["technical_info"],
                extra={
                    "spread_pct": item["ctx"].get("Spread_Pct"),
                    "depth_1pct": item["ctx"].get("DepthMin1Pct"),
                    "score_total": score,
                    "capital_disponivel_usdt": _float(
                        getattr(cfg, "capital_disponivel_usdt", 0.0)
                    ),
                    "capital_referencia_usdt": _float(
                        getattr(cfg, "capital_referencia_usdt", 0.0)
                    ),
                    "capacity": capacity,
                    "rank": rank,
                },
            )

    stats["slots_ocupados"] = sum(
        1
        for item in desired_items
        if item["rec"].get("priority_slot_active", False)
        or item["rec"].get("ready_sent", False)
    )

    stale = [k for k in state if k not in active_keys]

    for k in stale:
        stale_rec = state.get(k, {})

        if (
            not getattr(cfg, "bot_watch_automatico", False)
            and stale_rec.get("activation_state") == "AGUARDANDO_ATIVACAO"
        ):
            stale_rec["activation_state"] = "LEGACY_READY"
            changed = True

        if stale_rec.get("activation_state") == "AGUARDANDO_ATIVACAO":
            snapshot = stale_rec.get("activation_row") or {}

            if snapshot:
                stale_row = pd.Series(snapshot)
                stale_par = str(snapshot.get("Par", "")).strip().upper()

                try:
                    stale_ticker = get_ticker(stale_par)
                    stale_last = _float(stale_ticker.get("lastPrice"), 0.0)
                except Exception:
                    stale_last = 0.0

                if stale_last:
                    lifecycle_changed, lifecycle_event = process_activation_watch(
                        stale_row,
                        stale_rec,
                        cfg,
                        current_price=stale_last,
                        dry_run=dry_run,
                        verbose=verbose,
                    )

                    if lifecycle_changed:
                        changed = True

                    if lifecycle_event == "GATILHO_ATINGIDO":
                        stats["gatilhos_bot_atingidos"] += 1
                    elif lifecycle_event == "SETUP_ENCERRADO_SEM_ATIVACAO":
                        stats["setups_sem_ativacao"] += 1

            if stale_rec.get("activation_state") == "AGUARDANDO_ATIVACAO":
                continue

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
            f"aguardando_efetivo={stats['aguardando_efetivo']} "
            f"prealertas={stats['prealertas']} "
            f"prealert_tecnico_bloq={stats['prealertas_bloq_tecnico']} "
            f"prontos={stats['prontos']} "
            f"revalidacao_bloq={stats['bloqueados_revalidacao']} "
            f"tecnico_bloq={stats['bloqueados_tecnico']} "
            f"setup_expirado={stats['setups_expirados']} "
            f"gatilho_ultrapassado={stats['gatilho_ultrapassado']} "
            f"bybit_invalidos={stats['bybit_invalidos']} "
            f"bot_gatilho_atingido={stats['gatilhos_bot_atingidos']} "
            f"bot_setup_sem_ativacao={stats['setups_sem_ativacao']} "
            f"grid_deferidos={stats['prontos_deferidos_prioridade']} "
            f"shortlist={stats['slots_ocupados']}/{stats['slots_capital']}"
        )

    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Monitor Spot Grid v3.3.0")
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
                if not a.dry_run:
                    process_telegram_callbacks(verbose=not a.quiet)
                once(
                    tolerance_pct=a.tolerance_pct,
                    dry_run=a.dry_run,
                    verbose=not a.quiet,
                )
            except Exception as exc:
                print(f"[MONITOR] ERRO: {exc}")
            time.sleep(max(60, a.interval))
