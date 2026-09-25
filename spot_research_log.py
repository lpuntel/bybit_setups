"""
Research log passivo do scanner/monitor Spot.

- Não envia ordens.
- Não altera decisões do scanner ou monitor.
- Escreve somente JSONL append-only em research_logs/.
- Se houver falha de logging, a produção continua normalmente.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

BASE_DIR = Path(__file__).resolve().parent
RESEARCH_DIR = BASE_DIR / "research_logs"


def _clean(value: Any):
    if value is None:
        return None

    if isinstance(value, (str, bool, int)):
        return value

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()

    if isinstance(value, Mapping):
        return {str(k): _clean(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_clean(v) for v in value]

    # numpy/pandas scalars
    try:
        item = value.item()
        if item is not value:
            return _clean(item)
    except Exception:
        pass

    # pandas Timestamp / datetime-like
    try:
        if hasattr(value, "isoformat"):
            return value.isoformat()
    except Exception:
        pass

    # NaN genérico
    try:
        if value != value:
            return None
    except Exception:
        pass

    return str(value)


def _float(value, default=None):
    try:
        if value is None:
            return default
        x = float(value)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().upper() in {
        "TRUE", "1", "SIM", "YES", "Y"
    }


def research_candidate_key(row: Mapping[str, Any]) -> str:
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


def _append(source: str, event_type: str, payload: Mapping[str, Any]):
    """
    Best-effort: NUNCA deixa falha do research log interromper produção.
    """
    try:
        RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

        now = datetime.now(timezone.utc)

        record = {
            "logged_at_utc": now.isoformat(),
            "source": source,
            "event_type": event_type,
            **dict(payload),
        }

        path = RESEARCH_DIR / (
            f"{source.lower()}_{now.strftime('%Y%m%d')}.jsonl"
        )

        line = json.dumps(
            _clean(record),
            ensure_ascii=False,
            separators=(",", ":"),
        )

        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    except Exception:
        # Logging de pesquisa não pode afetar scanner/monitor.
        return


def log_scanner_candidate(result: Mapping[str, Any]):
    """
    Registra somente candidatos que o monitor efetivamente poderia consumir.
    """
    try:
        direction = str(
            result.get("SINAL_ORIGINAL", "")
        ).upper().strip()

        decision = str(
            result.get("DECISAO_SPOT", "")
        ).upper().strip()

        approved = _bool(
            result.get("APROVADO_SCORE")
        )

        if direction != "COMPRA":
            return

        if not approved:
            return

        if decision not in {"GRID", "AGUARDAR_GATILHO"}:
            return

        grid_keys = [
            "ENTRY", "LOWER", "UPPER", "GRIDS",
            "GRID_INTERVAL_PRICE", "GRID_SPACING_PCT",
            "GRID_NET_EST_PCT", "SL", "TP",
            "TRAILING_UP", "TRAILING_UP_LIMIT",
            "TRAILING_UP_STEPS", "TP_EXTRA_GRIDS",
            "TS_RETRACAO_PCT", "REGIME_SPOT",
            "ATR_PCT_SPOT", "GRIDS_TECNICOS",
            "GRID_CAPITAL_CAP", "MIN_ORDER_AMT",
            "CAPITAL_LIMIT_USDT", "CAPITAL_BUFFER_PCT",
            "CAPITAL_MIN_EST_USDT",
        ]

        payload = {
            "candidate_key": research_candidate_key(result),
            "par": result.get("Par"),
            "timeframe": result.get("Timeframe"),
            "setup": result.get("Setup"),
            "scan_timestamp": result.get("Time Stamp"),
            "armar_disparar": result.get("ARMAR/DISPARAR"),
            "decision": decision,
            "trigger_original": result.get("GATILHO"),
            "price_scan": result.get("PRECO_ATUAL"),
            "low_setup_original": result.get("LOW_SETUP"),
            "candle_setup_ts": result.get("CANDLE_SETUP_TS"),
            "tick_size": result.get("TICK_SIZE"),
            "atr_period": result.get("ATR_PERIOD"),
            "atr_m1": result.get("ATR_M1"),
            "atr_pct": result.get("ATR_PCT"),
            "slope_mme9_pct": result.get("SLOPE_MME9_PCT"),
            "score_total": result.get("SCORE_TOTAL"),
            "score_liquidez": result.get("SCORE_LIQUIDEZ"),
            "score_regime": result.get("SCORE_REGIME"),
            "score_forca": result.get("SCORE_FORCA"),
            "rank_forca": result.get("RANK_FORCA"),
            "spread_pct": result.get("SPREAD_PCT"),
            "depth_1pct": result.get("DEPTH_1PCT"),
            "turnover24h": result.get("TURNOVER24H"),
            "parametros_bybit_validos": result.get(
                "PARAMETROS_BYBIT_VALIDOS"
            ),
            "parametros_bybit_motivo": result.get(
                "PARAMETROS_BYBIT_MOTIVO"
            ),
            "grid": {
                k: result.get(k)
                for k in grid_keys
                if k in result
            },
        }

        _append(
            "scanner",
            "SCANNER_CANDIDATE",
            payload,
        )

    except Exception:
        return


def _row_base(row: Mapping[str, Any]):
    trigger = _float(row.get("GATILHO"))

    return {
        "candidate_key": research_candidate_key(row),
        "par": row.get("Par"),
        "timeframe": row.get("Timeframe"),
        "setup": row.get("Setup"),
        "scan_timestamp": row.get("Time Stamp"),
        "decision_scan": row.get("DECISAO_SPOT"),
        "trigger_original": trigger,
        "low_setup_original": row.get("LOW_SETUP"),
        "candle_setup_ts_original": row.get("CANDLE_SETUP_TS"),
        "score_scan": row.get("SCORE_TOTAL"),
        "atr_scan": row.get("ATR_PCT"),
        "slope_scan": row.get("SLOPE_MME9_PCT"),
    }


def log_monitor_observation(
    row: Mapping[str, Any],
    current_price,
    decision,
    state_record: Mapping[str, Any] | None = None,
):
    try:
        payload = _row_base(row)

        trigger = _float(row.get("GATILHO"))
        price = _float(current_price)

        dist = None
        if price is not None and trigger not in (None, 0):
            dist = (price / trigger - 1.0) * 100.0

        payload.update({
            "decision": decision,
            "current_price": price,
            "distance_original_trigger_pct": dist,
            "prealert_sent": (
                state_record.get("prealert_sent")
                if state_record else None
            ),
            "ready_sent": (
                state_record.get("ready_sent")
                if state_record else None
            ),
            "overshoot_logged": (
                state_record.get("overshoot_logged")
                if state_record else None
            ),
        })

        _append(
            "monitor",
            "MONITOR_OBSERVATION",
            payload,
        )

    except Exception:
        return


def log_monitor_event(
    event_type: str,
    row: Mapping[str, Any],
    current_price=None,
    reasons=None,
    info: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
):
    try:
        payload = _row_base(row)

        payload["current_price"] = _float(current_price)
        payload["reasons"] = list(reasons or [])

        info = dict(info or {})
        grid = info.get("grid_atual") or {}

        payload.update({
            "technical_state": info.get("technical_state"),
            "setup_atual": info.get("setup_atual"),
            "status_atual": info.get("status_atual"),
            "direcao_atual": info.get("direcao_atual"),
            "gatilho_evento_atual": info.get(
                "gatilho_evento_atual"
            ),
            "gatilho_efetivo": info.get("gatilho_atual"),
            "low_setup_efetivo": info.get(
                "low_setup_atual"
            ),
            "candle_setup_ts_efetivo": info.get(
                "candle_setup_ts_atual"
            ),
            "atr_pct_atual": info.get("atr_pct_atual"),
            "atr_m1_atual": info.get("atr_m1_atual"),
            "slope_atual": info.get("slope_atual"),
            "warnings": info.get("warnings"),
            "candle_fechado": info.get("candle_fechado"),
            "grid_atual": grid,
        })

        if extra:
            payload["extra"] = dict(extra)

        _append(
            "monitor",
            event_type,
            payload,
        )

    except Exception:
        return
