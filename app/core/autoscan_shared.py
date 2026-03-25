#autoscan_shared.py
from datetime import datetime, timezone


def now_utc():
    return datetime.now(timezone.utc)


def dedupe_keep_order(items):
    seen = set()
    out = []
    for item in items or []:
        if not item:
            continue
        item = str(item).upper().strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out




def to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default


def to_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def quality_rank(value: str) -> int:
    mapping = {
        "A+": 5,
        "A": 4,
        "B": 3,
        "C": 2,
        "D": 1,
    }
    return mapping.get(str(value or "").upper(), 0)


def fmt_price(value) -> str:
    v = to_float(value, None)
    if v is None:
        return "-"
    return f"{v:.2f}"


def fmt_score_plain(value) -> str:
    try:
        v = int(value)
    except Exception:
        return "-"
    return f"{v:+d}"


def normalize_stock(stock: dict) -> dict:
    normalized = dict(stock or {})
    for key in ("latestClose", "PE", "marketCap", "beta", "trailingEps", "dividendYield"):
        normalized[key] = to_float(normalized.get(key), 0.0)
    return normalized


def build_pipeline_analysis(stock: dict) -> dict:
    stock = stock or {}
    sym = (stock.get("symbol") or "").upper().strip()

    pipeline_signal = stock.get("_pipeline_signal", stock.get("signal", "Håll"))
    pipeline_score = stock.get("_pipeline_final_score", stock.get("final_score", 0))
    pipeline_technicals = stock.get("_pipeline_technicals", stock.get("technicals", {})) or {}
    pipeline_scores = stock.get("_pipeline_scores", stock.get("scores", {})) or {}
    pipeline_score_details = stock.get("_pipeline_score_details", stock.get("score_details", {})) or {}

    return {
        "symbol": sym,
        "signal": pipeline_signal,
        "total_score": pipeline_score,
        "candidate_score": stock.get("_pipeline_candidate_score", stock.get("candidate_score", pipeline_score)),
        "entry_score": stock.get("_pipeline_entry_score", stock.get("entry_score", 0)),
        "candidate_quality": stock.get("_pipeline_candidate_quality", stock.get("candidate_quality", "C")),
        "setup_type": stock.get("_pipeline_setup_type", stock.get("setup_type", "unknown")),
        "timing_state": stock.get("_pipeline_timing_state", stock.get("timing_state", "unknown")),
        "action": stock.get("_pipeline_action", stock.get("action", "watch")),
        "positive_flags": stock.get("_pipeline_positive_flags", stock.get("positive_flags", [])) or [],
        "risk_flags": stock.get("_pipeline_risk_flags", stock.get("risk_flags", [])) or [],
        "entry_reasons": stock.get("_pipeline_entry_reasons", stock.get("entry_reasons", [])) or [],
        "retention_score": stock.get("_pipeline_retention_score", stock.get("retention_score", pipeline_score)),
        "replacement_score": stock.get("_pipeline_replacement_score", stock.get("replacement_score", pipeline_score)),
        "rank": stock.get("_pipeline_rank", stock.get("rank")),
        "raw_technicals": pipeline_technicals,
        "pipeline_scores": pipeline_scores,
        "pipeline_score_details": pipeline_score_details,
        "timestamp": now_utc().isoformat(),
    }


def score_bucket(score: int) -> str:
    score = to_int(score, 0)
    if score <= -5:
        return "very_bad"
    if score <= -2:
        return "weak"
    if score <= 2:
        return "neutral"
    if score <= 6:
        return "good"
    return "strong"


def retention_bucket(score: int) -> str:
    score = to_int(score, 0)
    if score <= 0:
        return "broken"
    if score <= 3:
        return "weak"
    if score <= 6:
        return "ok"
    return "strong"


def build_decision_snapshot(
    *,
    signal: str,
    action: str,
    timing_state: str,
    pressure: str | None,
    exit_mode: str,
    exit_stage: int,
    score: int,
    retention_score: int,
) -> dict:
    return {
        "signal": signal,
        "action": action,
        "timing_state": timing_state,
        "pressure": pressure,
        "exit_mode": exit_mode,
        "exit_stage": to_int(exit_stage, 0),
        "score_bucket": score_bucket(score),
        "retention_bucket": retention_bucket(retention_score),
    }


def classify_state_label(prev: dict, curr: dict) -> str:
    if not prev:
        return "actionable"

    if curr.get("exit_mode") == "full":
        return "critical"

    if curr.get("exit_mode") in {"soft", "watch"} and prev.get("exit_mode") != curr.get("exit_mode"):
        return "actionable"

    if prev.get("exit_stage", 0) < curr.get("exit_stage", 0):
        return "weakening"

    if prev.get("exit_stage", 0) > curr.get("exit_stage", 0):
        return "improving"

    if prev.get("score_bucket") != curr.get("score_bucket"):
        order = {"very_bad": 0, "weak": 1, "neutral": 2, "good": 3, "strong": 4}
        prev_rank = order.get(prev.get("score_bucket"), 0)
        curr_rank = order.get(curr.get("score_bucket"), 0)
        return "improving" if curr_rank > prev_rank else "weakening"

    if prev.get("signal") != curr.get("signal"):
        return "actionable"

    if prev.get("action") != curr.get("action"):
        return "actionable"

    return "unchanged"


def is_material_change(prev: dict, curr: dict) -> tuple[bool, list[str]]:
    changed = []

    keys = [
        "signal",
        "action",
        "timing_state",
        "pressure",
        "exit_mode",
        "exit_stage",
        "score_bucket",
        "retention_bucket",
    ]

    for key in keys:
        if prev.get(key) != curr.get(key):
            changed.append(key)

    return (len(changed) > 0, changed)