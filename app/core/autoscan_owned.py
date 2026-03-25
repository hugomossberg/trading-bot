#autoscan_owned.py
from app.core.autoscan_shared import (
    build_decision_snapshot,
    build_pipeline_analysis,
    classify_state_label,
    is_material_change,
    now_utc,
    quality_rank,
    to_int,
)


def resolve_owned_input(sym: str, by_sym: dict, state: dict) -> dict:
    current = by_sym.get(sym)
    previous = (state.get("owned_snapshot", {}) or {}).get(sym)

    if current:
        analysis = build_pipeline_analysis(current)
        analysis["data_source"] = "current_scan"
        analysis["missing_from_pipeline"] = False
        analysis["missing_from_pipeline_count"] = 0
        analysis["stale_snapshot"] = False
        return analysis

    if previous:
        analysis = {
            "symbol": sym,
            "signal": previous.get("signal", "Håll"),
            "action": previous.get("action", "hold_position"),
            "total_score": previous.get("total_score", 0),
            "candidate_quality": previous.get("candidate_quality"),
            "timing_state": previous.get("timing_state", "owned_snapshot"),
            "raw_technicals": previous.get("raw_technicals") or {},
            "retention_score": previous.get("retention_score", 999),
            "replacement_score": previous.get("replacement_score", 999),
            "entry_score": previous.get("entry_score", 0),
            "entry_reasons": previous.get("entry_reasons") or [],
            "data_source": "owned_snapshot",
            "missing_from_pipeline": True,
            "missing_from_pipeline_count": int(previous.get("missing_from_pipeline_count", 0) or 0) + 1,
            "stale_snapshot": True,
        }
        return analysis

    return {
        "symbol": sym,
        "signal": "Håll",
        "action": "hold_position",
        "total_score": 0,
        "candidate_quality": None,
        "timing_state": "owned_no_scan_refresh",
        "raw_technicals": {},
        "retention_score": 999,
        "replacement_score": 999,
        "entry_score": 0,
        "entry_reasons": [],
        "data_source": "minimal_fallback",
        "missing_from_pipeline": True,
        "missing_from_pipeline_count": 999,
        "stale_snapshot": True,
    }


def classify_exit_pressure(analysis: dict, current_pos: float) -> str:
    if current_pos <= 0:
        return "healthy"

    action = str(analysis.get("action") or "").strip().lower()
    timing_state = str(analysis.get("timing_state") or "").strip().lower()
    q_rank = quality_rank(analysis.get("candidate_quality"))
    score = to_int(analysis.get("total_score"), 0)
    retention = to_int(analysis.get("retention_score"), score)
    missing_count = to_int(analysis.get("missing_from_pipeline_count"), 0)

    if action == "exit_ready":
        return "emergency"
    if score <= -4:
        return "emergency"
    if retention <= 0 and timing_state == "avoid":
        return "emergency"
    if action == "sell_candidate":
        return "bearish"
    if action == "exit_watch":
        return "weak"
    if timing_state == "avoid" and (q_rank <= 2 or retention <= 3):
        return "bearish"
    if action in {"watch"} and retention <= 3:
        return "weak"
    if analysis.get("missing_from_pipeline") and missing_count >= 3:
        return "weak"

    return "healthy"


def advance_long_exit_state(exit_state: dict, analysis: dict, pressure: str) -> dict:
    stage = to_int(exit_state.get("stage"), 0)
    bearish_count = to_int(exit_state.get("bearish_count"), 0)
    soft_exit_done = bool(exit_state.get("soft_exit_done", False))

    score = to_int(analysis.get("total_score"), 0)
    retention = to_int(analysis.get("retention_score"), score)
    action = str(analysis.get("action") or "").strip().lower()
    timing_state = str(analysis.get("timing_state") or "").strip().lower()

    if pressure == "emergency":
        stage = 4
        bearish_count += 1
    elif pressure == "bearish":
        bearish_count += 1
        stage = max(stage, min(4, bearish_count))
    elif pressure == "weak":
        bearish_count = max(1, bearish_count)
        stage = max(stage, 1)
        if stage > 2:
            stage = 2
    else:
        bearish_count = 0
        if action in {"buy_ready", "hold_candidate", "hold_position"} and retention >= 4:
            stage = 0
        else:
            stage = max(0, stage - 1)
        if stage == 0:
            soft_exit_done = False

    exit_state["stage"] = stage
    exit_state["bearish_count"] = bearish_count
    exit_state["last_action"] = action or "hold"
    exit_state["last_score"] = score
    exit_state["last_retention_score"] = retention
    exit_state["last_timing_state"] = timing_state or "unknown"
    exit_state["soft_exit_done"] = soft_exit_done
    exit_state["updated_at"] = now_utc().isoformat()
    return exit_state


def decide_long_exit(exit_state: dict, pressure: str) -> tuple[str, str]:
    stage = to_int(exit_state.get("stage"), 0)
    bearish_count = to_int(exit_state.get("bearish_count"), 0)
    soft_exit_done = bool(exit_state.get("soft_exit_done", False))
    last_score = to_int(exit_state.get("last_score"), 0)
    last_retention = to_int(exit_state.get("last_retention_score"), 0)

    if pressure == "emergency":
        return "full_exit", "emergency_exit"
    if last_score <= -5:
        return "full_exit", "very_low_score_exit"
    if last_retention <= 0:
        return "full_exit", "retention_broken"
    if stage >= 4 and bearish_count >= 3:
        return "full_exit", "confirmed_full_exit"
    if stage >= 3 and bearish_count >= 2 and not soft_exit_done:
        return "soft_exit", "confirmed_soft_exit"
    if stage >= 2:
        return "watch_exit", "confirmed_bearish_watch"
    if stage >= 1:
        return "watch_exit", "first_exit_warning"
    return "hold", "healthy"


def owned_label_from_exit_mode(exit_mode: str) -> str:
    if exit_mode == "full":
        return "EXIT"
    if exit_mode == "soft":
        return "EXIT SOON"
    if exit_mode == "watch":
        return "EXIT WATCH"
    return "HOLD"


def build_owned_review_row(
    *,
    sym: str,
    raw: dict,
    analysis: dict,
    current_pos: float,
    effective_signal: str,
    exit_mode: str,
    owned_reason: str,
    exit_state: dict,
) -> dict:
    owned_label = owned_label_from_exit_mode(exit_mode)

    if current_pos < 0 and effective_signal == "Köp":
        owned_label = "EXIT"
    elif str(analysis.get("action") or "").lower() == "buy_ready" and current_pos > 0 and exit_mode == "hold":
        owned_label = "ADD"
    elif analysis.get("missing_from_pipeline") and exit_mode == "hold":
        owned_label = "HOLD"

    return {
        "symbol": sym,
        "name": raw.get("name") or raw.get("companyName") or sym,
        "held_position": current_pos,
        "signal": effective_signal,
        "action": analysis.get("action"),
        "candidate_quality": analysis.get("candidate_quality"),
        "entry_score": analysis.get("entry_score", 0),
        "retention_score": analysis.get("retention_score", analysis.get("total_score", 0)),
        "replacement_score": analysis.get("replacement_score", analysis.get("total_score", 0)),
        "timing_state": analysis.get("timing_state"),
        "entry_reasons": analysis.get("entry_reasons") or [],
        "raw_technicals": analysis.get("raw_technicals") or {},
        "score": analysis.get("total_score", 0),
        "exit_mode": exit_mode,
        "exit_stage": exit_state.get("stage", 0),
        "exit_reason": owned_reason,
        "owned_label": owned_label,
        "data_source": analysis.get("data_source", "unknown"),
        "missing_from_pipeline": bool(analysis.get("missing_from_pipeline")),
        "missing_from_pipeline_count": analysis.get("missing_from_pipeline_count", 0),
        "updated_at": now_utc().isoformat(),
    }

def build_owned_decision_state(
    *,
    prev_decision: dict,
    effective_signal: str,
    analysis: dict,
    pressure: str,
    exit_mode: str,
    exit_state: dict,
):
    curr_decision = build_decision_snapshot(
        signal=effective_signal,
        action=str(analysis.get("action") or "hold_position"),
        timing_state=str(analysis.get("timing_state") or "unknown"),
        pressure=pressure,
        exit_mode=exit_mode,
        exit_stage=exit_state.get("stage", 0),
        score=analysis.get("total_score", 0),
        retention_score=analysis.get("retention_score", analysis.get("total_score", 0)),
    )

    state_label = classify_state_label(prev_decision, curr_decision)
    curr_decision["state_label"] = state_label
    material_change, changed_fields = is_material_change(prev_decision, curr_decision)
    return curr_decision, state_label, material_change, changed_fields