#entry_engine.py
from app.core.scoring import _safe_float


def evaluate_entry(
    stock: dict,
    technicals: dict,
    candidate_score: int,
    profile: dict,
) -> dict:
    stock = stock or {}
    technicals = technicals or {}
    profile = profile or {}

    risk_flags = profile.get("risk_flags", []) or []
    candidate_quality = profile.get("candidate_quality", "D")
    setup_type = profile.get("setup_type", "low_quality_noise")

    price = _safe_float(technicals.get("price"))
    sma20 = _safe_float(technicals.get("sma20"))
    sma50 = _safe_float(technicals.get("sma50"))
    rsi14 = _safe_float(technicals.get("rsi14"))
    atr_pct = _safe_float(technicals.get("atr_pct"))
    volume_ratio = _safe_float(technicals.get("volume_ratio"))
    momentum_20 = _safe_float(technicals.get("momentum_20"))
    momentum_60 = _safe_float(technicals.get("momentum_60"))
    avg_dollar_volume_20 = _safe_float(technicals.get("avg_dollar_volume_20"))

    entry_score = 0
    reasons = []

    if price is None or sma20 is None or sma50 is None or rsi14 is None:
        return {
            "entry_score": -5,
            "timing_state": "avoid",
            "action": "avoid",
            "entry_reasons": ["missing_core_technicals"],
        }

    # ---------------------------
    # Hårda stop-villkor först
    # ---------------------------
    if price <= sma20:
        reasons.append("below_sma20_hard_block")
    if sma20 < sma50:
        reasons.append("below_trend_structure_hard_block")
    if rsi14 > 78:
        reasons.append("too_extended_hard_block")
    if atr_pct is not None and atr_pct > 8:
        reasons.append("too_volatile_hard_block")
    if avg_dollar_volume_20 is not None and avg_dollar_volume_20 < 5_000_000:
        reasons.append("too_illiquid_hard_block")

    hard_blocks = {
        "below_sma20_hard_block",
        "below_trend_structure_hard_block",
        "too_extended_hard_block",
        "too_volatile_hard_block",
        "too_illiquid_hard_block",
    }

    # ---------------------------
    # Trend / struktur
    # ---------------------------
    if price > sma20:
        entry_score += 2
        reasons.append("price_above_sma20")
    else:
        entry_score -= 3
        reasons.append("price_below_sma20")

    if sma20 >= sma50:
        entry_score += 2
        reasons.append("sma20_above_or_equal_sma50")
    else:
        entry_score -= 3
        reasons.append("sma20_below_sma50")

    # Bonus om pris ligger tydligt över trend
    if price > sma20 and sma20 > sma50:
        entry_score += 1
        reasons.append("full_trend_alignment")

    # ---------------------------
    # RSI
    # ---------------------------
    if 54 <= rsi14 <= 68:
        entry_score += 2
        reasons.append("healthy_rsi")
    elif 50 <= rsi14 < 54:
        entry_score += 1
        reasons.append("acceptable_rsi")
    elif 68 < rsi14 <= 74:
        entry_score += 0
        reasons.append("slightly_extended_rsi")
    elif 74 < rsi14 <= 78:
        entry_score -= 1
        reasons.append("extended_rsi")
    elif rsi14 > 78:
        entry_score -= 3
        reasons.append("too_extended_rsi")
    elif rsi14 < 35:
        entry_score -= 2
        reasons.append("too_oversold_rsi")

    # ---------------------------
    # Momentum
    # ---------------------------
    if momentum_20 is not None:
        if momentum_20 > 6:
            entry_score += 2
            reasons.append("strong_short_momentum")
        elif momentum_20 > 2:
            entry_score += 1
            reasons.append("positive_short_momentum")
        elif momentum_20 < -4:
            entry_score -= 2
            reasons.append("negative_short_momentum")

    if momentum_60 is not None:
        if momentum_60 > 12:
            entry_score += 1
            reasons.append("strong_medium_momentum")
        elif momentum_60 < -10:
            entry_score -= 1
            reasons.append("weak_medium_momentum")

    # ---------------------------
    # Volume confirmation
    # ---------------------------
    if volume_ratio is not None:
        if volume_ratio >= 1.8:
            entry_score += 2
            reasons.append("strong_volume_confirmation")
        elif volume_ratio >= 1.2:
            entry_score += 1
            reasons.append("ok_volume_confirmation")
        elif volume_ratio < 0.85:
            entry_score -= 1
            reasons.append("weak_volume")

    # ---------------------------
    # Volatilitet
    # ---------------------------
    if atr_pct is not None:
        if atr_pct < 2.5:
            entry_score += 1
            reasons.append("controlled_volatility")
        elif atr_pct > 8:
            entry_score -= 3
            reasons.append("too_volatile")
        elif atr_pct > 6:
            entry_score -= 1
            reasons.append("high_volatility")

    # ---------------------------
    # Likviditet
    # ---------------------------
    if avg_dollar_volume_20 is not None:
        if avg_dollar_volume_20 >= 20_000_000:
            entry_score += 1
            reasons.append("good_liquidity")
        elif avg_dollar_volume_20 < 5_000_000:
            entry_score -= 3
            reasons.append("poor_liquidity")

    # ---------------------------
    # Profile
    # ---------------------------
    if candidate_quality == "A+":
        entry_score += 3
        reasons.append("quality_A_plus")
    elif candidate_quality == "A":
        entry_score += 2
        reasons.append("quality_A")
    elif candidate_quality == "B":
        entry_score += 0
        reasons.append("quality_B")
    elif candidate_quality in {"C", "D"}:
        entry_score -= 2
        reasons.append(f"quality_{candidate_quality}")

    if setup_type == "trend_continuation":
        entry_score += 2
        reasons.append("trend_continuation_setup")
    elif setup_type == "early_breakout":
        entry_score += 1
        reasons.append("early_breakout_setup")
    elif setup_type == "weak_breakdown":
        entry_score -= 3
        reasons.append("weak_breakdown_setup")
    elif setup_type == "low_quality_noise":
        entry_score -= 2
        reasons.append("noise_setup")

    hard_risk_flags = {"missing_data", "thin_liquidity", "price_below_trend", "negative_news"}
    soft_risk_flags = {"extended_rsi", "overstretched", "high_volatility", "weak_financials", "oversold"}

    for flag in risk_flags:
        if flag in hard_risk_flags:
            entry_score -= 2
        elif flag in soft_risk_flags:
            entry_score -= 1

    if candidate_score >= 10:
        entry_score += 2
        reasons.append("very_strong_candidate_score")
    elif candidate_score >= 7:
        entry_score += 1
        reasons.append("strong_candidate_score")
    elif candidate_score <= 0:
        entry_score -= 2
        reasons.append("weak_candidate_score")

    # ---------------------------
    # Action
    # ---------------------------
    timing_state = "watch_only"
    action = "watch"

    sell_conditions = [
        price < sma20 < sma50,
        rsi14 > 80,
        momentum_20 is not None and momentum_20 < -6,
        "price_below_trend" in risk_flags and candidate_quality in {"C", "D"},
    ]

    hard_block_count = sum(1 for r in reasons if r in hard_blocks)

    if any(sell_conditions):
        timing_state = "avoid"
        action = "sell_candidate"

    elif hard_block_count >= 2:
        timing_state = "avoid"
        action = "avoid"

    elif (
        entry_score >= 9
        and candidate_quality in {"A+", "A"}
        and price > sma20
        and sma20 >= sma50
    ):
        timing_state = "ready"
        action = "buy_ready"

    elif (
        entry_score >= 7
        and candidate_quality in {"A+", "A", "B"}
    ):
        timing_state = "almost_ready"
        action = "watch"

    elif (
        entry_score >= 4
        and candidate_quality in {"A+", "A", "B"}
    ):
        timing_state = "watch_only"
        action = "hold_candidate"

    elif entry_score >= 1:
        timing_state = "watch_only"
        action = "watch"

    else:
        timing_state = "avoid"
        action = "avoid"

    return {
        "entry_score": entry_score,
        "timing_state": timing_state,
        "action": action,
        "entry_reasons": reasons,
    }