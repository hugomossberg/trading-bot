from app.core.autoscan_shared import build_pipeline_analysis, quality_rank, to_float


def candidate_bucket(stock: dict) -> str | None:
    analysis = build_pipeline_analysis(stock or {})
    action = str(analysis.get("action") or "").strip().lower()
    quality = str(analysis.get("candidate_quality") or "").upper()
    retention_score = int(analysis.get("retention_score", analysis.get("total_score", 0)) or 0)
    entry_score = int(analysis.get("entry_score", 0) or 0)
    total_score = int(analysis.get("total_score", 0) or 0)

    if (
        action == "buy_ready"
        and quality in {"A+", "A", "B"}
        and entry_score >= 5
        and total_score >= 3
    ):
        return "entry"

    if (
        action in {"watch", "hold_candidate"}
        and quality in {"A+", "A", "B"}
        and retention_score >= 2
        and entry_score >= 0
    ):
        return "watch"

    if (
        action in {"watch", "hold_candidate"}
        and quality in {"A+", "A", "B", "C"}
        and retention_score >= 0
    ):
        return "fallback"

    return None


def is_affordable(stock: dict, qty: int, max_order_value: float) -> bool:
    price = to_float((stock or {}).get("latestClose"), None)
    if price is None or price <= 0:
        return False
    return (price * qty) <= max_order_value


def is_allowed_replacement_action(action: str, held_pos: float = 0.0) -> bool:
    action = str(action or "").strip().lower()

    if action in {"buy_ready", "watch", "hold_candidate", "hold_position"}:
        return True

    if action in {"review_needed", "exit_watch"} and held_pos <= 0:
        return True

    if action == "sell_candidate" and held_pos <= 0:
        return True

    if action == "exit_ready":
        return held_pos > 0

    if action == "avoid":
        return False

    return False


def should_rotate_candidate(action: str, retention_score: int, quality: str, held_pos: float) -> tuple[bool, str | None]:
    action = str(action or "").strip().lower()
    quality_value = quality_rank(quality)

    if action == "avoid":
        return True, "replace due to action=avoid"

    if action in {"exit_ready", "sell_candidate"} and held_pos <= 0:
        return True, f"replace due to {action} without position"

    if retention_score <= 4 and quality_value <= 2 and held_pos <= 0:
        return True, f"replace due to retention_score={retention_score} quality={quality}"

    if retention_score <= 2 and held_pos <= 0:
        return True, f"replace due to low retention_score={retention_score}"

    return False, None


def required_replacement_delta(
    watch_streak: int,
    current_action: str,
    current_quality: str,
    current_retention: int,
) -> int:
    current_action = str(current_action or "").strip().lower()
    current_quality_rank = quality_rank(current_quality)

    if watch_streak >= 30 and current_action in {"watch", "hold_candidate"}:
        return 0
    if watch_streak >= 20 and current_action in {"watch", "hold_candidate"}:
        return 1
    if watch_streak >= 10 and current_action in {"watch", "hold_candidate"}:
        return 2

    if current_retention <= 3 or current_quality_rank <= 2:
        return 1

    return 3


def replacement_is_meaningfully_better(
    current_analysis: dict,
    replacement_analysis: dict,
    watch_streak: int = 0,
) -> bool:
    current_retention = int(current_analysis.get("retention_score", current_analysis.get("total_score", 0)) or 0)
    current_quality = current_analysis.get("candidate_quality")
    current_action = str(current_analysis.get("action") or "").strip().lower()

    repl_replacement_score = int(
        replacement_analysis.get("replacement_score", replacement_analysis.get("total_score", 0)) or 0
    )
    repl_quality = replacement_analysis.get("candidate_quality")
    repl_action = str(replacement_analysis.get("action") or "").strip().lower()

    if repl_action == "buy_ready" and current_action not in {"buy_ready", "hold_position"}:
        return True

    required_delta = required_replacement_delta(
        watch_streak=watch_streak,
        current_action=current_action,
        current_quality=current_quality,
        current_retention=current_retention,
    )

    if repl_replacement_score >= current_retention + required_delta:
        return True

    if quality_rank(repl_quality) > quality_rank(current_quality):
        if repl_replacement_score >= current_retention + max(0, required_delta - 1):
            return True

    return False


def candidate_sort_key_factory(by_sym: dict):
    def _candidate_sort_key(sym: str):
        stock = by_sym.get(sym) or {}
        analysis = build_pipeline_analysis(stock)

        action = str(analysis.get("action") or "").strip().lower()
        replacement_score = int(analysis.get("replacement_score", analysis.get("total_score", 0)) or 0)
        retention_score = int(analysis.get("retention_score", analysis.get("total_score", 0)) or 0)
        entry_score = int(analysis.get("entry_score", 0) or 0)
        total_score = int(analysis.get("total_score", 0) or 0)
        q_rank = quality_rank(analysis.get("candidate_quality"))
        price = to_float(stock.get("latestClose"), 999999)

        return (
            1 if action == "buy_ready" else 0,
            replacement_score,
            retention_score,
            entry_score,
            total_score,
            q_rank,
            -price,
        )

    return _candidate_sort_key


def build_analysis_cache(by_sym: dict) -> dict:
    return {sym: build_pipeline_analysis(stock) for sym, stock in (by_sym or {}).items()}


def _replacement_profile(analysis: dict) -> str:
    action = str(analysis.get("action") or "").strip().lower()
    quality = str(analysis.get("candidate_quality") or "").upper()
    q_rank = quality_rank(quality)

    total_score = int(analysis.get("total_score", 0) or 0)
    retention = int(analysis.get("retention_score", total_score) or 0)
    replacement_score = int(analysis.get("replacement_score", total_score) or 0)
    entry_score = int(analysis.get("entry_score", 0) or 0)

    if (
        action == "buy_ready"
        and q_rank >= 3
        and replacement_score >= 6
        and entry_score >= 3
        and total_score >= 3
    ):
        return "upgrade"

    if (
        action in {"watch", "hold_candidate", "buy_ready"}
        and q_rank >= 2
        and retention >= 4
        and replacement_score >= 4
        and total_score >= 1
    ):
        return "stable"

    if (
        action in {"watch", "hold_candidate", "buy_ready", "sell_candidate", "review_needed", "exit_watch"}
        and q_rank >= 2
        and retention >= 3
        and replacement_score >= 2
    ):
        return "fallback"

    if (
        action == "sell_candidate"
        and q_rank >= 3
        and retention >= 5
        and replacement_score >= 3
    ):
        return "fallback"

    return "reject"


def _replacement_rank_tuple(analysis: dict) -> tuple:
    action = str(analysis.get("action") or "").strip().lower()
    return (
        1 if action == "buy_ready" else 0,
        1 if action in {"watch", "hold_candidate"} else 0,
        int(analysis.get("replacement_score", analysis.get("total_score", 0)) or 0),
        int(analysis.get("retention_score", analysis.get("total_score", 0)) or 0),
        int(analysis.get("entry_score", 0) or 0),
        quality_rank(analysis.get("candidate_quality")),
        int(analysis.get("total_score", 0) or 0),
    )


def available_replacements(
    *,
    current_scan: list[str],
    replacement_source: list[str],
    by_sym: dict,
    analysis_cache: dict,
    held: dict,
    open_buy_syms: set[str],
    is_excluded_fn,
    banned: set[str] | None = None,
) -> tuple[list[str], dict]:
    banned = banned or set()
    current_set = set(current_scan)

    reason_counts = {
        "in_current_scan": 0,
        "banned": 0,
        "held": 0,
        "open_buy": 0,
        "excluded": 0,
        "bad_action": 0,
        "weak_profile": 0,
        "accepted": 0,
    }

    upgrade_pool: list[str] = []
    stable_pool: list[str] = []
    fallback_pool: list[str] = []

    for s in replacement_source:
        if s in current_set:
            reason_counts["in_current_scan"] += 1
            continue
        if s in banned:
            reason_counts["banned"] += 1
            continue
        if s in held:
            reason_counts["held"] += 1
            continue
        if s in open_buy_syms:
            reason_counts["open_buy"] += 1
            continue
        if is_excluded_fn(s):
            reason_counts["excluded"] += 1
            continue

        analysis = analysis_cache.get(s) or build_pipeline_analysis(by_sym.get(s) or {})
        action = str(analysis.get("action") or "").strip().lower()
        held_pos = float(held.get(s, 0.0))

        if not is_allowed_replacement_action(action, held_pos):
            reason_counts["bad_action"] += 1
            continue

        profile = _replacement_profile(analysis)

        if profile == "upgrade":
            upgrade_pool.append(s)
            reason_counts["accepted"] += 1
        elif profile == "stable":
            stable_pool.append(s)
            reason_counts["accepted"] += 1
        elif profile == "fallback":
            fallback_pool.append(s)
            reason_counts["accepted"] += 1
        else:
            reason_counts["weak_profile"] += 1

    upgrade_pool.sort(key=lambda s: _replacement_rank_tuple(analysis_cache.get(s) or {}), reverse=True)
    stable_pool.sort(key=lambda s: _replacement_rank_tuple(analysis_cache.get(s) or {}), reverse=True)
    fallback_pool.sort(key=lambda s: _replacement_rank_tuple(analysis_cache.get(s) or {}), reverse=True)

    pool = upgrade_pool + stable_pool + fallback_pool
    return pool, reason_counts