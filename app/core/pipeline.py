import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from app.core.logview import debug_log
from app.config import (
    FINAL_CANDIDATES_PATH,
    PIPELINE_SNAPSHOT_PATH,
    UNIVERSE_ROWS,
    CANDIDATE_MULTIPLIER,
)
from app.core.scanner import ensure_stock_info
from app.core.technicals import build_technical_snapshot
from app.core.filters import precheck_stock
from app.data.market_data_shared import md
from app.core.candidate_profile import build_candidate_profile
from app.core.entry_engine import evaluate_entry
from app.core.scoring import (
    score_price_trend,
    score_rsi,
    score_volume_spike,
    score_volatility,
    score_momentum,
    score_liquidity,
    score_revenue_growth,
    score_profit_margin,
    score_debt_to_equity,
    score_news,
)

log = logging.getLogger("pipeline")



def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on", "y"}


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default


def _technicals_ready(technicals: dict) -> bool:
    if not isinstance(technicals, dict):
        return False

    required = (
        "price",
        "sma20",
        "sma50",
        "rsi14",
        "atr14",
        "momentum_20",
    )

    for key in required:
        if _to_float(technicals.get(key), None) is None:
            return False

    return True


def _write_json(path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _normalize_stock(stock: dict) -> dict:
    stock = dict(stock or {})
    for key in ("latestClose", "PE", "marketCap", "beta", "trailingEps", "dividendYield"):
        stock[key] = _to_float(stock.get(key), 0.0)
    return stock


def _stage1_score(stock: dict, technicals: dict) -> tuple[int, dict]:
    details = {
        "price_trend": score_price_trend(technicals),
        "rsi": score_rsi(technicals),
        "volume_spike": score_volume_spike(technicals),
        "volatility": score_volatility(technicals),
        "momentum": score_momentum(technicals),
        "liquidity": score_liquidity(technicals),
    }
    total = sum(details.values())
    return total, details


def _run_stage1(universe: list[dict], use_ib: bool = False) -> list[dict]:
    results = []

    for raw in universe:
        stock = _normalize_stock(raw)
        symbol = (stock.get("symbol") or "").upper().strip()
        if not symbol:
            continue

        try:
            technicals = build_technical_snapshot(symbol, use_ib=use_ib) or {}
        except Exception as e:
            log.warning("[pipeline] TECH %-6s | snapshot fail: %s", symbol, e)
            technicals = {}

        technicals_ok = _technicals_ready(technicals)

        if technicals_ok:
            filters = precheck_stock(stock, technicals)
            score, score_details = _stage1_score(stock, technicals)
        else:
            filters = {
                "allowed": False,
                "reason": "missing_technicals",
            }
            score = -1
            score_details = {
                "price_trend": 0,
                "rsi": 0,
                "volume_spike": 0,
                "volatility": 0,
                "momentum": 0,
                "liquidity": 0,
            }

        passed = technicals_ok and filters.get("allowed", False) and score >= 0

        if passed:
            reason = "passed"
        elif not technicals_ok:
            reason = "missing_technicals"
        else:
            reason = "filtered_or_low_score"

        results.append({
            "symbol": symbol,
            "name": stock.get("name") or symbol,
            "stage": 1,
            "passed": passed,
            "score": score,
            "reason": reason,
            "filters": filters,
            "score_details": score_details,
            "stock": stock,
            "technicals": technicals,
        })

    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results
        


def _stage2_score(stock: dict) -> tuple[int, dict]:
    details = {
        "revenue_growth": score_revenue_growth(stock),
        "profit_margin": score_profit_margin(stock),
        "debt_to_equity": score_debt_to_equity(stock),
    }
    total = sum(details.values())
    return total, details


def _run_stage2(stage1_passed: list[dict]) -> list[dict]:
    results = []

    for item in stage1_passed:
        stock = dict(item.get("stock") or {})
        symbol = item.get("symbol")

        score, score_details = _stage2_score(stock)
        passed = score >= 0

        results.append({
            "symbol": symbol,
            "name": item.get("name") or symbol,
            "stage": 2,
            "passed": passed,
            "score": score,
            "reason": "passed" if passed else "weak_financials",
            "score_details": score_details,
            "stock": stock,
            "technicals": item.get("technicals") or {},
            "stage1_score": item.get("score", 0),
            "stage1_details": item.get("score_details", {}),
            "stage2_details": score_details,
        })

    results.sort(
        key=lambda x: (x.get("stage1_score", 0) + x.get("score", 0)),
        reverse=True,
    )
    return results


def _run_stage3(stage2_passed: list[dict]) -> list[dict]:
    results = []

    news_fetch_limit = _env_int("PIPELINE_NEWS_FETCH_LIMIT", 20)
    news_items_limit = _env_int("PIPELINE_NEWS_ITEMS", 5)
    min_news_score = _env_int("PIPELINE_STAGE3_MIN_NEWS_SCORE", 0)
    require_news = _env_bool("PIPELINE_STAGE3_REQUIRE_NEWS", False)

    for idx, item in enumerate(stage2_passed, start=1):
        stock = dict(item.get("stock") or {})
        symbol = item.get("symbol")

        news_items = []

        if idx <= news_fetch_limit and news_fetch_limit > 0 and news_items_limit > 0:
            try:
                news_items = md.get_stock_news(symbol, limit=news_items_limit) or []
            except Exception as e:
                log.warning("[pipeline] NEWS %-6s | fetch fail: %s", symbol, e)
                news_items = []

        stock["News"] = [
            {
                "content": {
                    "title": n.get("title", ""),
                    "summary": n.get("text", "") or "",
                    "publisher": n.get("publisher") or n.get("site", ""),
                    "link": n.get("url", ""),
                }
            }
            for n in news_items
        ]

        news_score, raw_sentiment = score_news(stock)
        news_count = len(stock.get("News", []))

        if require_news and news_count == 0:
            passed = False
            reason = "no_news"
        elif news_score < min_news_score:
            passed = False
            reason = f"news_score_below_min:{news_score}"
        else:
            passed = True
            reason = "passed"

        score_details = {
            "news_sentiment_score": news_score,
            "raw_sentiment": raw_sentiment,
            "news_count": news_count,
            "min_news_score": min_news_score,
            "require_news": require_news,
        }

        if stock.get("News"):
            top_titles = [
                ((n.get("content") or {}).get("title") or "-")
                for n in stock["News"][:2]
            ]
            debug_log(
                log,
                "[pipeline][NEWS] %-6s | items=%d | score=%+d | passed=%s | %s",
                symbol,
                news_count,
                news_score,
                passed,
                " || ".join(top_titles),
            )
        else:
            debug_log(
                log,
                "[pipeline][NEWS] %-6s | items=0 | score=%+d | passed=%s",
                symbol,
                news_score,
                passed,
            )

        results.append({
            "symbol": symbol,
            "name": item.get("name") or symbol,
            "stage": 3,
            "passed": passed,
            "score": news_score,
            "reason": reason,
            "score_details": score_details,
            "stock": stock,
            "technicals": item.get("technicals") or {},
            "stage1_score": item.get("stage1_score", 0),
            "stage2_score": item.get("score", 0),
            "stage1_details": item.get("stage1_details", {}),
            "stage2_details": item.get("stage2_details", {}),
        })

    results.sort(
        key=lambda x: (
            int(x.get("passed", False)),
            x.get("stage1_score", 0) + x.get("stage2_score", 0) + x.get("score", 0),
        ),
        reverse=True,
    )
    return results


def _action_priority(action: str) -> int:
    order = {
        "buy_ready": 5,
        "watch": 4,
        "hold_candidate": 3,
        "avoid": 2,
        "sell_candidate": 1,
    }
    return order.get(action, 0)


def _build_final_candidates(stage3_passed: list[dict], limit: int = 10) -> list[dict]:
    final_candidates = []

    for item in stage3_passed[:limit]:
        symbol = item.get("symbol")
        name = item.get("name") or symbol
        stock = item.get("stock") or {}
        technicals = item.get("technicals") or {}

        stage1_score = int(item.get("stage1_score", 0) or 0)
        stage2_score = int(item.get("stage2_score", 0) or 0)
        stage3_score = int(item.get("score", 0) or 0)

        stage1_details = item.get("stage1_details", {}) or {}
        stage2_details = item.get("stage2_details", {}) or {}
        stage3_details = item.get("score_details", {}) or {}

        candidate_score = stage1_score + stage2_score + stage3_score

        profile = build_candidate_profile(
            stock=stock,
            technicals=technicals,
            candidate_score=candidate_score,
            stage1_details=stage1_details,
            stage2_details=stage2_details,
            stage3_details=stage3_details,
        )

        entry = evaluate_entry(
            stock=stock,
            technicals=technicals,
            candidate_score=candidate_score,
            profile=profile,
        )

        final_candidates.append({
            "symbol": symbol,
            "name": name,
            "candidate_score": candidate_score,
            "entry_score": entry.get("entry_score", 0),
            "final_score": candidate_score + int(entry.get("entry_score", 0) * 0.5),
            "signal": (
                "Köp"
                if entry.get("action") == "buy_ready"
                else "Sälj"
                if entry.get("action") == "sell_candidate"
                else "Håll"
            ),
            "candidate_quality": profile.get("candidate_quality", "D"),
            "setup_type": profile.get("setup_type", "low_quality_noise"),
            "timing_state": entry.get("timing_state", "watch_only"),
            "action": entry.get("action", "watch"),
            "positive_flags": profile.get("positive_flags", []),
            "risk_flags": profile.get("risk_flags", []),
            "retention_score": profile.get("retention_score", 0),
            "replacement_score": profile.get("replacement_score", 0),
            "entry_reasons": entry.get("entry_reasons", []),
            "stock": stock,
            "technicals": technicals,
            "scores": {
                "stage1": stage1_score,
                "stage2": stage2_score,
                "stage3": stage3_score,
            },
            "score_details": {
                "stage1": stage1_details,
                "stage2": stage2_details,
                "stage3": stage3_details,
            },
        })

    final_candidates.sort(
        key=lambda x: (
            _action_priority(x.get("action")),
            int(x.get("entry_score", 0) or 0),
            int(x.get("candidate_score", 0) or 0),
            int(x.get("retention_score", 0) or 0),
        ),
        reverse=True,
    )

    for idx, item in enumerate(final_candidates, start=1):
        item["rank"] = idx

    return final_candidates


async def run_pipeline(ib_client) -> dict:
    rows_target = _env_int("SCANNER_MIN_USABLE_ROWS", max(UNIVERSE_ROWS * CANDIDATE_MULTIPLIER, UNIVERSE_ROWS))
    use_ib_technicals = _env_bool("PIPELINE_USE_IB_TECHNICALS", False)

    stage1_limit = _env_int("PIPELINE_STAGE1_LIMIT", 1200)
    stage2_limit = _env_int("PIPELINE_STAGE2_LIMIT", 500)
    stage3_limit = _env_int("PIPELINE_STAGE3_LIMIT", 250)
    final_limit = _env_int("PIPELINE_FINAL_LIMIT", 175)

    universe = await ensure_stock_info(ib_client, min_count=rows_target)
    universe = universe or []

    stage1_input = universe[:stage1_limit]
    stage1 = _run_stage1(stage1_input, use_ib=use_ib_technicals)
    stage1_passed = [x for x in stage1 if x.get("passed")]

    stage2 = _run_stage2(stage1_passed[:stage2_limit])
    stage2_passed = [x for x in stage2 if x.get("passed")]

    stage3_limit = _env_int("PIPELINE_STAGE3_LIMIT", max(UNIVERSE_ROWS * 4, 120))
    stage3_input = stage2_passed[:stage3_limit]
    stage3 = _run_stage3(stage3_input)
    stage3_passed = [x for x in stage3 if x.get("passed")]

    final_candidates = _build_final_candidates(
        stage3_passed,
        limit=final_limit,
    )
    snapshot = {
        "generated_at": _now_iso(),
        "universe_size": len(universe),
        "stage1_total": len(stage1),
        "stage1_passed": len(stage1_passed),
        "stage2_total": len(stage2),
        "stage2_passed": len(stage2_passed),
        "stage3_total": len(stage3),
        "stage3_passed": len(stage3_passed),
        "stage1_candidates": stage1,
        "stage2_candidates": stage2,
        "stage3_candidates": stage3,
        "final_candidates": final_candidates,
    }

    _write_json(PIPELINE_SNAPSHOT_PATH, snapshot)
    _write_json(FINAL_CANDIDATES_PATH, final_candidates)

    log.info(
        "[pipeline] done | universe=%d | s1=%d | s2=%d | s3=%d | final=%d | ib_technicals=%s",
        len(universe),
        len(stage1_passed),
        len(stage2_passed),
        len(stage3_passed),
        len(final_candidates),
        use_ib_technicals,
    )

    return snapshot