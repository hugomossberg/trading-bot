import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.config import (
    FINAL_CANDIDATES_PATH,
    PIPELINE_SNAPSHOT_PATH,
    UNIVERSE_ROWS,
    CANDIDATE_MULTIPLIER,
)
from app.core.scanner import ensure_stock_info
from app.core.technicals import build_technical_snapshot
from app.core.filters import precheck_stock
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


def _to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default

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


def _run_stage1(universe: list[dict]) -> list[dict]:
    results = []

    for raw in universe:
        stock = _normalize_stock(raw)
        symbol = (stock.get("symbol") or "").upper().strip()
        if not symbol:
            continue

        technicals = build_technical_snapshot(symbol) or {}
        filters = precheck_stock(stock, technicals)

        score, score_details = _stage1_score(stock, technicals)

        passed = filters.get("allowed", False) and score >= 0

        results.append({
            "symbol": symbol,
            "name": stock.get("name") or symbol,
            "stage": 1,
            "passed": passed,
            "score": score,
            "reason": "passed" if passed else "filtered_or_low_score",
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
        reverse=True
    )
    return results

def _run_stage3(stage2_passed: list[dict]) -> list[dict]:
    results = []

    for item in stage2_passed:
        stock = dict(item.get("stock") or {})
        symbol = item.get("symbol")

        news_score, raw_sentiment = score_news(stock)
        score_details = {
            "news_sentiment_score": news_score,
            "raw_sentiment": raw_sentiment,
        }

        passed = True  # i början låter vi news justera, inte blockera hårt

        results.append({
            "symbol": symbol,
            "name": item.get("name") or symbol,
            "stage": 3,
            "passed": passed,
            "score": news_score,
            "reason": "passed",
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
            x.get("stage1_score", 0)
            + x.get("stage2_score", 0)
            + x.get("score", 0)
        ),
        reverse=True
    )
    return results

def _build_final_candidates(stage3_passed: list[dict], limit: int = 10) -> list[dict]:
    final_candidates = []

    for item in stage3_passed[:limit]:
        final_score = (
            item.get("stage1_score", 0)
            + item.get("stage2_score", 0)
            + item.get("score", 0)
        )

        signal = "Håll"
        if final_score >= 4:
            signal = "Köp"
        elif final_score <= -3:
            signal = "Sälj"

        final_candidates.append({
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "final_score": final_score,
            "signal": signal,
            "stock": item.get("stock") or {},
            "technicals": item.get("technicals") or {},
            "scores": {
                "stage1": item.get("stage1_score", 0),
                "stage2": item.get("stage2_score", 0),
                "stage3": item.get("score", 0),
            },
            "score_details": {
                "stage1": item.get("stage1_details", {}),
                "stage2": item.get("stage2_details", {}),
                "stage3": item.get("score_details", {}),
            },
        })

    return final_candidates


async def run_pipeline(ib_client) -> dict:
    rows_target = max(UNIVERSE_ROWS * CANDIDATE_MULTIPLIER, UNIVERSE_ROWS)

    universe = await ensure_stock_info(ib_client, min_count=rows_target)
    universe = universe or []

    stage1 = _run_stage1(universe)
    stage1_passed = [x for x in stage1 if x.get("passed")]

    stage2 = _run_stage2(stage1_passed[:40])
    stage2_passed = [x for x in stage2 if x.get("passed")]

    stage3 = _run_stage3(stage2_passed[:30])
    stage3_passed = [x for x in stage3 if x.get("passed")]

    final_candidates = _build_final_candidates(
        stage3_passed,
        limit=max(UNIVERSE_ROWS * 3, 25)
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
        "[pipeline] done | universe=%d | s1=%d | s2=%d | s3=%d | final=%d",
        len(universe),
        len(stage1_passed),
        len(stage2_passed),
        len(stage3_passed),
        len(final_candidates),
    )

    return snapshot