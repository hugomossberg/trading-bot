from ib_insync import IB


def extract_positions(positions) -> list[dict]:
    nonzero = []
    seen = set()

    for p in positions:
        qty = float(p.position or 0.0)
        if abs(qty) < 1e-6:
            continue

        key = p.contract.conId or (p.contract.symbol, p.contract.exchange)
        if key in seen:
            continue
        seen.add(key)

        qty_str = str(int(qty)) if float(qty).is_integer() else f"{qty:.2f}"
        avg = float(p.avgCost or 0.0)

        nonzero.append(
            {
                "symbol": p.contract.symbol or "?",
                "qty": qty_str,
                "avg": f"{avg:.2f}",
                "qty_raw": qty,
                "avg_raw": avg,
            }
        )

    nonzero.sort(key=lambda x: abs(float(x["qty_raw"])), reverse=True)
    return nonzero


def extract_open_orders(ib: IB) -> list[dict]:
    rows = []
    for t in ib.openTrades():
        rows.append(
            {
                "symbol": t.contract.symbol or "?",
                "side": t.order.action,
                "qty": int(t.order.totalQuantity),
                "filled": int(t.orderStatus.filled or 0),
                "status": (t.orderStatus.status or "?").lower(),
                "session": "AH" if getattr(t.order, "outsideRth", False) else "RTH",
            }
        )
    return rows