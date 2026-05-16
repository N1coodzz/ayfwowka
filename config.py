from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import csv
from datetime import datetime

from models import MatchState, Decision


FIELDNAMES = [
    "date",
    "match",
    "map",
    "time",
    "score",
    "total_kills_now",
    "networth_diff",
    "line",
    "side",
    "odds",
    "probability",
    "fair_odds",
    "edge",
    "expected_total_range",
    "decision",
    "reason",
    "result_total_kills",
    "win_loss",
    "profit_loss",
]


def append_signal(csv_path: str | Path, state: MatchState, decision: Decision) -> None:
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    best = decision.best

    row = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "match": state.match,
        "map": state.map_no or "",
        "time": state.time_str,
        "score": state.score_text,
        "total_kills_now": state.total_kills_now,
        "networth_diff": state.networth_diff_k,
        "line": f"{best.line:.1f}" if best else "",
        "side": best.side_ru if best else "",
        "odds": f"{best.odds:.2f}" if best else "",
        "probability": f"{best.probability:.4f}" if best else "",
        "fair_odds": f"{best.fair_odds:.2f}" if best else "",
        "edge": f"{best.edge:.4f}" if best else "",
        "expected_total_range": f"{decision.expected_low}-{decision.expected_high}",
        "decision": decision.decision,
        "reason": decision.reason,
        "result_total_kills": "",
        "win_loss": "",
        "profit_loss": "",
    }

    file_exists = csv_path.exists()

    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def read_history(csv_path: str | Path, limit: int = 10) -> list[dict]:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return []

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    return rows[-limit:][::-1]


def format_history(csv_path: str | Path, limit: int = 10) -> str:
    rows = read_history(csv_path, limit=limit)
    if not rows:
        return "История пока пустая."

    out = []
    for idx, row in enumerate(rows, start=1):
        if row.get("decision") == "брать":
            pick = f"{row.get('side')} {row.get('line')} @ {row.get('odds')}"
            edge = row.get("edge", "")
            try:
                edge_txt = f" | Edge {float(edge) * 100:+.1f}%"
            except Exception:
                edge_txt = ""
        else:
            pick = "нет"
            edge_txt = ""

        out.append(
            f"{idx}) {row.get('match')} | {row.get('time')} | {pick}{edge_txt} | {row.get('decision')}"
        )

    return "\n".join(out)


def format_stats(csv_path: str | Path) -> str:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return "Статистика пока пустая."

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return "Статистика пока пустая."

    total = len(rows)
    takes = [r for r in rows if r.get("decision") == "брать"]
    passes = [r for r in rows if r.get("decision") == "пас"]

    settled = [r for r in rows if r.get("win_loss")]
    wins = [r for r in settled if r.get("win_loss", "").lower() in {"win", "w", "1", "+", "плюс"}]

    profit_values = []
    for r in rows:
        raw = r.get("profit_loss", "").replace(",", ".").strip()
        if not raw:
            continue
        try:
            profit_values.append(float(raw))
        except ValueError:
            pass

    avg_edge_values = []
    for r in takes:
        raw = r.get("edge", "").replace(",", ".").strip()
        if not raw:
            continue
        try:
            avg_edge_values.append(float(raw))
        except ValueError:
            pass

    winrate = (len(wins) / len(settled) * 100) if settled else 0
    roi_text = "-"
    if profit_values:
        # ROI без размера ставок условный: сумма P/L по заполненным строкам.
        roi_text = f"{sum(profit_values):+.2f} ед."

    avg_edge = (sum(avg_edge_values) / len(avg_edge_values) * 100) if avg_edge_values else 0

    return (
        f"Всего анализов: {total}\n"
        f"Брать: {len(takes)}\n"
        f"Пас: {len(passes)}\n"
        f"Закрытых ставок: {len(settled)}\n"
        f"Winrate: {winrate:.1f}%\n"
        f"P/L: {roi_text}\n"
        f"Средний edge: {avg_edge:+.1f}%"
    )
