import asyncio
import html
import json
import os
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


# ============================================================
# TEST CONFIG — already filled for Replit test
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8586607615:AAFknNl5-ihG0FpykOpNHKbUqAOWHAaxklI").strip()
LOGIN_PASSWORD = os.getenv("LOGIN_PASSWORD", "OOORASSVET").strip()

POLYMARKET_WALLET = os.getenv(
    "POLYMARKET_WALLET",
    "0x88d1e3eb1b1b71d498db3b70a9e03f4b8238c1c3",
).strip().lower()

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.0"))
LIMIT = int(os.getenv("LIMIT", "20"))
TAKER_ONLY = os.getenv("TAKER_ONLY", "false").lower() == "true"
MIN_PLAYER_USDC = float(os.getenv("MIN_PLAYER_USDC", "0"))

# false = at start saves old trades and alerts only future trades.
# use button "Последние 10 сделок" to force-check real latest trades immediately.
FIRST_RUN_SEND_HISTORY = os.getenv("FIRST_RUN_SEND_HISTORY", "false").lower() == "true"

PORT = int(os.getenv("PORT", "8080"))
DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "bets.db"

POLYMARKET_TRADES_URL = "https://data-api.polymarket.com/trades"
POLYMARKET_POSITIONS_URL = "https://data-api.polymarket.com/positions"

STATUSES = {"OPEN", "WIN", "LOSE", "VOID"}


# ============================================================
# FSM
# ============================================================

class RepeatFlow(StatesGroup):
    waiting_odds = State()
    waiting_stake = State()


# ============================================================
# BASIC HELPERS
# ============================================================

def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def h(x: Any) -> str:
    return html.escape(str(x if x is not None else ""))


def as_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(str(x).replace(",", ".").replace("$", "").replace("%", "").strip())
    except Exception:
        return default


def parse_float(text: Any) -> Optional[float]:
    try:
        return float(str(text).replace(",", ".").strip())
    except Exception:
        return None


def money(x: Any) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return str(x)


def num(x: Any, n: int = 2) -> str:
    try:
        return f"{float(x):,.{n}f}"
    except Exception:
        return str(x)


def price_decimal(x: Any) -> str:
    try:
        return f"{float(x):.3f}"
    except Exception:
        return str(x)


def price_cents(x: Any) -> str:
    try:
        return f"{float(x) * 100:.1f}¢"
    except Exception:
        return str(x)


def short_tx(tx: Any) -> str:
    tx = str(tx or "")
    if len(tx) > 18:
        return tx[:10] + "..." + tx[-6:]
    return tx or "—"


def check_config() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty")
    if not LOGIN_PASSWORD:
        raise RuntimeError("LOGIN_PASSWORD is empty")
    if not POLYMARKET_WALLET.startswith("0x"):
        raise RuntimeError("POLYMARKET_WALLET must be 0x wallet")


# ============================================================
# DATABASE
# ============================================================

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    with closing(db()) as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            authed_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS seen_trades (
            trade_uid TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_uid TEXT UNIQUE,
            created_at TEXT NOT NULL,
            side TEXT,
            outcome TEXT,
            price REAL,
            size REAL,
            usdc_size REAL,
            market_title TEXT,
            market_url TEXT,
            tx_hash TEXT,
            condition_id TEXT,
            asset TEXT,
            position_shares REAL,
            position_avg_price REAL,
            position_current_price REAL,
            position_value REAL,
            position_pnl REAL,
            position_raw_json TEXT,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            alert_id INTEGER,
            created_at TEXT NOT NULL,
            market_title TEXT,
            market_url TEXT,
            side TEXT,
            outcome TEXT,
            player_price REAL,
            player_usdc_size REAL,
            player_shares REAL,
            position_shares REAL,
            position_avg_price REAL,
            position_current_price REAL,
            position_value REAL,
            position_pnl REAL,
            my_odds REAL,
            my_stake REAL,
            status TEXT DEFAULT 'OPEN',
            profit REAL DEFAULT 0,
            tx_hash TEXT
        );
        """)
        con.commit()


def is_authed_user(user_id: int) -> bool:
    with closing(db()) as con:
        row = con.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row is not None


def save_user(user_id: int, username: str, first_name: str) -> None:
    with closing(db()) as con:
        con.execute("""
        INSERT OR REPLACE INTO users(user_id, username, first_name, authed_at)
        VALUES(?,?,?,?)
        """, (user_id, username or "", first_name or "", now_iso()))
        con.commit()


def authed_users() -> List[int]:
    with closing(db()) as con:
        rows = con.execute("SELECT user_id FROM users ORDER BY authed_at ASC").fetchall()
    return [int(r["user_id"]) for r in rows]


def require_auth_message(message: Message) -> bool:
    return message.from_user is not None and is_authed_user(message.from_user.id)


def require_auth_cb(cb: CallbackQuery) -> bool:
    return cb.from_user is not None and is_authed_user(cb.from_user.id)


def seen_count() -> int:
    with closing(db()) as con:
        row = con.execute("SELECT COUNT(*) c FROM seen_trades").fetchone()
        return int(row["c"])


def is_seen(uid: str) -> bool:
    with closing(db()) as con:
        return con.execute("SELECT 1 FROM seen_trades WHERE trade_uid=?", (uid,)).fetchone() is not None


def mark_seen(uid: str) -> None:
    with closing(db()) as con:
        con.execute(
            "INSERT OR IGNORE INTO seen_trades(trade_uid, created_at) VALUES(?,?)",
            (uid, now_iso()),
        )
        con.commit()


def get_alert(alert_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as con:
        return con.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()


def get_bet(user_id: int, bet_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as con:
        return con.execute(
            "SELECT * FROM bets WHERE id=? AND user_id=?",
            (bet_id, user_id),
        ).fetchone()


# ============================================================
# POLYMARKET PARSING
# ============================================================

def trade_uid(t: Dict[str, Any]) -> str:
    return "|".join([
        str(t.get("transactionHash", "")),
        str(t.get("conditionId", "")),
        str(t.get("asset", "")),
        str(t.get("side", "")),
        str(t.get("outcomeIndex", "")),
        str(t.get("size", "")),
        str(t.get("price", "")),
        str(t.get("timestamp", "")),
    ])


def market_url(t: Dict[str, Any]) -> str:
    slug = t.get("eventSlug") or t.get("slug")
    return f"https://polymarket.com/event/{slug}" if slug else "https://polymarket.com"


def calc_usdc(t: Dict[str, Any]) -> float:
    if t.get("usdcSize") is not None:
        return as_float(t.get("usdcSize"))
    return as_float(t.get("size")) * as_float(t.get("price"))


def get_position_field(p: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in p and p[k] is not None:
            return p[k]
    return default


def normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def match_position_for_trade(trade: Dict[str, Any], positions: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    trade_title = normalize_text(trade.get("title"))
    trade_outcome = normalize_text(trade.get("outcome"))
    trade_asset = normalize_text(trade.get("asset"))
    trade_condition = normalize_text(trade.get("conditionId"))

    best = None
    best_score = 0

    for p in positions:
        score = 0

        p_title = normalize_text(
            get_position_field(p, ["title", "marketTitle", "question", "eventTitle", "market"])
        )
        p_outcome = normalize_text(
            get_position_field(p, ["outcome", "outcomeName", "answer", "assetName"])
        )
        p_asset = normalize_text(get_position_field(p, ["asset", "assetId", "tokenId"]))
        p_condition = normalize_text(get_position_field(p, ["conditionId", "condition_id"]))

        if trade_asset and p_asset and trade_asset == p_asset:
            score += 5
        if trade_condition and p_condition and trade_condition == p_condition:
            score += 4
        if trade_title and p_title and trade_title == p_title:
            score += 3
        elif trade_title and p_title and (trade_title in p_title or p_title in trade_title):
            score += 2
        if trade_outcome and p_outcome and trade_outcome == p_outcome:
            score += 2

        if score > best_score:
            best_score = score
            best = p

    return best if best_score >= 3 else None


def parse_position_snapshot(position: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not position:
        return {
            "position_shares": None,
            "position_avg_price": None,
            "position_current_price": None,
            "position_value": None,
            "position_pnl": None,
            "position_raw_json": None,
        }

    shares = get_position_field(position, [
        "size", "shares", "position", "positionSize", "balance", "quantity"
    ])

    avg_price = get_position_field(position, [
        "avgPrice", "averagePrice", "avg_price", "avg", "average"
    ])

    current_price = get_position_field(position, [
        "curPrice", "currentPrice", "current", "price", "markPrice"
    ])

    value = get_position_field(position, [
        "value", "currentValue", "marketValue"
    ])

    pnl = get_position_field(position, [
        "cashPnl", "pnl", "profit", "profitLoss", "totalPnl"
    ])

    return {
        "position_shares": as_float(shares) if shares is not None else None,
        "position_avg_price": as_float(avg_price) if avg_price is not None else None,
        "position_current_price": as_float(current_price) if current_price is not None else None,
        "position_value": as_float(value) if value is not None else None,
        "position_pnl": as_float(pnl) if pnl is not None else None,
        "position_raw_json": json.dumps(position, ensure_ascii=False),
    }


def save_alert_from_trade(t: Dict[str, Any], position: Optional[Dict[str, Any]] = None) -> int:
    uid = trade_uid(t)
    ps = parse_position_snapshot(position)

    with closing(db()) as con:
        con.execute("""
        INSERT OR IGNORE INTO alerts(
            trade_uid, created_at, side, outcome, price, size, usdc_size,
            market_title, market_url, tx_hash, condition_id, asset,
            position_shares, position_avg_price, position_current_price,
            position_value, position_pnl, position_raw_json, raw_json
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            now_iso(),
            str(t.get("side", "")).upper(),
            str(t.get("outcome", "")),
            as_float(t.get("price")),
            as_float(t.get("size")),
            calc_usdc(t),
            str(t.get("title", "")),
            market_url(t),
            str(t.get("transactionHash", "")),
            str(t.get("conditionId", "")),
            str(t.get("asset", "")),
            ps["position_shares"],
            ps["position_avg_price"],
            ps["position_current_price"],
            ps["position_value"],
            ps["position_pnl"],
            ps["position_raw_json"],
            json.dumps(t, ensure_ascii=False),
        ))

        row = con.execute("SELECT id FROM alerts WHERE trade_uid=?", (uid,)).fetchone()
        con.commit()
        return int(row["id"])


def create_test_alert() -> int:
    with closing(db()) as con:
        con.execute("""
        INSERT INTO alerts(
            trade_uid, created_at, side, outcome, price, size, usdc_size,
            market_title, market_url, tx_hash, condition_id, asset,
            position_shares, position_avg_price, position_current_price,
            position_value, position_pnl, position_raw_json, raw_json
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            f"test-{time.time()}",
            now_iso(),
            "BUY",
            "YES",
            0.39,
            205.22,
            80.04,
            "TEST MARKET — проверка кнопки Повторить",
            "https://polymarket.com",
            "test",
            "",
            "",
            3818.7,
            0.495,
            1.0,
            3818.66,
            1930.24,
            "{}",
            "{}",
        ))

        alert_id = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        con.commit()
        return int(alert_id)


async def fetch_trades(limit: int = LIMIT) -> List[Dict[str, Any]]:
    params = {
        "user": POLYMARKET_WALLET,
        "limit": limit,
        "offset": 0,
        "takerOnly": str(TAKER_ONLY).lower(),
    }

    async with aiohttp.ClientSession(headers={"Accept": "application/json"}) as session:
        async with session.get(POLYMARKET_TRADES_URL, params=params, timeout=10) as r:
            text = await r.text()
            if r.status != 200:
                raise RuntimeError(f"Polymarket trades {r.status}: {text[:300]}")
            data = json.loads(text)
            if not isinstance(data, list):
                raise RuntimeError(f"Unexpected trades response: {data}")
            return data


async def fetch_positions() -> List[Dict[str, Any]]:
    params = {
        "user": POLYMARKET_WALLET,
        "sizeThreshold": 0,
        "limit": 500,
    }

    async with aiohttp.ClientSession(headers={"Accept": "application/json"}) as session:
        async with session.get(POLYMARKET_POSITIONS_URL, params=params, timeout=10) as r:
            text = await r.text()
            if r.status != 200:
                print(f"[positions warning] Polymarket positions {r.status}: {text[:200]}")
                return []
            data = json.loads(text)
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and isinstance(data.get("data"), list):
                return data["data"]
            return []


# ============================================================
# BETS / STATS / EXPORT
# ============================================================

def create_bet(user_id: int, alert_id: int, odds: float, stake: float) -> int:
    alert = get_alert(alert_id)
    if not alert:
        raise RuntimeError("Alert not found")

    with closing(db()) as con:
        con.execute("""
        INSERT INTO bets(
            user_id, alert_id, created_at, market_title, market_url, side, outcome,
            player_price, player_usdc_size, player_shares,
            position_shares, position_avg_price, position_current_price,
            position_value, position_pnl,
            my_odds, my_stake, status, profit, tx_hash
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            user_id,
            alert_id,
            now_iso(),
            alert["market_title"],
            alert["market_url"],
            alert["side"],
            alert["outcome"],
            alert["price"],
            alert["usdc_size"],
            alert["size"],
            alert["position_shares"],
            alert["position_avg_price"],
            alert["position_current_price"],
            alert["position_value"],
            alert["position_pnl"],
            odds,
            stake,
            "OPEN",
            0,
            alert["tx_hash"],
        ))

        bet_id = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        con.commit()
        return int(bet_id)


def update_status(user_id: int, bet_id: int, status: str) -> None:
    with closing(db()) as con:
        bet = con.execute(
            "SELECT * FROM bets WHERE id=? AND user_id=?",
            (bet_id, user_id),
        ).fetchone()

        if not bet:
            return

        odds = float(bet["my_odds"])
        stake = float(bet["my_stake"])

        if status == "WIN":
            profit = round(stake * (odds - 1), 2)
        elif status == "LOSE":
            profit = round(-stake, 2)
        else:
            profit = 0

        con.execute(
            "UPDATE bets SET status=?, profit=? WHERE id=? AND user_id=?",
            (status, profit, bet_id, user_id),
        )
        con.commit()


def get_user_stats(user_id: int) -> Dict[str, Any]:
    with closing(db()) as con:
        rows = con.execute("SELECT * FROM bets WHERE user_id=?", (user_id,)).fetchall()

    total = len(rows)
    wins = sum(1 for r in rows if r["status"] == "WIN")
    loses = sum(1 for r in rows if r["status"] == "LOSE")
    voids = sum(1 for r in rows if r["status"] == "VOID")
    open_ = sum(1 for r in rows if r["status"] == "OPEN")
    settled = [r for r in rows if r["status"] in ("WIN", "LOSE")]

    stake = sum(float(r["my_stake"]) for r in rows)
    settled_stake = sum(float(r["my_stake"]) for r in settled)
    profit = sum(float(r["profit"]) for r in rows)
    avg_odds = sum(float(r["my_odds"]) for r in rows) / total if total else 0
    winrate = wins / len(settled) * 100 if settled else 0
    roi = profit / settled_stake * 100 if settled_stake else 0

    return {
        "total": total,
        "open": open_,
        "wins": wins,
        "loses": loses,
        "voids": voids,
        "stake": stake,
        "settled_stake": settled_stake,
        "profit": profit,
        "avg_odds": avg_odds,
        "winrate": winrate,
        "roi": roi,
    }


def export_user_excel(user_id: int) -> Path:
    with closing(db()) as con:
        bets = con.execute(
            "SELECT * FROM bets WHERE user_id=? ORDER BY id ASC",
            (user_id,),
        ).fetchall()

    out = DATA_DIR / f"bets_report_{user_id}.xlsx"

    wb = Workbook()

    ws = wb.active
    ws.title = "My Bets"
    ws.append([
        "ID", "Created UTC", "Status", "My Stake", "My Odds", "Profit",
        "Player Side", "Outcome", "Trade Price", "Trade Shares", "Trade USDC",
        "Position Shares", "Position Avg", "Position Current", "Position Value", "Position PnL",
        "Market", "URL", "TX"
    ])

    for r in bets:
        ws.append([
            r["id"], r["created_at"], r["status"], r["my_stake"], r["my_odds"], r["profit"],
            r["side"], r["outcome"], r["player_price"], r["player_shares"], r["player_usdc_size"],
            r["position_shares"], r["position_avg_price"], r["position_current_price"],
            r["position_value"], r["position_pnl"],
            r["market_title"], r["market_url"], r["tx_hash"]
        ])

    st = wb.create_sheet("Stats")
    s = get_user_stats(user_id)
    st.append(["Metric", "Value"])
    for k, v in s.items():
        st.append([k, v])

    by_market = wb.create_sheet("By Market")
    by_market.append(["Market", "Bets", "Open", "Wins", "Loses", "Voids", "Stake", "Profit", "ROI %", "Winrate %"])

    groups: Dict[str, List[sqlite3.Row]] = {}
    for r in bets:
        groups.setdefault(str(r["market_title"]), []).append(r)

    for market, rows in groups.items():
        settled = [r for r in rows if r["status"] in ("WIN", "LOSE")]
        wins = sum(1 for r in rows if r["status"] == "WIN")
        loses = sum(1 for r in rows if r["status"] == "LOSE")
        voids = sum(1 for r in rows if r["status"] == "VOID")
        open_ = sum(1 for r in rows if r["status"] == "OPEN")
        stake = sum(float(r["my_stake"]) for r in rows)
        settled_stake = sum(float(r["my_stake"]) for r in settled)
        profit = sum(float(r["profit"]) for r in rows)
        roi = profit / settled_stake * 100 if settled_stake else 0
        wr = wins / len(settled) * 100 if settled else 0
        by_market.append([market, len(rows), open_, wins, loses, voids, stake, profit, roi, wr])

    for sheet in wb.worksheets:
        sheet.freeze_panes = "A2"
        for cell in sheet[1]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="EAF2F8")
            cell.alignment = Alignment(horizontal="center")
        for col in sheet.columns:
            width = 12
            for cell in col:
                width = max(width, min(45, len(str(cell.value or "")) + 2))
            sheet.column_dimensions[get_column_letter(col[0].column)].width = width

    wb.save(out)
    return out


# ============================================================
# INLINE UI
# ============================================================

def menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔎 Проверить игрока", callback_data="menu:ping"),
            InlineKeyboardButton(text="📜 Последние 10 сделок", callback_data="menu:last10"),
        ],
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="menu:stats"),
            InlineKeyboardButton(text="📋 Мои ставки", callback_data="menu:bets"),
        ],
        [
            InlineKeyboardButton(text="🟡 Открытые", callback_data="menu:open"),
            InlineKeyboardButton(text="📤 Excel", callback_data="menu:export"),
        ],
        [
            InlineKeyboardButton(text="🧪 Тест алерт", callback_data="menu:test"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data="menu:home"),
        ],
    ])


def alert_kb(alert_id: int, url: str = "") -> InlineKeyboardMarkup:
    row1 = [InlineKeyboardButton(text="🔁 Повторить", callback_data=f"repeat:{alert_id}")]
    if url:
        row1.append(InlineKeyboardButton(text="🔗 Открыть рынок", url=url))

    return InlineKeyboardMarkup(inline_keyboard=[
        row1,
        [InlineKeyboardButton(text="📜 Последние 10 сделок игрока", callback_data="menu:last10")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu:home")],
    ])


def bet_kb(bet_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Win", callback_data=f"status:{bet_id}:WIN"),
            InlineKeyboardButton(text="❌ Lose", callback_data=f"status:{bet_id}:LOSE"),
            InlineKeyboardButton(text="↩️ Void", callback_data=f"status:{bet_id}:VOID"),
        ],
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="menu:stats"),
            InlineKeyboardButton(text="📤 Excel", callback_data="menu:export"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu:home")],
    ])


def login_text() -> str:
    return (
        "🔐 <b>Доступ закрыт</b>\n\n"
        "Введи пароль, чтобы пользоваться ботом."
    )


def menu_text(user_id: int) -> str:
    s = get_user_stats(user_id)
    return (
        "🏠 <b>Главное меню</b>\n\n"
        f"👛 Игрок:\n<code>{h(POLYMARKET_WALLET)}</code>\n"
        f"⏱ Проверка: <b>{POLL_SECONDS}s</b>\n\n"
        f"Твои ставки: <b>{s['total']}</b> | Открытые: <b>{s['open']}</b>\n"
        f"P/L: <b>{s['profit']:+.2f}</b> | ROI: <b>{s['roi']:+.1f}%</b>"
    )


def position_text_from_alert(alert: sqlite3.Row) -> str:
    if alert["position_shares"] is None and alert["position_value"] is None:
        return "📦 Позиция игрока: <i>не удалось получить из positions API</i>"

    parts = ["📦 <b>Текущая позиция игрока по этому outcome</b>"]

    if alert["position_shares"] is not None:
        parts.append(f"Shares всего: <b>{num(alert['position_shares'], 2)}</b>")

    if alert["position_avg_price"] is not None:
        parts.append(f"AVG вход: <b>{price_cents(alert['position_avg_price'])}</b>")

    if alert["position_current_price"] is not None:
        parts.append(f"Current: <b>{price_cents(alert['position_current_price'])}</b>")

    if alert["position_value"] is not None:
        parts.append(f"Value: <b>{money(alert['position_value'])}</b>")

    if alert["position_pnl"] is not None:
        parts.append(f"PnL: <b>{float(alert['position_pnl']):+,.2f}$</b>")

    return "\n".join(parts)


def alert_text(alert: sqlite3.Row, header: str = "🟢 <b>НОВАЯ СДЕЛКА ИГРОКА</b>") -> str:
    side_emoji = "🟢" if alert["side"] == "BUY" else "🔴" if alert["side"] == "SELL" else "⚪️"
    return (
        f"{header}\n\n"
        f"{side_emoji} <b>{h(alert['side'])} {h(alert['outcome'])}</b>\n"
        f"Цена сделки: <b>{price_cents(alert['price'])}</b> / <code>{price_decimal(alert['price'])}</code>\n"
        f"Размер этой сделки: <b>{money(alert['usdc_size'])}</b>\n"
        f"Shares в этой сделке: <b>{num(alert['size'], 2)}</b>\n\n"
        f"📌 <b>Рынок</b>\n{h(alert['market_title'])}\n"
        f"🔗 {h(alert['market_url'])}\n"
        f"TX: <code>{h(short_tx(alert['tx_hash']))}</code>\n\n"
        f"{position_text_from_alert(alert)}"
    )


def bet_text(bet: sqlite3.Row) -> str:
    emo = {"OPEN": "🟡", "WIN": "✅", "LOSE": "❌", "VOID": "↩️"}.get(bet["status"], "⚪️")

    return (
        f"{emo} <b>Ставка #{bet['id']}</b> — <b>{h(bet['status'])}</b>\n\n"
        f"📌 {h(bet['market_title'])}\n"
        f"🎯 Повтор: <b>{h(bet['side'])} {h(bet['outcome'])}</b>\n"
        f"Цена игрока: <b>{price_cents(bet['player_price'])}</b>\n"
        f"Сделка игрока: <b>{money(bet['player_usdc_size'])}</b> / {num(bet['player_shares'], 2)} shares\n\n"
        f"📈 Твой кф: <b>{price_decimal(bet['my_odds'])}</b>\n"
        f"💵 Твоя сумма: <b>{money(bet['my_stake'])}</b>\n"
        f"💰 P/L: <b>{float(bet['profit']):+,.2f}$</b>\n"
        f"🔗 {h(bet['market_url'])}"
    )


def stats_text(user_id: int) -> str:
    s = get_user_stats(user_id)
    return (
        f"📊 <b>Твоя статистика</b>\n\n"
        f"Всего ставок: <b>{s['total']}</b>\n"
        f"Открытых: <b>{s['open']}</b>\n"
        f"Win/Lose/Void: <b>{s['wins']}/{s['loses']}/{s['voids']}</b>\n"
        f"Winrate: <b>{s['winrate']:.1f}%</b>\n"
        f"Оборот: <b>{money(s['stake'])}</b>\n"
        f"Закрытый оборот: <b>{money(s['settled_stake'])}</b>\n"
        f"Профит: <b>{s['profit']:+,.2f}$</b>\n"
        f"ROI: <b>{s['roi']:+.1f}%</b>\n"
        f"Средний кф: <b>{s['avg_odds']:.3f}</b>"
    )


# ============================================================
# SENDERS
# ============================================================

async def send_alert_to_user(bot: Bot, user_id: int, alert_id: int, header: str = "🟢 <b>НОВАЯ СДЕЛКА ИГРОКА</b>") -> None:
    alert = get_alert(alert_id)
    if not alert:
        return

    await bot.send_message(
        user_id,
        alert_text(alert, header=header),
        reply_markup=alert_kb(alert_id, alert["market_url"] or ""),
        disable_web_page_preview=True,
    )


async def send_last_10_trades(chat_id: int, bot: Bot) -> None:
    trades = await fetch_trades(limit=10)
    positions = await fetch_positions()

    if not trades:
        await bot.send_message(chat_id, "Последние сделки игрока не найдены.", reply_markup=menu_kb())
        return

    await bot.send_message(chat_id, "📜 <b>Последние 10 сделок игрока</b>")

    for idx, t in enumerate(reversed(trades), start=1):
        position = match_position_for_trade(t, positions)
        alert_id = save_alert_from_trade(t, position)
        alert = get_alert(alert_id)

        await bot.send_message(
            chat_id,
            alert_text(alert, header=f"📜 <b>Сделка #{idx} из последних 10</b>"),
            reply_markup=alert_kb(alert_id, alert["market_url"] or ""),
            disable_web_page_preview=True,
        )

    await bot.send_message(chat_id, "Готово. Выбери действие:", reply_markup=menu_kb())


async def send_bets_list(chat_id: int, bot: Bot, status: Optional[str] = None) -> None:
    with closing(db()) as con:
        if status:
            rows = con.execute(
                "SELECT * FROM bets WHERE user_id=? AND status=? ORDER BY id DESC LIMIT 10",
                (chat_id, status),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM bets WHERE user_id=? ORDER BY id DESC LIMIT 10",
                (chat_id,),
            ).fetchall()

    if not rows:
        label = status if status else "ALL"
        await bot.send_message(chat_id, f"Ставок пока нет. Фильтр: <b>{h(label)}</b>", reply_markup=menu_kb())
        return

    for r in rows:
        await bot.send_message(
            chat_id,
            bet_text(r),
            reply_markup=bet_kb(int(r["id"])),
            disable_web_page_preview=True,
        )

    await bot.send_message(chat_id, "Выбери действие:", reply_markup=menu_kb())


# ============================================================
# WATCHER
# ============================================================

async def watcher(bot: Bot) -> None:
    first_loop = seen_count() == 0

    while True:
        started = time.perf_counter()

        try:
            trades = await fetch_trades(limit=LIMIT)
            positions = await fetch_positions()

            fresh: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []

            for t in trades:
                if calc_usdc(t) < MIN_PLAYER_USDC:
                    continue

                uid = trade_uid(t)
                if not is_seen(uid):
                    mark_seen(uid)
                    fresh.append((t, match_position_for_trade(t, positions)))

            if first_loop and not FIRST_RUN_SEND_HISTORY:
                print("[watcher] first run: history saved, old alerts skipped")
                first_loop = False
            else:
                fresh.sort(key=lambda x: float(x[0].get("timestamp") or 0))

                users = authed_users()
                for t, pos in fresh:
                    alert_id = save_alert_from_trade(t, pos)

                    for user_id in users:
                        try:
                            await send_alert_to_user(bot, user_id, alert_id)
                        except Exception as e:
                            print(f"[send alert error user={user_id}]", repr(e))

                first_loop = False

        except Exception as e:
            print("[watcher error]", repr(e))

        elapsed = time.perf_counter() - started
        await asyncio.sleep(max(0.05, POLL_SECONDS - elapsed))


# ============================================================
# HANDLERS
# ============================================================

async def start_handler(message: Message) -> None:
    if message.from_user is None:
        return

    if is_authed_user(message.from_user.id):
        await message.answer(menu_text(message.from_user.id), reply_markup=menu_kb())
    else:
        await message.answer(login_text())


async def password_handler(message: Message) -> None:
    if message.from_user is None:
        return

    if is_authed_user(message.from_user.id):
        await message.answer(menu_text(message.from_user.id), reply_markup=menu_kb())
        return

    if (message.text or "").strip() == LOGIN_PASSWORD:
        save_user(
            user_id=message.from_user.id,
            username=message.from_user.username or "",
            first_name=message.from_user.first_name or "",
        )
        await message.answer(
            "✅ Доступ открыт.\n\nТеперь бот будет присылать тебе новые сделки игрока.",
        )
        await message.answer(menu_text(message.from_user.id), reply_markup=menu_kb())
    else:
        await message.answer("❌ Неверный пароль.\n\n" + login_text())


async def menu_callback(cb: CallbackQuery, bot: Bot) -> None:
    if cb.from_user is None or not require_auth_cb(cb):
        await cb.answer("Сначала введи пароль в чат боту.", show_alert=True)
        return

    action = cb.data.split(":")[1]
    user_id = cb.from_user.id

    if action == "home":
        await cb.message.edit_text(menu_text(user_id), reply_markup=menu_kb())
        await cb.answer()
        return

    if action == "stats":
        await cb.message.answer(stats_text(user_id), reply_markup=menu_kb())
        await cb.answer()
        return

    if action == "bets":
        await cb.answer()
        await send_bets_list(user_id, bot, status=None)
        return

    if action == "open":
        await cb.answer()
        await send_bets_list(user_id, bot, status="OPEN")
        return

    if action == "export":
        path = export_user_excel(user_id)
        await cb.message.answer_document(FSInputFile(path), caption="📤 Твой Excel-отчёт")
        await cb.answer()
        return

    if action == "test":
        alert_id = create_test_alert()
        await send_alert_to_user(bot, user_id, alert_id, header="🧪 <b>ТЕСТОВЫЙ АЛЕРТ</b>")
        await cb.answer()
        return

    if action == "last10":
        await cb.answer()
        await send_last_10_trades(user_id, bot)
        return

    if action == "ping":
        await cb.message.answer("🔎 Проверяю реальные сделки и текущие позиции игрока...")
        try:
            await send_last_10_trades(user_id, bot)
            await cb.message.answer("✅ Связка Polymarket → Telegram работает.", reply_markup=menu_kb())
        except Exception as e:
            await cb.message.answer(f"❌ Ошибка проверки:\n<code>{h(e)}</code>", reply_markup=menu_kb())
        await cb.answer()
        return

    await cb.answer("Неизвестное действие", show_alert=True)


async def repeat_callback(cb: CallbackQuery, state: FSMContext) -> None:
    if cb.from_user is None or not require_auth_cb(cb):
        await cb.answer("Сначала введи пароль в чат боту.", show_alert=True)
        return

    alert_id = int(cb.data.split(":")[1])
    alert = get_alert(alert_id)

    if not alert:
        await cb.answer("Алерт не найден", show_alert=True)
        return

    await state.set_state(RepeatFlow.waiting_odds)
    await state.update_data(alert_id=alert_id, user_id=cb.from_user.id)

    await cb.message.answer(
        f"🔁 <b>Записываем повтор</b>\n\n"
        f"📌 {h(alert['market_title'])}\n"
        f"🎯 Игрок: <b>{h(alert['side'])} {h(alert['outcome'])}</b>\n"
        f"Цена игрока: <b>{price_cents(alert['price'])}</b>\n"
        f"Размер сделки игрока: <b>{money(alert['usdc_size'])}</b>\n\n"
        f"Какой кф ты взял? Например: <code>2.15</code>"
    )

    await cb.answer()


async def odds_handler(message: Message, state: FSMContext) -> None:
    if message.from_user is None or not require_auth_message(message):
        return

    odds = parse_float(message.text)

    if odds is None or odds < 1.01:
        await message.answer("Введи кф числом больше 1.01. Пример: <code>2.15</code>")
        return

    await state.update_data(odds=odds)
    await state.set_state(RepeatFlow.waiting_stake)
    await message.answer("Какую сумму поставил? Например: <code>25</code>")


async def stake_handler(message: Message, state: FSMContext) -> None:
    if message.from_user is None or not require_auth_message(message):
        return

    stake = parse_float(message.text)

    if stake is None or stake <= 0:
        await message.answer("Введи сумму числом. Пример: <code>25</code>")
        return

    data = await state.get_data()
    alert_id = int(data["alert_id"])
    odds = float(data["odds"])
    user_id = int(data["user_id"])

    bet_id = create_bet(user_id, alert_id, odds, stake)
    await state.clear()

    bet = get_bet(user_id, bet_id)

    await message.answer(
        "✅ Ставка записана.\n\n" + bet_text(bet),
        reply_markup=bet_kb(bet_id),
        disable_web_page_preview=True,
    )


async def status_callback(cb: CallbackQuery) -> None:
    if cb.from_user is None or not require_auth_cb(cb):
        await cb.answer("Сначала введи пароль в чат боту.", show_alert=True)
        return

    _, bet_id, status = cb.data.split(":")
    bet_id = int(bet_id)

    if status not in STATUSES:
        await cb.answer("Некорректный статус", show_alert=True)
        return

    update_status(cb.from_user.id, bet_id, status)
    bet = get_bet(cb.from_user.id, bet_id)

    if not bet:
        await cb.answer("Ставка не найдена", show_alert=True)
        return

    await cb.message.edit_text(
        bet_text(bet),
        reply_markup=bet_kb(bet_id),
        disable_web_page_preview=True,
    )

    await cb.answer(status)


# ============================================================
# WEB HEALTHCHECK
# ============================================================

async def health(request: web.Request) -> web.Response:
    return web.json_response({
        "ok": True,
        "service": "polymarket-inline-auth-bot",
        "wallet": POLYMARKET_WALLET,
        "poll_seconds": POLL_SECONDS,
        "authed_users": len(authed_users()),
    })


async def stats_json(request: web.Request) -> web.Response:
    return web.json_response({
        "ok": True,
        "authed_users": len(authed_users()),
        "seen_trades": seen_count(),
    })


async def start_web() -> None:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_get("/stats.json", stats_json)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    print(f"[web] running on port {PORT}")


# ============================================================
# MAIN
# ============================================================

async def main() -> None:
    check_config()
    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    await bot.delete_webhook(drop_pending_updates=True)

    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(start_handler, CommandStart())
    dp.callback_query.register(menu_callback, F.data.startswith("menu:"))
    dp.callback_query.register(repeat_callback, F.data.startswith("repeat:"))
    dp.callback_query.register(status_callback, F.data.startswith("status:"))

    dp.message.register(odds_handler, RepeatFlow.waiting_odds)
    dp.message.register(stake_handler, RepeatFlow.waiting_stake)

    # Last handler: login/password text.
    dp.message.register(password_handler)

    await start_web()

    for user_id in authed_users():
        try:
            await bot.send_message(
                user_id,
                "✅ Бот перезапущен и работает.\n"
                f"👛 Игрок: <code>{h(POLYMARKET_WALLET)}</code>",
                reply_markup=menu_kb(),
            )
        except Exception as e:
            print(f"[startup notify error user={user_id}]", repr(e))

    asyncio.create_task(watcher(bot))

    print("[telegram] polling started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
