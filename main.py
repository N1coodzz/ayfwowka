import asyncio
import html
import json
import os
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
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


# ============================================================
# TEST CONFIG — already filled for Replit test
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8586607615:AAFknNl5-ihG0FpykOpNHKbUqAOWHAaxklI").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "703605167").strip()
POLYMARKET_WALLET = os.getenv("POLYMARKET_WALLET", "0x88d1e3eb1b1b71d498db3b70a9e03f4b8238c1c3").strip().lower()

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.0"))
LIMIT = int(os.getenv("LIMIT", "20"))
TAKER_ONLY = os.getenv("TAKER_ONLY", "false").lower() == "true"
MIN_PLAYER_USDC = float(os.getenv("MIN_PLAYER_USDC", "0"))

# false = at start saves old trades and alerts only future trades.
# use /ping_polymarket to force-check real latest trades immediately.
FIRST_RUN_SEND_HISTORY = os.getenv("FIRST_RUN_SEND_HISTORY", "false").lower() == "true"

PORT = int(os.getenv("PORT", "8080"))
DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "bets.db"
XLSX_PATH = DATA_DIR / "bets_report.xlsx"

POLYMARKET_TRADES_URL = "https://data-api.polymarket.com/trades"
STATUSES = {"OPEN", "WIN", "LOSE", "VOID"}


class RepeatFlow(StatesGroup):
    waiting_odds = State()
    waiting_stake = State()


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def h(x: Any) -> str:
    return html.escape(str(x if x is not None else ""))


def fnum(x: Any, n: int = 2) -> str:
    try:
        return f"{float(x):.{n}f}"
    except Exception:
        return str(x)


def as_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(str(x).replace(",", ".").strip())
    except Exception:
        return default


def parse_float(text: Any) -> Optional[float]:
    try:
        return float(str(text).replace(",", ".").strip())
    except Exception:
        return None


def check_config() -> None:
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if not POLYMARKET_WALLET:
        missing.append("POLYMARKET_WALLET")
    if missing:
        raise RuntimeError(f"Missing config: {', '.join(missing)}")


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    with closing(db()) as con:
        con.executescript("""
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
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER,
            created_at TEXT NOT NULL,
            market_title TEXT,
            market_url TEXT,
            side TEXT,
            outcome TEXT,
            player_price REAL,
            player_usdc_size REAL,
            my_odds REAL,
            my_stake REAL,
            status TEXT DEFAULT 'OPEN',
            profit REAL DEFAULT 0,
            tx_hash TEXT
        );
        """)
        con.commit()


def is_allowed(message: Message) -> bool:
    return str(message.chat.id) == str(TELEGRAM_CHAT_ID)


def is_allowed_cb(cb: CallbackQuery) -> bool:
    return cb.from_user is not None and str(cb.from_user.id) == str(TELEGRAM_CHAT_ID)


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


def save_alert_from_trade(t: Dict[str, Any]) -> int:
    uid = trade_uid(t)
    with closing(db()) as con:
        con.execute("""
        INSERT OR IGNORE INTO alerts(
            trade_uid, created_at, side, outcome, price, size, usdc_size,
            market_title, market_url, tx_hash, raw_json
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
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
            market_title, market_url, tx_hash, raw_json
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """, (
            f"test-{time.time()}",
            now_iso(),
            "BUY",
            "YES",
            0.45,
            100,
            45,
            "TEST MARKET — проверка кнопки Повторить",
            "https://polymarket.com",
            "test",
            "{}",
        ))

        alert_id = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        con.commit()
        return int(alert_id)


def get_alert(alert_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as con:
        return con.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()


def create_bet(alert_id: int, odds: float, stake: float) -> int:
    alert = get_alert(alert_id)
    if not alert:
        raise RuntimeError("Alert not found")

    with closing(db()) as con:
        con.execute("""
        INSERT INTO bets(
            alert_id, created_at, market_title, market_url, side, outcome,
            player_price, player_usdc_size, my_odds, my_stake, status, profit, tx_hash
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            alert_id,
            now_iso(),
            alert["market_title"],
            alert["market_url"],
            alert["side"],
            alert["outcome"],
            alert["price"],
            alert["usdc_size"],
            odds,
            stake,
            "OPEN",
            0,
            alert["tx_hash"],
        ))

        bet_id = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        con.commit()
        return int(bet_id)


def get_bet(bet_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as con:
        return con.execute("SELECT * FROM bets WHERE id=?", (bet_id,)).fetchone()


def update_status(bet_id: int, status: str) -> None:
    with closing(db()) as con:
        bet = con.execute("SELECT * FROM bets WHERE id=?", (bet_id,)).fetchone()
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
            "UPDATE bets SET status=?, profit=? WHERE id=?",
            (status, profit, bet_id),
        )
        con.commit()


def stats() -> Dict[str, Any]:
    with closing(db()) as con:
        rows = con.execute("SELECT * FROM bets").fetchall()

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


def make_alert_text(alert: sqlite3.Row) -> str:
    emoji = "🟢" if alert["side"] == "BUY" else "🔴" if alert["side"] == "SELL" else "⚪️"
    tx = str(alert["tx_hash"] or "")
    short = tx[:10] + "..." + tx[-6:] if len(tx) > 16 else tx

    return (
        f"{emoji} <b>{h(alert['side'])} {h(alert['outcome'])}</b> @ <b>{fnum(alert['price'], 3)}</b>\n"
        f"💵 Игрок: ~<b>{fnum(alert['usdc_size'], 2)} USDC</b> | 🎟 {fnum(alert['size'], 2)}\n"
        f"📌 {h(alert['market_title'])}\n"
        f"🔗 {h(alert['market_url'])}\n"
        f"TX: <code>{h(short)}</code>"
    )


def make_bet_text(bet: sqlite3.Row) -> str:
    emo = {"OPEN": "🟡", "WIN": "✅", "LOSE": "❌", "VOID": "↩️"}.get(bet["status"], "⚪️")

    return (
        f"{emo} <b>Ставка #{bet['id']}</b> — <b>{h(bet['status'])}</b>\n"
        f"📌 {h(bet['market_title'])}\n"
        f"🎯 {h(bet['side'])} {h(bet['outcome'])}\n"
        f"📈 Кф: <b>{fnum(bet['my_odds'], 3)}</b>\n"
        f"💵 Сумма: <b>{fnum(bet['my_stake'], 2)}</b>\n"
        f"💰 P/L: <b>{float(bet['profit']):+.2f}</b>\n"
        f"🔗 {h(bet['market_url'])}"
    )


def repeat_kb(alert_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Повторить", callback_data=f"repeat:{alert_id}")]
    ])


def bet_kb(bet_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Win", callback_data=f"status:{bet_id}:WIN"),
            InlineKeyboardButton(text="❌ Lose", callback_data=f"status:{bet_id}:LOSE"),
            InlineKeyboardButton(text="↩️ Void", callback_data=f"status:{bet_id}:VOID"),
        ]
    ])


async def fetch_trades() -> List[Dict[str, Any]]:
    params = {
        "user": POLYMARKET_WALLET,
        "limit": LIMIT,
        "offset": 0,
        "takerOnly": str(TAKER_ONLY).lower(),
    }

    async with aiohttp.ClientSession(headers={"Accept": "application/json"}) as session:
        async with session.get(POLYMARKET_TRADES_URL, params=params, timeout=8) as r:
            text = await r.text()
            if r.status != 200:
                raise RuntimeError(f"Polymarket {r.status}: {text[:300]}")
            data = json.loads(text)
            if not isinstance(data, list):
                raise RuntimeError(f"Unexpected response: {data}")
            return data


async def watcher(bot: Bot) -> None:
    first_loop = seen_count() == 0

    while True:
        started = time.perf_counter()

        try:
            trades = await fetch_trades()
            fresh = []

            for t in trades:
                if calc_usdc(t) < MIN_PLAYER_USDC:
                    continue

                uid = trade_uid(t)
                if not is_seen(uid):
                    mark_seen(uid)
                    fresh.append(t)

            if first_loop and not FIRST_RUN_SEND_HISTORY:
                print("[watcher] first run: history saved, old alerts skipped")
                first_loop = False
            else:
                fresh.sort(key=lambda x: float(x.get("timestamp") or 0))

                for t in fresh:
                    alert_id = save_alert_from_trade(t)
                    alert = get_alert(alert_id)
                    await bot.send_message(
                        TELEGRAM_CHAT_ID,
                        make_alert_text(alert),
                        reply_markup=repeat_kb(alert_id),
                        disable_web_page_preview=True,
                    )

                first_loop = False

        except Exception as e:
            print("[watcher error]", repr(e))

        elapsed = time.perf_counter() - started
        await asyncio.sleep(max(0.05, POLL_SECONDS - elapsed))


async def cmd_start(message: Message) -> None:
    if not is_allowed(message):
        return

    await message.answer(
        "✅ Бот работает.\n\n"
        f"👛 Wallet:\n<code>{h(POLYMARKET_WALLET)}</code>\n"
        f"⏱ Poll: <b>{POLL_SECONDS}s</b>\n\n"
        "Команды:\n"
        "/ping_polymarket — проверить реальные сделки\n"
        "/test_alert — тест кнопки Повторить\n"
        "/stats — статистика\n"
        "/bets — последние ставки\n"
        "/export — Excel"
    )


async def cmd_ping(message: Message) -> None:
    if not is_allowed(message):
        return

    await message.answer("🔎 Проверяю реальные сделки Polymarket...")

    try:
        trades = await fetch_trades()
        trades = [t for t in trades if calc_usdc(t) >= MIN_PLAYER_USDC]

        if not trades:
            await message.answer("Сделок не найдено.")
            return

        for t in reversed(trades[:3]):
            alert_id = save_alert_from_trade(t)
            alert = get_alert(alert_id)
            await message.answer(
                make_alert_text(alert),
                reply_markup=repeat_kb(alert_id),
                disable_web_page_preview=True,
            )

        await message.answer("✅ Связка Polymarket → Telegram работает.")

    except Exception as e:
        await message.answer(f"❌ Ошибка:\n<code>{h(e)}</code>")


async def cmd_test(message: Message) -> None:
    if not is_allowed(message):
        return

    alert_id = create_test_alert()
    alert = get_alert(alert_id)
    await message.answer(
        make_alert_text(alert),
        reply_markup=repeat_kb(alert_id),
        disable_web_page_preview=True,
    )


async def cmd_stats(message: Message) -> None:
    if not is_allowed(message):
        return

    s = stats()
    await message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"Всего ставок: <b>{s['total']}</b>\n"
        f"Открытых: <b>{s['open']}</b>\n"
        f"Win/Lose/Void: <b>{s['wins']}/{s['loses']}/{s['voids']}</b>\n"
        f"Winrate: <b>{s['winrate']:.1f}%</b>\n"
        f"Оборот: <b>{s['stake']:.2f}</b>\n"
        f"Профит: <b>{s['profit']:+.2f}</b>\n"
        f"ROI: <b>{s['roi']:+.1f}%</b>\n"
        f"Средний кф: <b>{s['avg_odds']:.2f}</b>"
    )


async def cmd_bets(message: Message) -> None:
    if not is_allowed(message):
        return

    with closing(db()) as con:
        rows = con.execute("SELECT * FROM bets ORDER BY id DESC LIMIT 10").fetchall()

    if not rows:
        await message.answer("Ставок пока нет.")
        return

    for bet in rows:
        await message.answer(
            make_bet_text(bet),
            reply_markup=bet_kb(int(bet["id"])),
            disable_web_page_preview=True,
        )


async def cmd_export(message: Message) -> None:
    if not is_allowed(message):
        return

    with closing(db()) as con:
        bets = con.execute("SELECT * FROM bets ORDER BY id ASC").fetchall()
        alerts = con.execute("SELECT * FROM alerts ORDER BY id ASC").fetchall()

    wb = Workbook()

    ws = wb.active
    ws.title = "Bets"
    ws.append([
        "ID", "Created", "Status", "Stake", "Odds", "Profit",
        "Side", "Outcome", "Player Price", "Player USDC",
        "Market", "URL", "TX"
    ])

    for r in bets:
        ws.append([
            r["id"], r["created_at"], r["status"], r["my_stake"], r["my_odds"],
            r["profit"], r["side"], r["outcome"], r["player_price"],
            r["player_usdc_size"], r["market_title"], r["market_url"], r["tx_hash"]
        ])

    st = wb.create_sheet("Stats")
    s = stats()
    st.append(["Metric", "Value"])
    for k, v in s.items():
        st.append([k, v])

    al = wb.create_sheet("Alerts")
    al.append([
        "ID", "Created", "Side", "Outcome", "Price", "Size",
        "USDC", "Market", "URL", "TX"
    ])

    for r in alerts:
        al.append([
            r["id"], r["created_at"], r["side"], r["outcome"], r["price"],
            r["size"], r["usdc_size"], r["market_title"], r["market_url"], r["tx_hash"]
        ])

    wb.save(XLSX_PATH)
    await message.answer_document(FSInputFile(XLSX_PATH), caption="📊 Excel-отчёт")


async def cb_repeat(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_cb(cb):
        return

    alert_id = int(cb.data.split(":")[1])
    alert = get_alert(alert_id)

    if not alert:
        await cb.answer("Алерт не найден", show_alert=True)
        return

    await state.set_state(RepeatFlow.waiting_odds)
    await state.update_data(alert_id=alert_id)

    await cb.message.answer(
        f"🔁 Записываем повтор.\n\n"
        f"📌 {h(alert['market_title'])}\n"
        f"🎯 {h(alert['side'])} {h(alert['outcome'])} @ {fnum(alert['price'], 3)}\n\n"
        f"Какой кф ты взял? Например: <code>2.15</code>"
    )

    await cb.answer()


async def get_odds(message: Message, state: FSMContext) -> None:
    if not is_allowed(message):
        return

    odds = parse_float(message.text)

    if odds is None or odds < 1.01:
        await message.answer("Введи кф числом, например: <code>2.15</code>")
        return

    await state.update_data(odds=odds)
    await state.set_state(RepeatFlow.waiting_stake)
    await message.answer("Какую сумму поставил? Например: <code>25</code>")


async def get_stake(message: Message, state: FSMContext) -> None:
    if not is_allowed(message):
        return

    stake = parse_float(message.text)

    if stake is None or stake <= 0:
        await message.answer("Введи сумму числом, например: <code>25</code>")
        return

    data = await state.get_data()
    alert_id = int(data["alert_id"])
    odds = float(data["odds"])

    bet_id = create_bet(alert_id, odds, stake)
    await state.clear()

    bet = get_bet(bet_id)
    await message.answer(
        "✅ Ставка записана.\n\n" + make_bet_text(bet),
        reply_markup=bet_kb(bet_id),
        disable_web_page_preview=True,
    )


async def cb_status(cb: CallbackQuery) -> None:
    if not is_allowed_cb(cb):
        return

    _, bet_id, status = cb.data.split(":")
    bet_id = int(bet_id)

    if status not in STATUSES:
        return

    update_status(bet_id, status)
    bet = get_bet(bet_id)

    await cb.message.edit_text(
        make_bet_text(bet),
        reply_markup=bet_kb(bet_id),
        disable_web_page_preview=True,
    )

    await cb.answer(status)


async def health(request: web.Request) -> web.Response:
    return web.json_response({
        "ok": True,
        "service": "polymarket-telegram-bot",
        "wallet": POLYMARKET_WALLET,
        "poll_seconds": POLL_SECONDS,
    })


async def stats_json(request: web.Request) -> web.Response:
    return web.json_response(stats())


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


async def main() -> None:
    check_config()
    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    await bot.delete_webhook(drop_pending_updates=True)

    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_ping, Command("ping_polymarket"))
    dp.message.register(cmd_test, Command("test_alert"))
    dp.message.register(cmd_stats, Command("stats"))
    dp.message.register(cmd_bets, Command("bets"))
    dp.message.register(cmd_export, Command("export"))

    dp.callback_query.register(cb_repeat, F.data.startswith("repeat:"))
    dp.callback_query.register(cb_status, F.data.startswith("status:"))

    dp.message.register(get_odds, RepeatFlow.waiting_odds)
    dp.message.register(get_stake, RepeatFlow.waiting_stake)

    await start_web()

    await bot.send_message(
        TELEGRAM_CHAT_ID,
        "✅ Replit bot запущен.\n"
        f"👛 Wallet: <code>{h(POLYMARKET_WALLET)}</code>\n"
        "Проверка реальных сделок: /ping_polymarket"
    )

    asyncio.create_task(watcher(bot))

    print("[telegram] polling started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
