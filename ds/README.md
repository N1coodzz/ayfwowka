# Polymarket Railway Telegram Tracker

Repo-ready бот для теста через GitHub + Railway.

## Что делает

- Проверяет реальные сделки кошелька Polymarket.
- Присылает уведомление в Telegram.
- В уведомлении есть кнопка `🔁 Повторить`.
- Спрашивает твой кф и сумму.
- Записывает ставку в SQLite.
- Позволяет выставить результат `Win / Lose / Void`.
- Считает ROI, winrate, средний кф, профит.
- Отдаёт Excel по `/export`.
- Имеет web healthcheck `/health` для Railway.

## Railway Variables

```env
TELEGRAM_BOT_TOKEN=твой_токен
TELEGRAM_CHAT_ID=703605167
POLYMARKET_WALLET=0x88d1e3eb1b1b71d498db3b70a9e03f4b8238c1c3
POLL_SECONDS=1.0
LIMIT=20
MIN_PLAYER_USDC=0
TAKER_ONLY=false
FIRST_RUN_SEND_HISTORY=false
DATA_DIR=.
```

## Проверка

В Telegram:

```text
/start
/ping_polymarket
/test_alert
/stats
/bets
/export
```

`/ping_polymarket` отправит последние реальные сделки кошелька и покажет, что связка Polymarket → Telegram работает.

## Важно про хранение

Для короткого теста можно `DATA_DIR=.`. Для нормальной работы на Railway создай Volume, mount path `/data`, и поставь `DATA_DIR=/data`.

## Безопасность

Не коммить `.env` и токен в GitHub. Если токен уже отправлялся в чат — перевыпусти его в BotFather.
