# ============================================================
# SOLANA EARLY SIGNAL — PUMP.FUN LIVE TRADE COLLECTOR
# GOOGLE COLAB | TELEGRAM | WEBSOCKET
# FINAL DIAGNOSTIC VERSION
# ============================================================

!pip -q install aiohttp websockets nest_asyncio base58

import asyncio
import aiohttp
import websockets
import nest_asyncio
import json
import time
import base64
import hashlib
import struct
import base58
import os
import csv
import html

from collections import defaultdict, deque
from google.colab import userdata

nest_asyncio.apply()

# ============================================================
# 1. TELEGRAM SECRETS — IMPROVED DIAGNOSTICS
# ============================================================

def read_colab_secret(names):
    errors = []

    for secret_name in names:
        try:
            value = userdata.get(secret_name)

            if value is not None and str(value).strip():
                return str(value).strip(), None, secret_name

        except Exception as exc:
            errors.append(
                f"{secret_name}: {type(exc).__name__}: {exc}"
            )

    return "", (
        "; ".join(errors) if errors
        else "Secret not found"
    ), None


TELEGRAM_BOT_TOKEN, TOKEN_READ_ERROR, TOKEN_SECRET_NAME = (
    read_colab_secret([
        "TELEGRAM_BOT_TOKEN",
        "BOT_TOKEN",
        "TELEGRAM_TOKEN",
    ])
)

TELEGRAM_CHAT_ID, CHAT_READ_ERROR, CHAT_SECRET_NAME = (
    read_colab_secret([
        "TELEGRAM_CHAT_ID",
        "CHAT_ID",
        "TELEGRAM_CHATID",
    ])
)

print("Telegram token loaded:", bool(TELEGRAM_BOT_TOKEN))
print("Telegram chat ID loaded:", bool(TELEGRAM_CHAT_ID))

if not TELEGRAM_BOT_TOKEN:
    print("Telegram token read diagnostic:", TOKEN_READ_ERROR)

if not TELEGRAM_CHAT_ID:
    print("Telegram chat ID read diagnostic:", CHAT_READ_ERROR)


# ============================================================
# 2. SOURCES
# ============================================================

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"

RPC_WS_URLS = [
    "wss://solana-rpc.publicnode.com",
    "wss://api.mainnet.solana.com",
]

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

TRADE_DISCRIMINATOR = hashlib.sha256(
    b"event:TradeEvent"
).digest()[:8]


# ============================================================
# 3. SETTINGS — ORIGINAL THRESHOLDS PRESERVED
# ============================================================

WINDOW_SEC = 300
MIN_SCORE_ALERT = 45.0
MIN_PRESCORE = 25
MIN_UNIQUE_WALLETS = 12

MIN_MCAP_SOL = 0.0
MIN_TRANSACTION_SOL = 0.008

MAX_WALLET_TRANSACTIONS = 3
BUNDLE_WINDOW_SEC = 5
ALERT_COOLDOWN_SEC = 300

SOL_USD = 110.0
TOTAL_SUPPLY = 1_000_000_000_000_000
TOKEN_DECIMALS = 6
LAMPORTS_PER_SOL = 1_000_000_000

MAX_SAMPLE_EVENTS = 3

OUTPUT_DIR = "/content/SOLANA_EARLY_SIGNAL"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TRADES_FILE = os.path.join(
    OUTPUT_DIR, "live_trades.csv"
)

TOKENS_FILE = os.path.join(
    OUTPUT_DIR, "token_summary.csv"
)

SAMPLES_FILE = os.path.join(
    OUTPUT_DIR, "trade_event_samples.txt"
)

WEIGHTS = {
    "unique_wallets": 0.24,
    "velocity": 0.10,
    "buy_pressure": 0.16,
    "unique_ratio": 0.10,
    "bundle": -0.12,
    "dev_holding": -0.10,
    "dev_sold": -0.20,
    "concentration": -0.08,
}

THR = {
    "max_dev_holding": 0.20,
    "max_top3_share": 0.65,
    "min_unique_wallets": MIN_UNIQUE_WALLETS,
    "min_buy_delta": -0.25,
}


# ============================================================
# 4. GLOBAL STATE
# ============================================================

http_session = None

tokens = {}
stats = defaultdict(int)

csv_lock = asyncio.Lock()
alert_lock = asyncio.Lock()

seen_trade_events = set()
MAX_SEEN_TRADE_EVENTS = 100000


# ============================================================
# 5. CSV INITIALIZATION
# ============================================================

def initialize_csv():

    if not os.path.exists(TRADES_FILE):

        with open(
            TRADES_FILE,
            "w",
            newline="",
            encoding="utf-8",
        ) as f:

            csv.writer(f).writerow([
                "received_at",
                "mint",
                "name",
                "signature",
                "wallet",
                "is_buy",
                "sol_amount",
                "token_amount",
                "market_cap_sol",
                "trade_timestamp",
            ])

    if not os.path.exists(TOKENS_FILE):

        with open(
            TOKENS_FILE,
            "w",
            newline="",
            encoding="utf-8",
        ) as f:

            csv.writer(f).writerow([
                "mint",
                "name",
                "symbol",
                "creator",
                "age_seconds",
                "trades",
                "unique_wallets",
                "unique_buyers",
                "unique_sellers",
                "buy_sol",
                "sell_sol",
                "buy_pressure",
                "market_cap_sol",
                "peak_market_cap_sol",
                "score",
                "alerted",
                "status",
            ])

    if not os.path.exists(SAMPLES_FILE):

        with open(
            SAMPLES_FILE,
            "w",
            encoding="utf-8",
        ) as f:

            f.write(
                "SOLANA EARLY SIGNAL - "
                "TRADE EVENT DIAGNOSTIC SAMPLES\n"
            )


initialize_csv()


# ============================================================
# 6. TELEGRAM
# ============================================================

async def telegram_send(message):

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(
            "[TELEGRAM ERROR] Secrets unavailable "
            "to this notebook."
        )
        return False

    if http_session is None or http_session.closed:
        print("[TELEGRAM ERROR] HTTP session unavailable.")
        return False

    url = (
        "https://api.telegram.org/bot"
        + TELEGRAM_BOT_TOKEN
        + "/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:

        async with http_session.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as response:

            try:
                result = await response.json(
                    content_type=None
                )
            except Exception:
                result = {
                    "description": (
                        await response.text()
                    )[:200]
                }

            if response.status == 200 and result.get("ok"):
                stats["telegram_sent"] += 1
                return True

            print(
                "[TELEGRAM ERROR]",
                response.status,
                str(result)[:250],
            )

    except Exception as exc:

        print(
            "[TELEGRAM CONNECTION ERROR]",
            type(exc).__name__,
            str(exc)[:200],
        )

    return False


# ============================================================
# 7. HELPERS
# ============================================================

def safe_float(value, default=0.0):

    try:
        result = float(value)

        if (
            result == result
            and abs(result) != float("inf")
        ):
            return result

        return default

    except (TypeError, ValueError):
        return default


def normalize_score(value):
    return max(
        0.0,
        min(1.0, safe_float(value)),
    )


def append_csv(path, row):

    with open(
        path,
        "a",
        newline="",
        encoding="utf-8",
    ) as f:

        csv.writer(f).writerow(row)


def market_cap_from_reserves(
    virtual_sol,
    virtual_token,
):

    if virtual_sol <= 0 or virtual_token <= 0:
        return 0.0

    return (
        virtual_sol / virtual_token
    ) * 1_000_000


# ============================================================
# 8. TOKEN REGISTRATION + PRESCORE DEBUG
# ============================================================

def register_token(data):

    mint = (
        data.get("mint")
        or data.get("tokenMint")
        or data.get("address")
    )

    if not mint or mint in tokens:
        return

    now = time.time()

    name = str(data.get("name") or "Unknown")
    symbol = str(data.get("symbol") or "?")

    creator = (
        data.get("traderPublicKey")
        or data.get("creator")
        or data.get("user")
        or ""
    )

    initial_buy_raw = data.get("initialBuy")
    mcap_sol_raw = data.get("marketCapSol")

    initial_buy = safe_float(initial_buy_raw)
    mcap_sol = safe_float(mcap_sol_raw)

    prescore = 0

    if 0 < initial_buy <= 10:
        prescore += 25

    if 0 < mcap_sol <= 150:
        prescore += 10

    if len(name) >= 3:
        prescore += 10

    print(
        f"[PRESCORE DEBUG] "
        f"name={name!r} | "
        f"initialBuy={initial_buy_raw!r} | "
        f"marketCapSol={mcap_sol_raw!r} | "
        f"prescore={prescore} | "
        f"mint={mint}"
    )

    tokens[mint] = {
        "mint": mint,
        "name": name,
        "symbol": symbol,
        "creator": creator,
        "created_at": now,
        "prescore": prescore,

        "trades": deque(),
        "wallet_counts": defaultdict(int),
        "wallet_net_tokens": defaultdict(float),
        "wallet_buy_sol": defaultdict(float),
        "wallet_sell_sol": defaultdict(float),

        "buy_sol": 0.0,
        "sell_sol": 0.0,
        "buy_count": 0,
        "sell_count": 0,

        "unique_buyers": set(),
        "unique_sellers": set(),
        "unique_wallets": set(),

        "mcap_sol": mcap_sol,
        "peak_mcap_sol": mcap_sol,

        "last_alert": 0,
        "alerted": False,
        "status": "COLLECTING",
    }

    stats["new_tokens"] += 1

    print(
        f"[NEW TOKEN] {name} ({symbol}) "
        f"| pre={prescore} | mint={mint}"
    )


# ============================================================
# 9. PUMPPORTAL TOKEN DISCOVERY
# ============================================================

async def pumpportal_listener():

    while True:

        try:

            print("[PUMPPORTAL] Connecting...")

            async with websockets.connect(
                PUMPPORTAL_WS,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=15,
                max_size=2**22,
            ) as ws:

                await ws.send(json.dumps({
                    "method": "subscribeNewToken"
                }))

                print(
                    "[PUMPPORTAL] "
                    "New-token subscription sent."
                )

                while True:

                    raw = await asyncio.wait_for(
                        ws.recv(),
                        timeout=180,
                    )

                    try:
                        data = json.loads(raw)
                    except Exception:
                        stats["pumpportal_invalid_json"] += 1
                        continue

                    if (
                        isinstance(data, dict)
                        and data.get("mint")
                    ):
                        register_token(data)

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            stats["pumpportal_reconnects"] += 1

            print(
                "[PUMPPORTAL ERROR]",
                type(exc).__name__,
                str(exc)[:150],
            )

            await asyncio.sleep(3)


# ============================================================
# 10. DIAGNOSTIC SAMPLE CAPTURE
# ============================================================

def capture_trade_sample(
    encoded,
    signature,
    source_url,
):

    if stats["sample_events"] >= MAX_SAMPLE_EVENTS:
        return

    try:

        raw = base64.b64decode(
            encoded,
            validate=True,
        )

        sample_number = stats["sample_events"] + 1

        sample_text = (
            f"\n========== SAMPLE {sample_number} ==========\n"
            f"Source: {source_url}\n"
            f"Signature: {signature}\n"
            f"Raw bytes length: {len(raw)}\n"
            f"First 8 bytes HEX: {raw[:8].hex()}\n"
            f"First 16 bytes HEX: {raw[:16].hex()}\n"
            f"Base64 data:\n{encoded}\n"
            f"============================================\n"
        )

        print(sample_text)

        with open(
            SAMPLES_FILE,
            "a",
            encoding="utf-8",
        ) as f:
            f.write(sample_text)

        stats["sample_events"] += 1

        if stats["sample_events"] >= MAX_SAMPLE_EVENTS:

            print(
                "[DIAGNOSTIC] Three samples captured. "
                "Saved to:",
                SAMPLES_FILE,
            )

    except Exception as exc:

        stats["sample_capture_errors"] += 1

        print(
            "[SAMPLE CAPTURE ERROR]",
            repr(exc),
        )


# ============================================================
# 11. TRADE EVENT DECODER
# ============================================================

def decode_trade_event(encoded):

    try:

        raw = base64.b64decode(
            encoded,
            validate=True,
        )

        if len(raw) < 129:
            stats["trade_events_too_short"] += 1
            return None

        if raw[:8] != TRADE_DISCRIMINATOR:
            stats["non_trade_program_data"] += 1
            return None

        mint = base58.b58encode(
            raw[8:40]
        ).decode()

        sol_amount = struct.unpack_from(
            "<Q", raw, 40
        )[0]

        token_amount = struct.unpack_from(
            "<Q", raw, 48
        )[0]

        is_buy = bool(raw[56])

        wallet = base58.b58encode(
            raw[57:89]
        ).decode()

        timestamp = struct.unpack_from(
            "<q", raw, 89
        )[0]

        virtual_sol = struct.unpack_from(
            "<Q", raw, 97
        )[0]

        virtual_token = struct.unpack_from(
            "<Q", raw, 105
        )[0]

        real_sol = struct.unpack_from(
            "<Q", raw, 113
        )[0]

        real_token = struct.unpack_from(
            "<Q", raw, 121
        )[0]

        sol_amount /= LAMPORTS_PER_SOL

        token_amount /= (
            10 ** TOKEN_DECIMALS
        )

        mcap_sol = market_cap_from_reserves(
            virtual_sol,
            virtual_token,
        )

        return {
            "mint": mint,
            "sol_amount": sol_amount,
            "token_amount": token_amount,
            "is_buy": is_buy,
            "wallet": wallet,
            "timestamp": timestamp,
            "mcap_sol": mcap_sol,
            "real_sol": (
                real_sol / LAMPORTS_PER_SOL
            ),
            "real_token": (
                real_token / (10 ** TOKEN_DECIMALS)
            ),
        }

    except Exception as exc:

        stats["trade_decode_errors"] += 1

        if stats["trade_decode_errors"] <= 3:
            print(
                "[TRADE DECODE ERROR]",
                type(exc).__name__,
                str(exc)[:150],
            )

        return None


# ============================================================
# 12. LIVE TRADE PROCESSING
# ============================================================

async def process_trade(event, signature):

    mint = event["mint"]
    info = tokens.get(mint)

    if info is None:
        stats["trades_for_untracked_tokens"] += 1
        return

    now = time.time()
    age = now - info["created_at"]

    if age < 0 or age > WINDOW_SEC:
        stats["trades_outside_window"] += 1
        return

    sol_amount = event["sol_amount"]
    token_amount = event["token_amount"]
    wallet = event["wallet"]
    is_buy = event["is_buy"]

    if sol_amount < MIN_TRANSACTION_SOL:
        stats["small_trades_ignored"] += 1
        return

    trade = {
        "time": now,
        "wallet": wallet,
        "is_buy": is_buy,
        "sol": sol_amount,
        "tokens": token_amount,
        "signature": signature,
    }

    info["trades"].append(trade)

    info["wallet_counts"][wallet] += 1
    info["unique_wallets"].add(wallet)

    if is_buy:

        info["buy_sol"] += sol_amount
        info["buy_count"] += 1

        info["unique_buyers"].add(wallet)
        info["wallet_buy_sol"][wallet] += sol_amount
        info["wallet_net_tokens"][wallet] += token_amount

    else:

        info["sell_sol"] += sol_amount
        info["sell_count"] += 1

        info["unique_sellers"].add(wallet)
        info["wallet_sell_sol"][wallet] += sol_amount
        info["wallet_net_tokens"][wallet] -= token_amount

    if event["mcap_sol"] > 0:

        info["mcap_sol"] = event["mcap_sol"]

        info["peak_mcap_sol"] = max(
            info["peak_mcap_sol"],
            event["mcap_sol"],
        )

    stats["decoded_trades"] += 1

    async with csv_lock:

        append_csv(
            TRADES_FILE,
            [
                now,
                mint,
                info["name"],
                signature,
                wallet,
                "BUY" if is_buy else "SELL",
                round(sol_amount, 8),
                round(token_amount, 4),
                round(event["mcap_sol"], 4),
                event["timestamp"],
            ],
        )

    await evaluate_token(info)


# ============================================================
# 13. SCORE
# ============================================================

def calculate_score(info):

    trades = list(info["trades"])

    if not trades:

        return 0.0, {
            "trades": 0,
            "wallets": 0,
            "buyers": 0,
            "sellers": 0,
            "buy_sol": 0.0,
            "sell_sol": 0.0,
            "buy_pressure": 0.0,
            "unique_ratio": 0.0,
            "bundle_ratio": 0.0,
            "top3_share": 0.0,
            "dev_holding": 0.0,
            "dev_sold": 0.0,
            "velocity": 0.0,
        }

    wallet_counts = info["wallet_counts"]
    wallets = set(info["unique_wallets"])

    eligible_wallets = {
        wallet
        for wallet in wallets
        if wallet_counts[wallet] <= MAX_WALLET_TRANSACTIONS
    }

    eligible_buys = [
        trade for trade in trades
        if trade["is_buy"]
        and wallet_counts[trade["wallet"]]
        <= MAX_WALLET_TRANSACTIONS
    ]

    eligible_sells = [
        trade for trade in trades
        if not trade["is_buy"]
        and wallet_counts[trade["wallet"]]
        <= MAX_WALLET_TRANSACTIONS
    ]

    unique_count = len(eligible_wallets)

    total_trades = (
        len(eligible_buys)
        + len(eligible_sells)
    )

    buy_sol = sum(
        trade["sol"] for trade in eligible_buys
    )

    sell_sol = sum(
        trade["sol"] for trade in eligible_sells
    )

    total_sol = buy_sol + sell_sol

    buy_pressure = (
        buy_sol / total_sol
        if total_sol > 0 else 0.0
    )

    unique_ratio = (
        unique_count / total_trades
        if total_trades > 0 else 0.0
    )

    elapsed = max(
        1.0,
        time.time() - info["created_at"],
    )

    velocity = min(
        1.0,
        total_trades / max(1.0, elapsed * 2),
    )

    unique_wallet_score = min(
        1.0,
        unique_count / 50.0,
    )

    buy_times = sorted(
        trade["time"] for trade in eligible_buys
    )

    max_cluster = 0
    left = 0

    for right in range(len(buy_times)):

        while (
            left <= right
            and buy_times[right] - buy_times[left]
            > BUNDLE_WINDOW_SEC
        ):
            left += 1

        max_cluster = max(
            max_cluster,
            right - left + 1,
        )

    bundle_ratio = (
        max_cluster / len(eligible_buys)
        if eligible_buys else 0.0
    )

    positive_balances = sorted(
        [
            max(
                0.0,
                info["wallet_net_tokens"].get(
                    wallet, 0.0
                ),
            )
            for wallet in eligible_wallets
        ],
        reverse=True,
    )

    total_positive = sum(positive_balances)

    top3_share = (
        sum(positive_balances[:3]) / total_positive
        if total_positive > 0 else 0.0
    )

    creator = info["creator"]

    creator_net = max(
        0.0,
        info["wallet_net_tokens"].get(
            creator, 0.0
        ),
    )

    dev_holding = (
        creator_net / (
            TOTAL_SUPPLY / (10 ** TOKEN_DECIMALS)
        )
        if creator else 0.0
    )

    creator_buy = info["wallet_buy_sol"].get(
        creator, 0.0
    )

    creator_sell = info["wallet_sell_sol"].get(
        creator, 0.0
    )

    dev_sold = (
        creator_sell / (creator_buy + creator_sell)
        if creator_buy + creator_sell > 0 else 0.0
    )

    positive_score = (
        WEIGHTS["unique_wallets"]
        * unique_wallet_score

        + WEIGHTS["velocity"]
        * velocity

        + WEIGHTS["buy_pressure"]
        * buy_pressure

        + WEIGHTS["unique_ratio"]
        * normalize_score(unique_ratio)
    )

    penalties = (
        abs(WEIGHTS["bundle"])
        * normalize_score(bundle_ratio)

        + abs(WEIGHTS["dev_holding"])
        * normalize_score(dev_holding / 0.20)

        + abs(WEIGHTS["dev_sold"])
        * normalize_score(dev_sold)

        + abs(WEIGHTS["concentration"])
        * normalize_score(top3_share / 0.65)
    )

    score = max(
        0.0,
        min(100.0, (positive_score - penalties) * 100),
    )

    metrics = {
        "trades": total_trades,
        "wallets": unique_count,

        "buyers": len({
            trade["wallet"]
            for trade in eligible_buys
        }),

        "sellers": len({
            trade["wallet"]
            for trade in eligible_sells
        }),

        "buy_sol": buy_sol,
        "sell_sol": sell_sol,
        "buy_pressure": buy_pressure,
        "unique_ratio": unique_ratio,
        "bundle_ratio": bundle_ratio,
        "top3_share": top3_share,
        "dev_holding": dev_holding,
        "dev_sold": dev_sold,
        "velocity": velocity,
    }

    return score, metrics


# ============================================================
# 14. EVALUATION AND ALERTS
# ============================================================

async def evaluate_token(info):

    now = time.time()
    age = now - info["created_at"]

    score, metrics = calculate_score(info)

    info["last_score"] = score
    info["last_metrics"] = metrics

    if now - info.get("last_summary", 0) >= 30:

        info["last_summary"] = now

        async with csv_lock:

            append_csv(
                TOKENS_FILE,
                [
                    info["mint"],
                    info["name"],
                    info["symbol"],
                    info["creator"],
                    round(age, 2),
                    metrics["trades"],
                    metrics["wallets"],
                    metrics["buyers"],
                    metrics["sellers"],
                    round(metrics["buy_sol"], 4),
                    round(metrics["sell_sol"], 4),
                    round(metrics["buy_pressure"], 4),
                    round(info["mcap_sol"], 4),
                    round(info["peak_mcap_sol"], 4),
                    round(score, 2),
                    info["alerted"],
                    info["status"],
                ],
            )

    if info["alerted"]:
        return

    if age > WINDOW_SEC:
        info["status"] = "WINDOW_CLOSED"
        return

    if info["prescore"] < MIN_PRESCORE:
        info["status"] = "PRESCORE_TOO_LOW"
        return

    if metrics["wallets"] < MIN_UNIQUE_WALLETS:
        info["status"] = "WAITING_FOR_WALLETS"
        return

    if info["mcap_sol"] < MIN_MCAP_SOL:
        info["status"] = "MCAP_TOO_LOW"
        return

    if score < MIN_SCORE_ALERT:
        info["status"] = "SCORE_TOO_LOW"
        return

    if now - info["last_alert"] < ALERT_COOLDOWN_SEC:
        return

    async with alert_lock:

        if info["alerted"]:
            return

        name = html.escape(str(info["name"]))
        symbol = html.escape(str(info["symbol"]))
        mint = html.escape(str(info["mint"]))

        message = (
            "🚨 <b>SOLANA EARLY SIGNAL</b>\n\n"
            f"<b>Name:</b> {name}\n"
            f"<b>Symbol:</b> {symbol}\n"
            f"<b>Mint:</b> <code>{mint}</code>\n"
            f"<b>Age:</b> {age:.1f}s\n"
            f"<b>Score:</b> {score:.1f}/100\n"
            f"<b>Trades:</b> {metrics['trades']}\n"
            f"<b>Eligible wallets:</b> {metrics['wallets']}\n"
            f"<b>Buyers:</b> {metrics['buyers']}\n"
            f"<b>Sellers:</b> {metrics['sellers']}\n"
            f"<b>Buy SOL:</b> {metrics['buy_sol']:.3f}\n"
            f"<b>Sell SOL:</b> {metrics['sell_sol']:.3f}\n"
            f"<b>Buy pressure:</b> "
            f"{metrics['buy_pressure']:.1%}\n"
            f"<b>Current MCAP:</b> "
            f"{info['mcap_sol']:.2f} SOL\n"
            f"<b>Peak MCAP:</b> "
            f"{info['peak_mcap_sol']:.2f} SOL\n"
            f"<b>Top-3 share:</b> "
            f"{metrics['top3_share']:.1%}\n\n"
            f'<a href="https://pump.fun/coin/'
            f'{info["mint"]}">Pump.fun</a>'
        )

        sent = await telegram_send(message)

        if sent:

            info["alerted"] = True
            info["last_alert"] = now
            info["status"] = "ALERT_SENT"

            stats["qualified_alerts"] += 1

            print(
                "[ALERT SENT]",
                info["mint"],
                score,
            )


# ============================================================
# 15. WEBSOCKET LOG LISTENER
# ============================================================

async def rpc_websocket_source(url):

    while True:

        try:

            print("[RPC WS] Connecting:", url)

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=15,
                max_size=2**22,
            ) as ws:

                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [PUMP_PROGRAM]},
                        {"commitment": "processed"},
                    ],
                }))

                raw_response = await asyncio.wait_for(
                    ws.recv(),
                    timeout=20,
                )

                response = json.loads(raw_response)

                if response.get("error"):

                    stats["ws_subscription_errors"] += 1

                    print(
                        "[WS SUBSCRIPTION ERROR]",
                        url,
                        response["error"],
                    )

                    await asyncio.sleep(5)
                    continue

                print(
                    "[RPC WS CONNECTED]",
                    url,
                    "| subscription:",
                    response.get("result"),
                )

                while True:

                    raw = await asyncio.wait_for(
                        ws.recv(),
                        timeout=120,
                    )

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        stats["invalid_rpc_json"] += 1
                        continue

                    if msg.get("method") != "logsNotification":
                        continue

                    result = (
                        msg.get("params", {})
                        .get("result", {})
                    )

                    value = result.get("value", {})
                    signature = value.get("signature")

                    if not signature or value.get("err"):
                        continue

                    stats["signatures_received"] += 1

                    for line in value.get("logs") or []:

                        if (
                            not isinstance(line, str)
                            or not line.startswith("Program data: ")
                        ):
                            continue

                        encoded = line.split(
                            "Program data: ", 1
                        )[1].strip()

                        stats["program_data_logs"] += 1

                        capture_trade_sample(
                            encoded,
                            signature,
                            url,
                        )

                        try:
                            raw_event = base64.b64decode(
                                encoded,
                                validate=True,
                            )
                        except Exception:
                            stats["invalid_base64_events"] += 1
                            continue

                        if (
                            len(raw_event) < 8
                            or raw_event[:8] != TRADE_DISCRIMINATOR
                        ):
                            stats["non_trade_program_data"] += 1
                            continue

                        event_key = (
                            signature,
                            hashlib.sha256(
                                raw_event
                            ).hexdigest(),
                        )

                        if event_key in seen_trade_events:
                            stats["duplicate_trade_events"] += 1
                            continue

                        seen_trade_events.add(event_key)

                        if len(seen_trade_events) > MAX_SEEN_TRADE_EVENTS:
                            seen_trade_events.clear()

                        event = decode_trade_event(encoded)

                        if event is not None:
                            await process_trade(
                                event,
                                signature,
                            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            stats["ws_source_errors"] += 1

            print(
                "[RPC WS ERROR]",
                url,
                type(exc).__name__,
                str(exc)[:120],
            )

            await asyncio.sleep(5)


# ============================================================
# 16. STATUS MONITOR
# ============================================================

async def monitor():

    while True:

        now = time.time()

        for info in list(tokens.values()):

            age = now - info["created_at"]

            if age > WINDOW_SEC and not info["alerted"]:
                info["status"] = "WINDOW_CLOSED"

        print("\n" + "=" * 55)
        print("SOLANA EARLY SIGNAL — STATUS")
        print("=" * 55)

        print("Tracked tokens:", len(tokens))
        print("Signatures received:", stats["signatures_received"])
        print("Program data logs:", stats["program_data_logs"])
        print("Captured samples:", stats["sample_events"])
        print("Decoded trades:", stats["decoded_trades"])
        print("Decode errors:", stats["trade_decode_errors"])
        print("Events too short:", stats["trade_events_too_short"])
        print("Non-trade program data:", stats["non_trade_program_data"])
        print("Invalid Base64 events:", stats["invalid_base64_events"])
        print("Duplicate events ignored:", stats["duplicate_trade_events"])
        print("Untracked-token trades:", stats["trades_for_untracked_tokens"])
        print("Small trades ignored:", stats["small_trades_ignored"])
        print("Trades outside window:", stats["trades_outside_window"])
        print("Qualified alerts:", stats["qualified_alerts"])
        print("Telegram sent:", stats["telegram_sent"])
        print("WS errors:", stats["ws_source_errors"])
        print("WS subscription errors:", stats["ws_subscription_errors"])
        print("PumpPortal reconnects:", stats["pumpportal_reconnects"])

        statuses = defaultdict(int)

        for info in tokens.values():
            statuses[info["status"]] += 1

        print("Token statuses:", dict(statuses))
        print("=" * 55)

        if stats["sample_events"] >= MAX_SAMPLE_EVENTS:
            print("SAMPLE FILE:", SAMPLES_FILE)

        await asyncio.sleep(30)


# ============================================================
# 17. MAIN
# ============================================================

async def main():

    global http_session

    http_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20),
        connector=aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300,
        ),
    )

    print("=" * 55)
    print("SOLANA EARLY SIGNAL — LIVE TRADE COLLECTOR")
    print("=" * 55)

    print(
        "Telegram configured:",
        bool(
            TELEGRAM_BOT_TOKEN
            and TELEGRAM_CHAT_ID
        ),
    )

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:

        print(
            "Telegram token secret name:",
            TOKEN_SECRET_NAME,
        )

        print(
            "Telegram chat secret name:",
            CHAT_SECRET_NAME,
        )

    else:

        print(
            "Telegram is NOT configured in this runtime."
        )

        print(
            "Check Colab Secrets names: "
            "TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
        )

        print(
            "Notebook must have access enabled "
            "for both secrets."
        )

    print("PumpPortal:", PUMPPORTAL_WS)
    print("WebSocket sources:", len(RPC_WS_URLS))
    print("Analysis window:", WINDOW_SEC, "seconds")
    print("Alert threshold:", MIN_SCORE_ALERT)
    print("Minimum prescore:", MIN_PRESCORE)
    print("Minimum unique wallets:", MIN_UNIQUE_WALLETS)
    print("Trade discriminator:", TRADE_DISCRIMINATOR.hex())
    print("Trades CSV:", TRADES_FILE)
    print("Summary CSV:", TOKENS_FILE)
    print("Sample file:", SAMPLES_FILE)

    print("=" * 55)

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:

        ok = await telegram_send(
            "✅ <b>SOLANA EARLY SIGNAL</b>\n"
            "Collector started.\n"
            "Trade event diagnostic enabled."
        )

        print(
            "Telegram startup test:",
            "SUCCESS" if ok
            else "FAILED — see Telegram error above",
        )

    tasks = [
        asyncio.create_task(
            pumpportal_listener()
        ),

        *[
            asyncio.create_task(
                rpc_websocket_source(url)
            )
            for url in RPC_WS_URLS
        ],

        asyncio.create_task(
            monitor()
        ),
    ]

    try:

        await asyncio.gather(*tasks)

    except asyncio.CancelledError:

        for task in tasks:
            task.cancel()

        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    finally:

        if http_session and not http_session.closed:
            await http_session.close()

        print("[STOP] Collector closed.")


await main()
# ============================================================
# 10. DIAGNOSTIC SAMPLE CAPTURE
# ============================================================

def capture_trade_sample(encoded, signature, source_url):
    if stats["sample_events"] >= MAX_SAMPLE_EVENTS:
        return
    try:
        raw = base64.b64decode(encoded, validate=True)
        sample_number = stats["sample_events"] + 1
        sample_text = (
            f"\n========== SAMPLE {sample_number} ==========\n"
            f"Source: {source_url}\nSignature: {signature}\n"
            f"Raw bytes length: {len(raw)}\n"
            f"First 8 bytes HEX: {raw[:8].hex()}\n"
            f"First 16 bytes HEX: {raw[:16].hex()}\n"
            f"Base64 data:\n{encoded}\n"
            f"============================================\n"
        )
        print(sample_text)
        with open(SAMPLES_FILE, "a", encoding="utf-8") as f:
            f.write(sample_text)
        stats["sample_events"] += 1
        if stats["sample_events"] >= MAX_SAMPLE_EVENTS:
            print("[DIAGNOSTIC] Three samples captured. Saved to:", SAMPLES_FILE)
    except Exception as exc:
        stats["sample_capture_errors"] += 1
        print("[SAMPLE CAPTURE ERROR]", repr(exc))


# ============================================================
# 11. TRADE EVENT DECODER
# ============================================================

def decode_trade_event(encoded):
    try:
        raw = base64.b64decode(encoded, validate=True)
        if len(raw) < 129:
            stats["trade_events_too_short"] += 1
            return None
        if raw[:8] != TRADE_DISCRIMINATOR:
            stats["non_trade_program_data"] += 1
            return None

        mint = base58.b58encode(raw[8:40]).decode()
        sol_amount = struct.unpack_from("<Q", raw, 40)[0]
        token_amount = struct.unpack_from("<Q", raw, 48)[0]
        is_buy = bool(raw[56])
        wallet = base58.b58encode(raw[57:89]).decode()
        timestamp = struct.unpack_from("<q", raw, 89)[0]
        virtual_sol = struct.unpack_from("<Q", raw, 97)[0]
        virtual_token = struct.unpack_from("<Q", raw, 105)[0]
        real_sol = struct.unpack_from("<Q", raw, 113)[0]
        real_token = struct.unpack_from("<Q", raw, 121)[0]

        sol_amount /= LAMPORTS_PER_SOL
        token_amount /= 10 ** TOKEN_DECIMALS
        mcap_sol = market_cap_from_reserves(virtual_sol, virtual_token)

        return {
            "mint": mint,
            "sol_amount": sol_amount,
            "token_amount": token_amount,
            "is_buy": is_buy,
            "wallet": wallet,
            "timestamp": timestamp,
            "mcap_sol": mcap_sol,
            "real_sol": real_sol / LAMPORTS_PER_SOL,
            "real_token": real_token / (10 ** TOKEN_DECIMALS),
        }
    except Exception as exc:
        stats["trade_decode_errors"] += 1
        if stats["trade_decode_errors"] <= 3:
            print("[TRADE DECODE ERROR]", type(exc).__name__, str(exc)[:150])
        return None


# ============================================================
# 12. LIVE TRADE PROCESSING
# ============================================================

async def process_trade(event, signature):
    mint = event["mint"]
    info = tokens.get(mint)
    if info is None:
        stats["trades_for_untracked_tokens"] += 1
        return

    now = time.time()
    age = now - info["created_at"]
    if age < 0 or age > WINDOW_SEC:
        stats["trades_outside_window"] += 1
        return

    sol_amount = event["sol_amount"]
    token_amount = event["token_amount"]
    wallet = event["wallet"]
    is_buy = event["is_buy"]

    if sol_amount < MIN_TRANSACTION_SOL:
        stats["small_trades_ignored"] += 1
        return

    trade = {
        "time": now,
        "wallet": wallet,
        "is_buy": is_buy,
        "sol": sol_amount,
        "tokens": token_amount,
        "signature": signature,
    }
    info["trades"].append(trade)
    info["wallet_counts"][wallet] += 1
    info["unique_wallets"].add(wallet)

    if is_buy:
        info["buy_sol"] += sol_amount
        info["buy_count"] += 1
        info["unique_buyers"].add(wallet)
        info["wallet_buy_sol"][wallet] += sol_amount
        info["wallet_net_tokens"][wallet] += token_amount
    else:
        info["sell_sol"] += sol_amount
        info["sell_count"] += 1
        info["unique_sellers"].add(wallet)
        info["wallet_sell_sol"][wallet] += sol_amount
        info["wallet_net_tokens"][wallet] -= token_amount

    if event["mcap_sol"] > 0:
        info["mcap_sol"] = event["mcap_sol"]
        info["peak_mcap_sol"] = max(
            info["peak_mcap_sol"], event["mcap_sol"]
        )

    stats["decoded_trades"] += 1

    async with csv_lock:
        append_csv(TRADES_FILE, [
            now,
            mint,
            info["name"],
            signature,
            wallet,
            "BUY" if is_buy else "SELL",
            round(sol_amount, 8),
            round(token_amount, 4),
            round(event["mcap_sol"], 4),
            event["timestamp"],
        ])

    await evaluate_token(info)


# ============================================================
# 13. SCORE
# ============================================================

def calculate_score(info):
    trades = list(info["trades"])
    if not trades:
        return 0.0, {
            "trades": 0,
            "wallets": 0,
            "buyers": 0,
            "sellers": 0,
            "buy_sol": 0.0,
            "sell_sol": 0.0,
            "buy_pressure": 0.0,
            "unique_ratio": 0.0,
            "bundle_ratio": 0.0,
            "top3_share": 0.0,
            "dev_holding": 0.0,
            "dev_sold": 0.0,
            "velocity": 0.0,
        }

    wallet_counts = info["wallet_counts"]
    wallets = set(info["unique_wallets"])

    eligible_wallets = {
        w for w in wallets
        if wallet_counts[w] <= MAX_WALLET_TRANSACTIONS
    }

    eligible_buys = [
        t for t in trades
        if t["is_buy"]
        and wallet_counts[t["wallet"]] <= MAX_WALLET_TRANSACTIONS
    ]

    eligible_sells = [
        t for t in trades
        if not t["is_buy"]
        and wallet_counts[t["wallet"]] <= MAX_WALLET_TRANSACTIONS
    ]

    unique_count = len(eligible_wallets)
    total_trades = len(eligible_buys) + len(eligible_sells)
    buy_sol = sum(t["sol"] for t in eligible_buys)
    sell_sol = sum(t["sol"] for t in eligible_sells)
    total_sol = buy_sol + sell_sol

    buy_pressure = buy_sol / total_sol if total_sol > 0 else 0.0
    unique_ratio = (
        unique_count / total_trades if total_trades > 0 else 0.0
    )

    elapsed = max(1.0, time.time() - info["created_at"])
    velocity = min(1.0, total_trades / max(1.0, elapsed * 2))
    unique_wallet_score = min(1.0, unique_count / 50.0)

    buy_times = sorted(t["time"] for t in eligible_buys)
    max_cluster = 0
    left = 0

    for right in range(len(buy_times)):
        while (
            left <= right
            and buy_times[right] - buy_times[left] > BUNDLE_WINDOW_SEC
        ):
            left += 1
        max_cluster = max(max_cluster, right - left + 1)

    bundle_ratio = (
        max_cluster / len(eligible_buys) if eligible_buys else 0.0
    )

    positive_balances = sorted(
        [
            max(0.0, info["wallet_net_tokens"].get(w, 0.0))
            for w in eligible_wallets
        ],
        reverse=True,
    )

    total_positive = sum(positive_balances)
    top3_share = (
        sum(positive_balances[:3]) / total_positive
        if total_positive > 0 else 0.0
    )

    creator = info["creator"]
    creator_net = max(
        0.0, info["wallet_net_tokens"].get(creator, 0.0)
    )

    dev_holding = (
        creator_net / (TOTAL_SUPPLY / (10 ** TOKEN_DECIMALS))
        if creator else 0.0
    )

    creator_buy = info["wallet_buy_sol"].get(creator, 0.0)
    creator_sell = info["wallet_sell_sol"].get(creator, 0.0)

    dev_sold = (
        creator_sell / (creator_buy + creator_sell)
        if creator_buy + creator_sell > 0 else 0.0
    )

    positive_score = (
        WEIGHTS["unique_wallets"] * unique_wallet_score
        + WEIGHTS["velocity"] * velocity
        + WEIGHTS["buy_pressure"] * buy_pressure
        + WEIGHTS["unique_ratio"] * normalize_score(unique_ratio)
    )

    penalties = (
        abs(WEIGHTS["bundle"]) * normalize_score(bundle_ratio)
        + abs(WEIGHTS["dev_holding"]) * normalize_score(dev_holding / 0.20)
        + abs(WEIGHTS["dev_sold"]) * normalize_score(dev_sold)
        + abs(WEIGHTS["concentration"]) * normalize_score(top3_share / 0.65)
    )

    score = max(0.0, min(100.0, (positive_score - penalties) * 100))

    metrics = {
        "trades": total_trades,
        "wallets": unique_count,
        "buyers": len({t["wallet"] for t in eligible_buys}),
        "sellers": len({t["wallet"] for t in eligible_sells}),
        "buy_sol": buy_sol,
        "sell_sol": sell_sol,
        "buy_pressure": buy_pressure,
        "unique_ratio": unique_ratio,
        "bundle_ratio": bundle_ratio,
        "top3_share": top3_share,
        "dev_holding": dev_holding,
        "dev_sold": dev_sold,
        "velocity": velocity,
    }

    return score, metrics


# ============================================================
# 14. EVALUATION AND ALERTS
# ============================================================

async def evaluate_token(info):
    now = time.time()
    age = now - info["created_at"]
    score, metrics = calculate_score(info)

    info["last_score"] = score
    info["last_metrics"] = metrics

    if now - info.get("last_summary", 0) >= 30:
        info["last_summary"] = now
        async with csv_lock:
            append_csv(TOKENS_FILE, [
                info["mint"],
                info["name"],
                info["symbol"],
                info["creator"],
                round(age, 2),
                metrics["trades"],
                metrics["wallets"],
                metrics["buyers"],
                metrics["sellers"],
                round(metrics["buy_sol"], 4),
                round(metrics["sell_sol"], 4),
                round(metrics["buy_pressure"], 4),
                round(info["mcap_sol"], 4),
                round(info["peak_mcap_sol"], 4),
                round(score, 2),
                info["alerted"],
                info["status"],
            ])

    if info["alerted"]:
        return

    if age > WINDOW_SEC:
        info["status"] = "WINDOW_CLOSED"
        return

    if info["prescore"] < MIN_PRESCORE:
        info["status"] = "PRESCORE_TOO_LOW"
        return

    if metrics["wallets"] < MIN_UNIQUE_WALLETS:
        info["status"] = "WAITING_FOR_WALLETS"
        return

    if info["mcap_sol"] < MIN_MCAP_SOL:
        info["status"] = "MCAP_TOO_LOW"
        return
        if score < MIN_SCORE_ALERT:
        info["status"] = "SCORE_TOO_LOW"
        return

    if now - info["last_alert"] < ALERT_COOLDOWN_SEC:
        return

    async with alert_lock:
        if info["alerted"]:
            return

        name = html.escape(str(info["name"]))
        symbol = html.escape(str(info["symbol"]))
        mint = html.escape(str(info["mint"]))

        message = (
            "🚨 <b>SOLANA EARLY SIGNAL</b>\n\n"
            f"<b>Name:</b> {name}\n"
            f"<b>Symbol:</b> {symbol}\n"
            f"<b>Mint:</b> <code>{mint}</code>\n"
            f"<b>Age:</b> {age:.1f}s\n"
            f"<b>Score:</b> {score:.1f}/100\n"
            f"<b>Trades:</b> {metrics['trades']}\n"
            f"<b>Eligible wallets:</b> {metrics['wallets']}\n"
            f"<b>Buyers:</b> {metrics['buyers']}\n"
            f"<b>Sellers:</b> {metrics['sellers']}\n"
            f"<b>Buy SOL:</b> {metrics['buy_sol']:.3f}\n"
            f"<b>Sell SOL:</b> {metrics['sell_sol']:.3f}\n"
            f"<b>Buy pressure:</b> {metrics['buy_pressure']:.1%}\n"
            f"<b>Current MCAP:</b> {info['mcap_sol']:.2f} SOL\n"
            f"<b>Peak MCAP:</b> {info['peak_mcap_sol']:.2f} SOL\n"
            f"<b>Top-3 share:</b> {metrics['top3_share']:.1%}\n\n"
            f'<a href="https://pump.fun/coin/{info["mint"]}">Pump.fun</a>'
        )

        sent = await telegram_send(message)

        if sent:
            info["alerted"] = True
            info["last_alert"] = now
            info["status"] = "ALERT_SENT"
            stats["qualified_alerts"] += 1
            print("[ALERT SENT]", info["mint"], score)


# ============================================================
# 15. WEBSOCKET LOG LISTENER
# ============================================================

async def rpc_websocket_source(url):
    while True:
        try:
            print("[RPC WS] Connecting:", url)

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=15,
                max_size=2**22
            ) as ws:

                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [PUMP_PROGRAM]},
                        {"commitment": "processed"}
                    ],
                }))

                raw_response = await asyncio.wait_for(
                    ws.recv(), timeout=20
                )
                response = json.loads(raw_response)

                if response.get("error"):
                    stats["ws_subscription_errors"] += 1
                    print(
                        "[WS SUBSCRIPTION ERROR]",
                        url,
                        response["error"]
                    )
                    await asyncio.sleep(5)
                    continue

                print(
                    "[RPC WS CONNECTED]",
                    url,
                    "| subscription:",
                    response.get("result")
                )

                while True:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=120
                    )

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        stats["invalid_rpc_json"] += 1
                        continue

                    if msg.get("method") != "logsNotification":
                        continue

                    result = msg.get("params", {}).get("result", {})
                    value = result.get("value", {})
                    signature = value.get("signature")

                    if not signature or value.get("err"):
                        continue

                    stats["signatures_received"] += 1

                    for line in value.get("logs") or []:
                        if (
                            not isinstance(line, str)
                            or not line.startswith("Program data: ")
                        ):
                            continue

                        encoded = line.split(
                            "Program data: ", 1
                        )[1].strip()

                        stats["program_data_logs"] += 1

                        capture_trade_sample(
                            encoded, signature, url
                        )

                        try:
                            raw_event = base64.b64decode(
                                encoded, validate=True
                            )
                        except Exception:
                            stats["invalid_base64_events"] += 1
                            continue

                        if (
                            len(raw_event) < 8
                            or raw_event[:8] != TRADE_DISCRIMINATOR
                        ):
                            stats["non_trade_program_data"] += 1
                            continue

                        event_key = (
                            signature,
                            hashlib.sha256(raw_event).hexdigest()
                        )

                        if event_key in seen_trade_events:
                            stats["duplicate_trade_events"] += 1
                            continue

                        seen_trade_events.add(event_key)

                        if len(seen_trade_events) > MAX_SEEN_TRADE_EVENTS:
                            seen_trade_events.clear()

                        event = decode_trade_event(encoded)

                        if event is not None:
                            await process_trade(event, signature)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            stats["ws_source_errors"] += 1
            print(
                "[RPC WS ERROR]",
                url,
                type(exc).__name__,
                str(exc)[:120]
            )
            await asyncio.sleep(5)


# ============================================================
# 16. STATUS MONITOR
# ============================================================

async def monitor():
    while True:
        now = time.time()

        for info in list(tokens.values()):
            age = now - info["created_at"]

            if age > WINDOW_SEC and not info["alerted"]:
                info["status"] = "WINDOW_CLOSED"

        print("\n" + "=" * 55)
        print("SOLANA EARLY SIGNAL — STATUS")
        print("=" * 55)
        print("Tracked tokens:", len(tokens))
        print("Signatures received:", stats["signatures_received"])
        print("Program data logs:", stats["program_data_logs"])
        print("Captured samples:", stats["sample_events"])
        print("Decoded trades:", stats["decoded_trades"])
        print("Decode errors:", stats["trade_decode_errors"])
        print("Events too short:", stats["trade_events_too_short"])
        print("Non-trade program data:", stats["non_trade_program_data"])
        print("Invalid Base64 events:", stats["invalid_base64_events"])
        print("Duplicate events ignored:", stats["duplicate_trade_events"])
        print("Untracked-token trades:", stats["trades_for_untracked_tokens"])
        print("Small trades ignored:", stats["small_trades_ignored"])
        print("Trades outside window:", stats["trades_outside_window"])
        print("Qualified alerts:", stats["qualified_alerts"])
        print("Telegram sent:", stats["telegram_sent"])
        print("WS errors:", stats["ws_source_errors"])
        print("WS subscription errors:", stats["ws_subscription_errors"])
        print("PumpPortal reconnects:", stats["pumpportal_reconnects"])

        statuses = defaultdict(int)

        for info in tokens.values():
            statuses[info["status"]] += 1

        print("Token statuses:", dict(statuses))
        print("=" * 55)

        if stats["sample_events"] >= MAX_SAMPLE_EVENTS:
            print("SAMPLE FILE:", SAMPLES_FILE)

        await asyncio.sleep(30)


# ============================================================
# 17. RAILWAY HEALTH ENDPOINT
# ============================================================

async def health_handler(request):
    return web.json_response({
        "status": "ok",
        "service": "solana-early-signal",
        "tracked_tokens": len(tokens),
        "decoded_trades": stats["decoded_trades"],
        "telegram_sent": stats["telegram_sent"],
    })


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)

    await site.start()

    print(
        f"[HEALTH] HTTP endpoint listening on 0.0.0.0:{port}"
    )

    return runner


# ============================================================
# 18. MAIN
# ============================================================

async def main():
    global http_session

    health_runner = await start_health_server()

    http_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20),
        connector=aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300
        ),
    )

    print("=" * 55)
    print("SOLANA EARLY SIGNAL — LIVE TRADE COLLECTOR")
    print("=" * 55)
    print(
        "Telegram configured:",
        bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    )

    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print(
            "Telegram is not configured. Add TELEGRAM_BOT_TOKEN "
            "and TELEGRAM_CHAT_ID in Railway Variables."
        )

    print("PumpPortal:", PUMPPORTAL_WS)
    print("WebSocket sources:", len(RPC_WS_URLS))
    print("Analysis window:", WINDOW_SEC, "seconds")
    print("Alert threshold:", MIN_SCORE_ALERT)
    print("Minimum prescore:", MIN_PRESCORE)
    print("Minimum unique wallets:", MIN_UNIQUE_WALLETS)
    print("Trade discriminator:", TRADE_DISCRIMINATOR.hex())
    print("Trades CSV:", TRADES_FILE)
    print("Summary CSV:", TOKENS_FILE)
    print("Sample file:", SAMPLES_FILE)
    print("=" * 55)

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        ok = await telegram_send(
            "✅ <b>SOLANA EARLY SIGNAL</b>\n"
            "Collector started.\n"
            "Trade event diagnostic enabled."
        )

        print(
            "Telegram startup test:",
            "SUCCESS" if ok else "FAILED — see Telegram error above"
        )

    tasks = [
        asyncio.create_task(pumpportal_listener()),
        *[
            asyncio.create_task(rpc_websocket_source(url))
            for url in RPC_WS_URLS
        ],
        asyncio.create_task(monitor()),
    ]

    try:
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()

        await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

    finally:
        if http_session and not http_session.closed:
            await http_session.close()

        await health_runner.cleanup()

        print("[STOP] Collector closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[STOP] Keyboard interrupt received.")
