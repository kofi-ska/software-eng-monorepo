import asyncio
import ccxt
import pandas as pd
import numpy as np
import websockets
import json
import datetime as dt
import uuid
from pathlib import Path
import logging


# =========================================================
# LOGGING CONFIG
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# =========================================================
# CONFIG
# =========================================================
SYMBOL = "HEMIUSDT"
TF = "1m"
TF_HOURS = 0.5
TRADE_LOG = Path("hawkes_trades.csv")

BASE_KAPPA = 0.1
BASE_NORM_LOOKBACK = 336
BASE_LOOKBACK = 168


# =========================================================
# PARAM SCALER
# =========================================================
def adjust_params_for_timeframe(tf_hours,
                                base_tf_hours=1,
                                base_kappa=0.1,
                                base_norm_lookback=336,
                                base_lookback=168):
    ratio = tf_hours / base_tf_hours
    kappa = base_kappa * ratio
    norm_lookback = int(base_norm_lookback / ratio)
    lookback = int(base_lookback / ratio)
    return kappa, norm_lookback, lookback


kappa, norm_lookback, lookback = adjust_params_for_timeframe(TF_HOURS)


# =========================================================
# CUSTOM ATR (replacing pandas_ta)
# =========================================================
def wilder_ewm(series, length):
    alpha = 1 / length
    return series.ewm(alpha=alpha, adjust=False).mean()


def atr(high, low, close, length):
    prev_close = close.shift(1)

    tr = pd.Series(
        np.maximum.reduce([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ]),
        index=high.index
    )

    return wilder_ewm(tr, length)



# =========================================================
# HAWKES PROCESS + SIGNAL
# =========================================================
def hawkes_process(data: pd.Series, kappa: float):
    alpha = np.exp(-kappa)
    arr = data.to_numpy()
    output = np.zeros(len(data))
    output[:] = np.nan
    for i in range(1, len(data)):
        if np.isnan(output[i - 1]):
            output[i] = arr[i]
        else:
            output[i] = output[i - 1] * alpha + arr[i]
    return pd.Series(output, index=data.index) * kappa


def vol_signal(close: pd.Series, vol_hawkes: pd.Series, lookback: int):
    signal = np.zeros(len(close))
    q05 = vol_hawkes.rolling(lookback).quantile(0.05)
    q95 = vol_hawkes.rolling(lookback).quantile(0.95)
    last_below = -1
    curr_sig = 0
    for i in range(len(signal)):
        if vol_hawkes.iloc[i] < q05.iloc[i]:
            last_below = i
            curr_sig = 0
        if vol_hawkes.iloc[i] > q95.iloc[i] \
           and vol_hawkes.iloc[i - 1] <= q95.iloc[i - 1] \
           and last_below > 0:
            change = close.iloc[i] - close.iloc[last_below]
            curr_sig = 1 if change > 0 else -1
        signal[i] = curr_sig
    return signal


def evaluate_hawkes_trade(symbol: str, df: pd.DataFrame) -> str | None:
    sig = int(df["sig"].iloc[-1])
    prev_sig = int(df["sig"].iloc[-2]) if len(df) > 1 else 0
    close_price = df["close"].iloc[-1]

    if sig != prev_sig:
        v = df["v_hawk"].iloc[-1]
        q05 = df["v_hawk"].rolling(lookback).quantile(0.05).iloc[-1]
        q95 = df["v_hawk"].rolling(lookback).quantile(0.95).iloc[-1]
        if sig == 1:
            return f"LONG {symbol} @{close_price:.4f} → q95 breakout (v={v:.4f}, q95={q95:.4f})"
        elif sig == -1:
            return f"SHORT {symbol} @{close_price:.4f} → q95 breakout (v={v:.4f}, q95={q95:.4f})"
        elif sig == 0:
            return f"EXIT {symbol} @{close_price:.4f} → q05 exit (v={v:.4f}, q05={q05:.4f})"
    return None


# =========================================================
# TRADE JOURNAL
# =========================================================
class TradeJournal:
    def __init__(self, path: Path):
        self.path = path
        self.journal = {}
        self._init_csv()

    def _init_csv(self):
        if not self.path.exists():
            df = pd.DataFrame(columns=[
                "journey_id", "symbol", "side", "entry_time", "entry_price",
                "exit_time", "exit_price", "return_pct"
            ])
            df.to_csv(self.path, index=False)

    def _load_csv(self):
        return pd.read_csv(self.path)

    def _save_csv(self, df):
        df.to_csv(self.path, index=False)

    def add_trade(self, symbol, side, entry_price):
        df = self._load_csv()
        jid = f"{symbol.replace('/', '')}-{uuid.uuid4().hex[:8]}"
        new_row = {
            "journey_id": jid,
            "symbol": symbol,
            "side": side,
            "entry_time": dt.datetime.utcnow().isoformat(),
            "entry_price": entry_price,
            "exit_time": "",
            "exit_price": "",
            "return_pct": ""
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        self._save_csv(df)

        if symbol not in self.journal:
            self.journal[symbol] = []
        self.journal[symbol].append(new_row)

        logging.info(f"Opened {side} {symbol} @ {entry_price:.4f} | journey_id={jid}")
        return jid

    def close_latest_trade(self, symbol, close_price):
        if symbol not in self.journal or len(self.journal[symbol]) == 0:
            return

        last_trade = self.journal[symbol][-1]
        if last_trade["exit_price"] != "":
            return

        jid = last_trade["journey_id"]
        side = last_trade["side"]
        entry_price = float(last_trade["entry_price"])
        pnl = ((close_price - entry_price) / entry_price) * (1 if side == "LONG" else -1)

        last_trade["exit_price"] = close_price
        last_trade["exit_time"] = dt.datetime.utcnow().isoformat()
        last_trade["return_pct"] = pnl

        df = self._load_csv()
        idx = df.index[df["journey_id"] == jid].tolist()[0]
        df.at[idx, "exit_time"] = last_trade["exit_time"]
        df.at[idx, "exit_price"] = close_price
        df.at[idx, "return_pct"] = pnl
        self._save_csv(df)

        logging.info(f"Closed {symbol} journey={jid} | pnl={pnl:.4%}")


# =========================================================
# DATA FETCH + COMPUTE
# =========================================================
def get_initial_data(symbol: str, tf: str, limit=1000):
    exchange = ccxt.binance({"options": {"defaultType": "future"}})
    bars = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
    data = pd.DataFrame(bars, columns=["timestamp", "open", "high", "low", "close", "volume"])
    data["timestamp"] = pd.to_datetime(data["timestamp"], unit="ms")
    return data.set_index("timestamp").astype(float)


def compute_hawkes_signal(df):
    log_h = np.log(df["high"])
    log_l = np.log(df["low"])
    log_c = np.log(df["close"])

    df["atr"] = atr(log_h, log_l, log_c, norm_lookback)
    df["norm_range"] = (log_h - log_l) / df["atr"]
    df["v_hawk"] = hawkes_process(df["norm_range"], kappa)
    df["sig"] = vol_signal(df["close"], df["v_hawk"], lookback)
    return df


# =========================================================
# LIVE EVALUATOR
# =========================================================
async def evaluate_ticker(symbol, tf, journal: TradeJournal):
    ws_symbol = symbol.replace("/", "").lower()
    url = f"wss://fstream.binance.com/ws/{ws_symbol}@kline_{tf}"

    data = get_initial_data(symbol, tf)
    data = compute_hawkes_signal(data)

    logging.info(f"Ready for {symbol} ({tf}) | Last signal={int(data['sig'].iloc[-1])}")

    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            msg = json.loads(msg)
            k = msg["k"]

            if k["x"]:
                

                ts = pd.to_datetime(k["t"], unit="ms")
                row = {
                    "open": float(k["o"]),
                    "high": float(k["h"]),
                    "low": float(k["l"]),
                    "close": float(k["c"]),
                    "volume": float(k["v"]),
                }
                print(json.dumps(row))
                data.loc[ts] = row
                data = data.tail(1000)
                data = compute_hawkes_signal(data)

                trade_line = evaluate_hawkes_trade(symbol, data)
                if trade_line:
                    logging.info(trade_line)

                    sig = int(data["sig"].iloc[-1])
                    price = float(data["close"].iloc[-1])

                    if sig == 1:
                        journal.add_trade(symbol, "LONG", price)
                    elif sig == -1:
                        journal.add_trade(symbol, "SHORT", price)
                    elif sig == 0:
                        journal.close_latest_trade(symbol, price)


# =========================================================
# MAIN
# =========================================================
async def main():
    journal = TradeJournal(TRADE_LOG)
    await evaluate_ticker(SYMBOL, TF, journal)


if __name__ == "__main__":
    asyncio.run(main())
