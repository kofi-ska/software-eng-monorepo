import pandas as pd
import numpy as np
import ta



def wyckoff_features(df, window=45):
    df = df.copy()
    df["ret"] = df["close"].pct_change()
    df["volatility"] = df["ret"].rolling(window).std()
    df["trend"] = df["close"].rolling(window).mean().diff()
    df["vol_mean"] = df["volume"].rolling(window).mean()
    df["vol_slope"] = df["vol_mean"].diff()
    return df


def classify_phase(df, window=45):
    df = wyckoff_features(df, window)
    phases = []
    for i in range(len(df)):
        if i < window:
            phases.append(np.nan)
            continue

        t = df["trend"].iloc[i]
        vol_slope = df["vol_slope"].iloc[i]
        vol = df["volatility"].iloc[i]
        vol_prev = df["volatility"].iloc[i - 1]

        if t > 0 and vol_slope > 0 and vol > vol_prev:
            phase = "markup"
        elif t < 0 and vol_slope > 0 and vol > vol_prev:
            phase = "markdown"
        else:
            mean_vol = df["volatility"].rolling(window).mean().iloc[i]
            if abs(t) < 1e-5 and vol < mean_vol:
                prior_trend = df["trend"].iloc[max(0, i - window) : i].mean()
                phase = "accumulation" if prior_trend < 0 else "distribution"
            else:
                phase = "transition"
        phases.append(phase)
    df["wyckoff_phase"] = phases
    return df



def compute_indicators(df):
    df["rsi"] = ta.momentum.RSIIndicator(df["close"], window=14).rsi()
    df["ema_fast"] = df["close"].ewm(span=12).mean()
    df["ema_slow"] = df["close"].ewm(span=26).mean()
    df["ema_signal"] = np.where(df["ema_fast"] > df["ema_slow"], 1, -1)
    return df


def support_resistance(df, window=60):
    df["support"] = df["low"].rolling(window).min()
    df["resistance"] = df["high"].rolling(window).max()
    return df



def kline_computation(df, window=45):
    df = classify_phase(df, window)
    df = compute_indicators(df)
    df = support_resistance(df, window)
    latest = df.iloc[-1]

    return {
        "wyckoff_phase": latest["wyckoff_phase"],
        "rsi": float(latest["rsi"]),
        "ema_fast": float(latest["ema_fast"]),
        "ema_slow": float(latest["ema_slow"]),
        "ema_signal": int(latest["ema_signal"]),
        "support": float(latest["support"]),
        "resistance": float(latest["resistance"]),
        "timestamp": str(latest.get("close_time", latest.name)),
    }
