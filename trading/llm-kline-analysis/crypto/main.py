import abc
import os
import json
import logging
import requests
import numpy as np
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timezone
from dotenv import load_dotenv


# ==========================================================
# Logging Setup
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ==========================================================
# Load Environment
# ==========================================================
load_dotenv()
LLM_KEY_OPENAI = os.getenv("OPENAI_API_KEY")
if not LLM_KEY_OPENAI:
    raise RuntimeError("LLM_KEY_OPENAI not found in .env file")


# ==========================================================
# Abstract Interfaces
# ==========================================================


class MarketDataSource(abc.ABC):
    """Abstract base for any market data provider."""

    @abc.abstractmethod
    def get_klines(
        self, symbol: str, interval: str, limit: int = 100
    ) -> List[List[Any]]:
        pass


class TradingLLM(abc.ABC):
    """Abstract base for any trading LLM provider."""

    SYSTEM_PROMPT: str = (
        "You are a trading reasoning assistant for a momentum trader. "
        "Analysis must be structured, detailed, and explanatory, using abductive and deductive reasoning. "
        "Do not hallucinate. Be clear and go as deep as possible, but ensure insights remain actionable."
        "\n\n"
        "Trading Analysis Framework:\n\n"
        "1. Price Action:\n"
        "- Identify if trend is up, down, or ranging.\n"
        "- Detect climaxes, lower highs, compressions.\n"
        "- Map major structural shifts.\n\n"
        "2. Volume:\n"
        "- Compare current volume to prior peaks in pumps/sell-offs.\n"
        "- Rising = conviction, Falling = exhaustion.\n\n"
        "- What all four actors are doing"
        "3. Order Book Depth:\n"
        "- Identify heavy bid/ask walls.\n"
        "- Note liquidity gaps where price can move rapidly.\n\n"
        "4. Momentum Indicators:\n"
        "- RSI: Overbought, oversold, or neutral? Any divergences?\n"
        "- MACD histogram: Positive/negative? Expanding/contracting?\n"
        "- Moving Averages (EMA7/EMA25): Is price above/below them? Any crossover shifts?\n\n"
        "5. Candlestick Patterns:\n"
        "- Look for rejection wicks, engulfing candles, doji, hammers.\n"
        "- Context matters: where do they form (top, mid-range, at support/resistance)?\n"
        "- infer any observed patterns from the guide provided. \n\n"
        "6. Wyckoff Framework:\n"
        "- Classify market phase: accumulation, markup, distribution, or markdown.\n"
        "- Identify BC (Buying Climax), AR (Automatic Reaction), ST (Secondary Test), UTAD, Spring.\n\n"
        "explanation with a detailed breakdown of the market state. This is critical."
        "7. Funding/Positioning (Crowd Bias):\n"
        "- Identify retail vs whale positioning.\n"
        "- Long/short ratio skew? Crowded trades? Fuel for squeeze?\n\n"
        "8. Support & Resistance Levels:\n"
        "- Use inferred_support and inferred_resistance levels.\n"
        "- Mark immediate support, next support, and critical resistance.\n"
        "- Which levels act as liquidity magnets?\n\n"
        "9. Spike Risks:\n"
        "- Spike risk: sudden move above resistance to liquidate shorts.\n"
        "- Reverse spike (Spring): drop below support to liquidate longs before reversal.\n\n"
        "Output Requirements:\n"
        "- Analysis must include Wyckoff phase, retail vs whale bull/bear dynamics, and detailed interpretation of support/resistance levels.\n"
        "- Provide a comprehensive thesis on current market behavior and intentions of the four actors (retail bulls, retail bears, whale bulls, whale bears).\n"
        "- Use abductive and deductive reasoning across all signals. infer from the intervals as well\n"
        "- Final output: JSON with keys: decision (BUY/SELL/HOLD), active_thesis, rationale, contracts[].\n"
        "- Each contract must include: action (LONG/SHORT), initial_position, target_position, entry_zone, exit_zone, stop_loss.\n"
        "- Contracts can be multiple for a trading strategy based on the thesis"
        "-Adhere to the response format else you break a downstream service"
    )

    @abc.abstractmethod
    def query(self, market_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        pass


# ==========================================================
# Binance Implementation
# ==========================================================


class BinanceDataSource(MarketDataSource):
    """Binance REST API wrapper for klines."""

    BASE_URL = "https://api.binance.com/api/v3/klines"

    def get_klines(
        self, symbol: str, interval: str, limit: int = 100
    ) -> List[List[Any]]:
        logging.info(f"Fetching {interval} klines for {symbol} from Binance...")
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        resp = requests.get(self.BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return [
            [k[0], float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])]
            for k in data
        ]


# ==========================================================
# Technical Analysis
# ==========================================================

candlestick_patterns = {
    "one_bar": {
        "doji": "Candle with nearly identical open and close prices, indicating indecision.",
        "dragonfly_doji": "Doji candle with no upper shadow and a long lower shadow; potential bullish reversal at the bottom of a trend.",
        "gravestone_doji": "Doji candle with no lower shadow and a long upper shadow; bearish reversal potential at trend tops.",
        "hammer": "Short-bodied candle with long lower shadow at the bottom of a downtrend; bullish reversal potential.",
        "hanging_man": "Hammer-shaped candle appearing at the top of an uptrend; signals potential bearish reversal.",
        "inverted_hammer": "Short-bodied candle with a long upper wick in a downtrend; suggests potential bullish reversal.",
        "shooting_star": "Short-bodied candle with long upper shadow at the top of an uptrend; indicates potential bearish reversal.",
        "spinning_top": "Short-bodied candle with relatively equal upper and lower shadows, reflecting indecision.",
        "marubozu": "Candle with no shadows indicating strong directional control by bulls or bears.",
        "high_wave": "Candle with very long upper and lower wicks; reflects extreme indecision and high volatility.",
        "long_legged_doji": "Doji with extremely long upper and lower shadows; signals major indecision and potential reversal.",
        "closing_marubozu": "Strong directional candle without a wick on the closing side; indicates momentum into the close.",
        "opening_marubozu": "Strong directional candle without a wick on the opening side; strong initial sentiment.",
    },
    "two_bar": {
        "bullish_engulfing": "Bullish candle fully engulfs previous bearish candle; indicates bullish reversal.",
        "bearish_engulfing": "Bearish candle fully engulfs previous bullish candle; indicates bearish reversal.",
        "piercing_line": "Bullish candle closes significantly above midpoint of prior bearish candle; bullish reversal pattern.",
        "dark_cloud_cover": "Bearish candle closes significantly below midpoint of prior bullish candle; bearish reversal pattern.",
        "harami": "Second candle's body fully contained within previous candle; signals weakening momentum and potential reversal.",
        "harami_cross": "Harami pattern with second candle as a Doji; stronger indication of reversal.",
        "tweezer_bottom": "Two candles with identical lows after a decline; signals bullish reversal.",
        "tweezer_top": "Two candles with identical highs after an advance; signals bearish reversal.",
        "inside_bar": "Second candle fully inside previous candle's range; suggests consolidation or reversal.",
        "outside_bar": "Second candle exceeds prior candle's range on both ends; indicates volatility and possible reversal.",
        "engulfing_star": "Engulfing pattern with a small second candle; strength depends heavily on context.",
    },
    "three_bar": {
        "morning_star": "Bearish candle → small-bodied candle (indecision) → bullish candle; indicates bullish reversal.",
        "evening_star": "Bullish candle → small-bodied candle (indecision) → bearish candle; indicates bearish reversal.",
        "three_white_soldiers": "Three consecutive bullish candles with strong upward momentum; signals bullish continuation.",
        "three_black_crows": "Three consecutive bearish candles with strong downward momentum; signals bearish continuation.",
        "three_inside_up": "Bullish Harami pattern followed by bullish confirmation candle; bullish reversal signal.",
        "three_inside_down": "Bearish Harami pattern followed by bearish confirmation candle; bearish reversal signal.",
        "rising_three_methods": "Strong bullish candle → small retracement candles → bullish continuation candle; bullish continuation pattern.",
        "falling_three_methods": "Strong bearish candle → small rally candles → bearish continuation candle; bearish continuation pattern.",
        "three_line_strike": "Three consecutive directional candles followed by a large opposite candle engulfing all three; strong reversal potential.",
        "stick_sandwich": "Bearish candle → bullish candle → bearish candle with matching lows; signals possible bullish reversal.",
        "side_by_side_white_lines": "Multiple candles with similar highs or lows after a gap; suggests continuation of prevailing trend.",
    },
    "advanced": {
        "island_reversal": "Price gaps away from the main trend, consolidates, then gaps back; strong reversal indication.",
        "abandoned_baby": "Doji candle separated by gaps from candles before and after; extremely strong reversal signal.",
        "hook_reversal": "Candle with lower high and higher low relative to previous; suggests intermediate-term reversal.",
        "san_ku": "Three sequential price gaps; indicates trend exhaustion and strong reversal potential.",
        "kicker_pattern": "Sudden reversal pattern with gap and large opposite-direction candle; highly significant.",
        "upside_tasuki_gap": "Bullish gap with subsequent failed bearish retracement attempt; bullish continuation signal.",
        "downside_tasuki_gap": "Bearish gap with subsequent failed bullish retracement attempt; bearish continuation signal.",
        "mat_hold": "Strong bullish candle → tight consolidation → bullish breakout; bullish continuation.",
        "deliberation": "Three bullish candles of decreasing size; warns of diminishing momentum and potential reversal.",
        "ladder_bottom": "Five candles descending with final reversal candle; bullish reversal.",
        "ladder_top": "Five candles ascending with final reversal candle; bearish reversal.",
        "side_gap_three_methods": "Gap followed by small consolidation candles and continuation; continuation signal.",
        "separating_lines": "Opposite-direction candles opening at same price; continuation pattern.",
        "belt_hold": "Candle opens at extreme high or low and moves strongly in the opposite direction; strong directional reversal or continuation.",
    },
}


def compute_ta(
    klines: List[List[float]],
    rsi_period: int = 14,
    bb_period: int = 20,
    bb_std: float = 2.0,
    ema_short: int = 7,
    ema_mid: int = 25,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
) -> Dict[str, Any]:
    """Compute EMA7, EMA25, RSI, Bollinger Bands, MACD from kline data."""
    df = pd.DataFrame(
        klines, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)

    # EMA
    df[f"ema{ema_short}"] = df["close"].ewm(span=ema_short, adjust=False).mean()
    df[f"ema{ema_mid}"] = df["close"].ewm(span=ema_mid, adjust=False).mean()

    # RSI
    delta = df["close"].diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(gain).rolling(window=rsi_period).mean()
    roll_down = pd.Series(loss).rolling(window=rsi_period).mean()
    rs = roll_up / roll_down
    df["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    # Bollinger Bands
    df["bb_middle"] = df["close"].rolling(window=bb_period).mean()
    df["bb_std"] = df["close"].rolling(window=bb_period).std()
    df["bb_upper"] = df["bb_middle"] + (bb_std * df["bb_std"])
    df["bb_lower"] = df["bb_middle"] - (bb_std * df["bb_std"])

    # MACD
    ema_fast_series = df["close"].ewm(span=macd_fast, adjust=False).mean()
    ema_slow_series = df["close"].ewm(span=macd_slow, adjust=False).mean()
    df["macd_line"] = ema_fast_series - ema_slow_series
    df["macd_signal"] = df["macd_line"].ewm(span=macd_signal, adjust=False).mean()
    df["macd_hist"] = df["macd_line"] - df["macd_signal"]

    last = df.iloc[-1]
    return {
        "close": float(last["close"]),
        "volume": float(last["volume"]),
        "rsi": float(last["rsi"]),
        f"ema{ema_short}": float(last[f"ema{ema_short}"]),
        f"ema{ema_mid}": float(last[f"ema{ema_mid}"]),
        "bb_upper": float(last["bb_upper"]),
        "bb_lower": float(last["bb_lower"]),
        "macd_line": float(last["macd_line"]),
        "macd_signal": float(last["macd_signal"]),
        "macd_hist": float(last["macd_hist"]),
    }


# ==========================================================
# Support/Resistance Inference
# ==========================================================


def swing_levels(prices: List[float], window: int = 5):
    support, resistance = [], []
    for i in range(window, len(prices) - window):
        local_low = all(
            prices[i] <= prices[j] for j in range(i - window, i + window + 1)
        )
        local_high = all(
            prices[i] >= prices[j] for j in range(i - window, i + window + 1)
        )
        if local_low:
            support.append(prices[i])
        if local_high:
            resistance.append(prices[i])
    return support, resistance


def volume_profile(prices: List[float], volumes: List[float], bins: int = 20):
    hist, edges = np.histogram(prices, bins=bins, weights=volumes)
    levels = [((edges[i] + edges[i + 1]) / 2, hist[i]) for i in range(len(hist))]
    levels = sorted(levels, key=lambda x: -x[1])
    return [lvl[0] for lvl in levels[:5]]


def compute_support_resistance(klines: List[List[float]], top_n: int = 3):
    closes = [k[4] for k in klines]
    volumes = [k[5] for k in klines]

    sup, res = swing_levels(closes, window=5)
    vp_levels = volume_profile(closes, volumes, bins=20)

    levels = sorted(set(sup + res + vp_levels))
    mid = closes[-1]

    inferred_support = [lvl for lvl in levels if lvl < mid][-top_n:]
    inferred_resistance = [lvl for lvl in levels if lvl > mid][:top_n]

    return inferred_support, inferred_resistance


# ==========================================================
# LLM Implementations
# ==========================================================


def parse_llm_response(raw: str) -> dict:
    """
    Strips Markdown-style ```json or ``` wrappers and parses the string as JSON.

    Args:
        raw (str): The raw response string from the LLM.

    Returns:
        dict: Parsed JSON content.

    Raises:
        json.JSONDecodeError: If the cleaned string is not valid JSON.
    """
    cleaned = (
        raw.strip()
        .removeprefix("```json")
        .removeprefix("```")
        .removesuffix("```")
        .strip()
    )
    return json.loads(cleaned)


class DeepSeekLLM(TradingLLM):
    def query(self, market_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("DeepSeek LLM integration not implemented.")


class OpenAILLM(TradingLLM):
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model
        self.endpoint = "https://api.openai.com/v1/chat/completions"

    def query(self, market_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        logging.info(f"Querying OpenAI model {self.model}...")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(market_snapshot)},
            ],
            "temperature": 0.3,
            "max_tokens": 5000,
        }
        resp = requests.post(self.endpoint, headers=headers, json=payload, timeout=90)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        logging.info("Received response from OpenAI.")
        return parse_llm_response(content)


# ==========================================================
# Snapshot Builder
# ==========================================================


def build_market_snapshot(source: MarketDataSource, symbol: str) -> Dict[str, Any]:
    logging.info(f"Building market snapshot for {symbol}...")
    intervals = ["1m", "5m", "15m", "1h", "4h"]
    retrieval_ts = datetime.now(timezone.utc).isoformat()

    snapshot = {"market": symbol, "retrieval_timestamp": retrieval_ts, "timeframes": {}}

    for interval in intervals:
        klines = source.get_klines(symbol, interval, limit=200)

        indicators = compute_ta(klines)
        sup, res = compute_support_resistance(klines, top_n=3)

        market_ts = int(klines[-1][0])
        market_dt = (
            datetime.utcfromtimestamp(market_ts / 1000)
            .replace(tzinfo=timezone.utc)
            .isoformat()
        )
        last_close = float(klines[-1][4])

        snapshot["timeframes"][interval] = {
            "klines": klines,
            "derived_ta_indicators": indicators,
            "inferred_support": sup,
            "inferred_resistance": res,
            "market_timestamp": market_ts,
            "market_datetime": market_dt,
            "retrieval_timestamp": retrieval_ts,
            "last_close": last_close,
        }
        logging.info(f"Completed indicators for {interval} interval.")

    logging.info("Snapshot build complete.")
    return snapshot


# ==========================================================
# Main Orchestration
# ==========================================================

if __name__ == "__main__":
    source = BinanceDataSource()
    ticker = "YBUSDT"
    snapshot = build_market_snapshot(source, ticker)
    snapshot["kline_pattern_guide"] = candlestick_patterns
    llm = OpenAILLM(api_key=LLM_KEY_OPENAI)
    decision = {}
   
    decision = llm.query(snapshot)
    decision["ticker"] = ticker

    logging.info("Final structured decision:")
    print(json.dumps(decision, indent=2))
