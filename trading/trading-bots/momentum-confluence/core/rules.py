# core/rules.py
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from statistics import median

# try to use your utils/indicators if present; otherwise fallback to simple impls
try:
    from utils.indicators import ema as util_ema, rsi as util_rsi, atr as util_atr
except Exception:
    util_ema = None
    util_rsi = None
    util_atr = None


def _ema(values: List[float], period: int) -> List[float]:
    if util_ema:
        return util_ema(values, period)
    if len(values) < period:
        return []
    alpha = 2 / (period + 1)
    out = [sum(values[:period]) / period]
    for v in values[period:]:
        out.append((v - out[-1]) * alpha + out[-1])
    return out


def _rsi(values: List[float], period: int = 14) -> float:
    if util_rsi:
        return util_rsi(values, period)
    if len(values) < period + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        delta = values[-i] - values[-i - 1]
        if delta > 0:
            gains += delta
        else:
            losses += abs(delta)
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    if util_atr:
        return util_atr(highs, lows, closes, period)
    if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        hl = highs[-i] - lows[-i]
        hc = abs(highs[-i] - closes[-i - 1])
        lc = abs(lows[-i] - closes[-i - 1])
        trs.append(max(hl, hc, lc))
    return sum(trs) / period


# ---------------------------------------------------------------------------
# Data shapes expected:
# - kline_data: Dict[str, List[Dict]] mapping interval -> list of candles
#     candle dict: {"open": float, "high": float, "low": float, "close": float, "volume": float}
# - orderbook: {"bids": [[price, qty], ...], "asks": [[price, qty], ...]}
# - optional market_stats: {"24h_volume": float, "market_cap": float, "funding_rate": float}
# ---------------------------------------------------------------------------


@dataclass
class RuleResult:
    # discrete signals: -1, 0, +1
    structure: int = 0
    momentum: int = 0
    flow: int = 0
    volatility: int = 0

    # continuous confidence scores in [-1.0, 1.0]
    scores: Dict[str, float] = None

    # optional metadata for audit/debug
    meta: Dict[str, Any] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "structure": self.structure,
            "momentum": self.momentum,
            "flow": self.flow,
            "volatility": self.volatility,
            "scores": self.scores or {},
            "meta": self.meta or {},
        }


class RuleEngine:
    """
    Robust rule engine. Accepts multi-interval klines and orderbook data.
    """

    # intervals recommended (short, mid, long)
    SHORT = "1m"
    MID = "15m"
    LONG = "4h"

    def __init__(
        self,
        kline_data: Dict[str, List[Dict[str, float]]],
        orderbook: Dict[str, List[List[float]]],
        market_stats: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        kline_data: mapping interval->list of candles (oldest..newest)
        orderbook: {'bids': [[price,qty],...], 'asks': [[price,qty],...]}
        market_stats: optional dict with '24h_volume', 'market_cap', etc.
        config: optional thresholds:
            - rsi_period, atr_period, imbalance_threshold, liquidity_min_volume, etc.
        """
        self.klines = kline_data or {}
        self.orderbook = orderbook or {"bids": [], "asks": []}
        self.market_stats = market_stats or {}
        cfg = config or {}
        self.rsi_period = int(cfg.get("rsi_period", 14))
        self.atr_period = int(cfg.get("atr_period", 14))
        self.imbalance_threshold = float(cfg.get("imbalance_threshold", 0.12))
        self.liquidity_min_24h = float(cfg.get("liquidity_min_24h", 5_000_000))  # USD
        self.volatility_expansion_factor = float(cfg.get("volatility_expansion_factor", 1.25))

    # -------------------------
    # Helpers
    # -------------------------
    def _get_closes(self, interval: str) -> List[float]:
        candles = self.klines.get(interval, [])
        return [float(c["close"]) for c in candles]

    def _get_highs(self, interval: str) -> List[float]:
        candles = self.klines.get(interval, [])
        return [float(c["high"]) for c in candles]

    def _get_lows(self, interval: str) -> List[float]:
        candles = self.klines.get(interval, [])
        return [float(c["low"]) for c in candles]

    def _get_volumes(self, interval: str) -> List[float]:
        candles = self.klines.get(interval, [])
        return [float(c.get("volume", 0.0)) for c in candles]

    # -------------------------
    # Market-regime filter
    # -------------------------
    def evaluate_market_regime(self, long_interval: str = "1d") -> int:
        """
        Basic regime filter: +1 bullish, -1 bearish, 0 neutral.
        Uses EMA(50) / EMA(200) on a long interval if available; falls back to mid interval.
        """
        # prefer daily if provided
        interval = long_interval if long_interval in self.klines else self.LONG
        closes = self._get_closes(interval)
        if len(closes) < 50:
            return 0
        ema50 = _ema(closes, 50)
        ema200 = _ema(closes, 200)
        if not ema50 or not ema200:
            return 0
        latest50 = ema50[-1]
        latest200 = ema200[-1]
        if latest50 > latest200:
            return +1
        elif latest50 < latest200:
            return -1
        return 0

    # -------------------------
    # Structure (multi-timeframe)
    # -------------------------
    def evaluate_structure(self) -> Tuple[int, float, Dict[str, Any]]:
        """
        Use mid and long intervals to determine structural bias.
        Returns (discrete_signal, score, meta)
        score is in [-1,1], positive bullish.
        """
        meta = {}
        intervals = [self.MID, self.LONG]
        scores = []
        reasons = []
        for itv in intervals:
            closes = self._get_closes(itv)
            if len(closes) < 30:
                # insufficient data -> neutral
                scores.append(0.0)
                reasons.append(f"{itv}:insufficient")
                continue
            # simple ema cross (20 vs 50) on this interval
            ema20 = _ema(closes, 20)
            ema50 = _ema(closes, 50)
            if not ema20 or not ema50:
                scores.append(0.0)
                reasons.append(f"{itv}:no-ema")
                continue
            last20 = ema20[-1]
            last50 = ema50[-1]
            raw = (last20 - last50) / last50  # normalized gap
            # clamp, convert to [-1,1] with a soft threshold
            score = max(-1.0, min(1.0, raw * 10))
            scores.append(score)
            reasons.append(f"{itv}:gap={raw:.4f}")
        # aggregate (mean)
        score = sum(scores) / len(scores) if scores else 0.0
        discrete = 1 if score > 0.18 else (-1 if score < -0.18 else 0)
        meta["reasons"] = reasons
        meta["score_components"] = scores
        return discrete, score, meta

    # -------------------------
    # Momentum (RSI + EMA slope)
    # -------------------------
    def evaluate_momentum(self) -> Tuple[int, float, Dict[str, Any]]:
        """
        Use short and mid intervals: RSI and EMA slope.
        Returns discrete, score, meta.
        """
        meta = {}
        short_closes = self._get_closes(self.SHORT)
        mid_closes = self._get_closes(self.MID)

        rsi_short = _rsi(short_closes, self.rsi_period) if len(short_closes) >= self.rsi_period + 1 else 50.0
        rsi_mid = _rsi(mid_closes, self.rsi_period) if len(mid_closes) >= self.rsi_period + 1 else 50.0
        meta["rsi_short"] = rsi_short
        meta["rsi_mid"] = rsi_mid

        # EMA slope on mid
        slope = 0.0
        if len(mid_closes) >= 10:
            ema_short = _ema(mid_closes, 10)
            ema_long = _ema(mid_closes, 50)
            if ema_short and ema_long:
                slope = (ema_short[-1] - ema_long[-1]) / (ema_long[-1] + 1e-12)

        # convert RSI to a -1..1 score (rsi 50 -> 0)
        rsi_score = ((rsi_mid - 50) / 50.0)
        raw_score = 0.6 * rsi_score + 0.4 * (slope * 5)  # weighting
        # clamp
        score = max(-1.0, min(1.0, raw_score))
        discrete = 1 if score > 0.25 else (-1 if score < -0.25 else 0)
        meta["ema_slope"] = slope
        meta["raw_score"] = raw_score
        return discrete, score, meta

    # -------------------------
    # Order-flow (depth-weighted imbalance + wall detection)
    # -------------------------
    def evaluate_flow(self) -> Tuple[int, float, Dict[str, Any]]:
        """
        Compute depth-weighted imbalance across top N levels and detect large walls.
        Returns discrete, score, meta.
        """
        meta = {}
        bids = self.orderbook.get("bids", [])[:20]
        asks = self.orderbook.get("asks", [])[:20]

        # total qty near top
        top_bid_qty = sum(q for _, q in bids[:5]) if bids else 0.0
        top_ask_qty = sum(q for _, q in asks[:5]) if asks else 0.0
        total_top = top_bid_qty + top_ask_qty + 1e-12
        imbalance = (top_bid_qty - top_ask_qty) / total_top
        meta["top_bid_qty"] = top_bid_qty
        meta["top_ask_qty"] = top_ask_qty
        meta["imbalance"] = imbalance

        # depth-weighted imbalance across 20 levels (weight by distance)
        def weighted_side(side):
            wsum = 0.0
            wtot = 0.0
            for i, (p, q) in enumerate(side):
                weight = 1.0 / (i + 1)  # nearer levels count more
                wsum += weight * q
                wtot += weight
            return wsum / (wtot + 1e-12)

        w_bids = weighted_side(bids)
        w_asks = weighted_side(asks)
        meta["w_bids"] = w_bids
        meta["w_asks"] = w_asks

        depth_score = (w_bids - w_asks) / (w_bids + w_asks + 1e-12)
        # combine with top imbalance
        raw_score = 0.6 * imbalance + 0.4 * depth_score
        # detect walls (a very large level close to top)
        wall_detected = False
        wall_info = None
        if bids and asks:
            # if a single top level quantity >> median of next 5 levels -> wall
            next_bid_med = median([q for _, q in bids[1:6]]) if len(bids) >= 6 else 0.0
            if next_bid_med > 0 and bids[0][1] > 6 * next_bid_med:
                wall_detected = True
                wall_info = ("bid", bids[0])
            next_ask_med = median([q for _, q in asks[1:6]]) if len(asks) >= 6 else 0.0
            if next_ask_med > 0 and asks[0][1] > 6 * next_ask_med:
                wall_detected = True
                wall_info = ("ask", asks[0])

        meta["wall_detected"] = wall_detected
        meta["wall_info"] = wall_info

        # clamp
        score = max(-1.0, min(1.0, raw_score))
        discrete = 1 if score > self.imbalance_threshold else (-1 if score < -self.imbalance_threshold else 0)

        # if wall detected opposite to imbalance, lower confidence (possible spoofing)
        if wall_detected and wall_info:
            side, level = wall_info
            # if imbalance implies buy pressure but a huge bid wall exists -> suspicious
            if (score > 0 and side == "ask") or (score < 0 and side == "bid"):
                score *= 0.5
                meta["spoof_adjustment"] = True

        return discrete, score, meta

    # -------------------------
    # Volatility (ATR / range expansion)
    # -------------------------
    def evaluate_volatility(self) -> Tuple[int, float, Dict[str, Any]]:
        """
        Detect expansion or compression in volatility on short and mid frames.
        Returns discrete, score, meta.
        """
        meta = {}
        short_highs = self._get_highs(self.SHORT)
        short_lows = self._get_lows(self.SHORT)
        short_closes = self._get_closes(self.SHORT)

        mid_highs = self._get_highs(self.MID)
        mid_lows = self._get_lows(self.MID)
        mid_closes = self._get_closes(self.MID)

        short_atr = _atr(short_highs, short_lows, short_closes, self.atr_period) if short_closes else 0.0
        mid_atr = _atr(mid_highs, mid_lows, mid_closes, self.atr_period) if mid_closes else 0.0
        meta["short_atr"] = short_atr
        meta["mid_atr"] = mid_atr

        # compare latest candle range vs rolling median of ranges
        def last_vs_median_range(clist):
            if len(clist) < 5:
                return 0.0
            ranges = [(c["high"] - c["low"]) for c in self.klines[self.SHORT][-14:]] if self.SHORT in self.klines else []
            if len(ranges) < 5:
                return 0.0
            last_range = ranges[-1]
            med = median(ranges[:-1]) if len(ranges) > 1 else ranges[-1]
            return (last_range / (med + 1e-12)) - 1.0  # relative expansion: 0 = neutral, >0 expansion

        expansion = last_vs_median_range(self.klines.get(self.SHORT, []))
        meta["short_range_expansion"] = expansion

        # combine signals (ATR normalized)
        raw = 0.0
        if mid_atr > 0:
            raw = (short_atr - mid_atr) / (mid_atr + 1e-12)
        # mix with expansion
        raw_score = 0.6 * raw + 0.4 * expansion
        score = max(-1.0, min(1.0, raw_score))
        discrete = 1 if score > 0.2 else (-1 if score < -0.2 else 0)
        return discrete, score, meta

    # -------------------------
    # Liquidity guard
    # -------------------------
    def liquidity_ok(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Quick guard: returns (ok, meta). ok=False means skip automated signals.
        Uses market_stats 24h volume (if provided) and top-of-book depth.
        """
        meta = {}
        vol24 = self.market_stats.get("24h_volume", 0.0)
        meta["24h_volume"] = vol24
        # check book depth near top
        bids = self.orderbook.get("bids", [])[:5]
        asks = self.orderbook.get("asks", [])[:5]
        top_liquidity = sum(q for _, q in bids + asks) if bids or asks else 0.0
        meta["top_liquidity"] = top_liquidity

        ok = True
        if vol24 and vol24 < self.liquidity_min_24h:
            ok = False
            meta["reason"] = "low_24h_volume"
        # if top liquidity tiny relative to volume -> not ok
        if top_liquidity < 1e-6:
            ok = False
            meta["reason"] = "no_book_depth"
        return ok, meta

    # -------------------------
    # Master evaluation
    # -------------------------
    def evaluate_all(self, require_liquidity: bool = True, regime_protect: bool = True) -> RuleResult:
        """
        Run all modules, return RuleResult.
        - require_liquidity: if True, returns neutral signals when liquidity check fails.
        - regime_protect: if True, apply market-regime guard (suppress shorts in strong bull, vice-versa).
        """
        meta: Dict[str, Any] = {}
        # liquidity
        liq_ok, liq_meta = self.liquidity_ok()
        meta["liquidity"] = liq_meta
        if require_liquidity and not liq_ok:
            # return neutral but include meta for auditing
            return RuleResult(
                structure=0,
                momentum=0,
                flow=0,
                volatility=0,
                scores={"structure": 0.0, "momentum": 0.0, "flow": 0.0, "volatility": 0.0},
                meta={"skipped": "low_liquidity", **meta},
            )

        # market regime
        regime = self.evaluate_market_regime()
        meta["regime"] = regime

        # run modules
        structure_d, structure_score, structure_meta = self.evaluate_structure()
        momentum_d, momentum_score, momentum_meta = self.evaluate_momentum()
        flow_d, flow_score, flow_meta = self.evaluate_flow()
        vol_d, vol_score, vol_meta = self.evaluate_volatility()

        meta["structure"] = structure_meta
        meta["momentum"] = momentum_meta
        meta["flow"] = flow_meta
        meta["volatility"] = vol_meta

        # regime protection: if regime_protect and regime strong, suppress opposite signals by lowering score
        if regime_protect and regime != 0:
            if regime > 0:
                # bullish regime: penalize short signals
                if momentum_score < 0:
                    momentum_score *= 0.45
                if structure_score < 0:
                    structure_score *= 0.45
                if flow_score < 0:
                    flow_score *= 0.45
            else:
                # bearish regime: penalize long signals
                if momentum_score > 0:
                    momentum_score *= 0.45
                if structure_score > 0:
                    structure_score *= 0.45
                if flow_score > 0:
                    flow_score *= 0.45

        scores = {
            "structure": round(structure_score, 4),
            "momentum": round(momentum_score, 4),
            "flow": round(flow_score, 4),
            "volatility": round(vol_score, 4),
        }

        # convert continuous scores back to discrete per thresholds used above
        def discretize(s, pos_th=0.2, neg_th=-0.2):
            if s >= pos_th:
                return 1
            if s <= neg_th:
                return -1
            return 0

        structure = discretize(scores["structure"], pos_th=0.18, neg_th=-0.18)
        momentum = discretize(scores["momentum"], pos_th=0.25, neg_th=-0.25)
        flow = discretize(scores["flow"], pos_th=self.imbalance_threshold, neg_th=-self.imbalance_threshold)
        volatility = discretize(scores["volatility"], pos_th=0.2, neg_th=-0.2)

        return RuleResult(
            structure=structure,
            momentum=momentum,
            flow=flow,
            volatility=volatility,
            scores=scores,
            meta=meta,
        )
