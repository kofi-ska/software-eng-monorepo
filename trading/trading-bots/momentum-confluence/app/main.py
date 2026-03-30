# app/main.py

import os
import asyncio
import aiohttp
from datetime import datetime,timezone
from app.config import Config
from core.engine import TradingEngine, BinanceFuturesTradeFeed
from outputs.csv_output import CSVTradeOutput


def _parse_intervals(env_val: str) -> list:
    """Parse comma-separated intervals from env var into a list (strip whitespace)."""
    if not env_val:
        return []
    return [s.strip() for s in env_val.split(",") if s.strip()]


async def run_forever(engine: TradingEngine, tickers: list, intervals: list, cycle_delay: int = 300):
    """
    Run continuous evaluation loop:
    - each cycle runs full analysis across tickers
    - if valid decisions are generated, they're written to CSV
    - sleeps for `cycle_delay` seconds between cycles
    """
    cycle = 1
    while True:
        now = datetime.now(timezone.utc)
        print(f"\n[{now.isoformat()}] Cycle {cycle} start — evaluating {len(tickers)} pairs")
        start = datetime.now(timezone.utc)
        try:
            await engine.run(tickers, intervals=intervals)
        except Exception as e:
            print(f"[Engine] Cycle {cycle} failed: {e}")
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        now_after = datetime.now(timezone.utc)
        print(f"[{now_after.isoformat()}] Cycle {cycle} complete ({elapsed:.1f}s)")
        print(f"[Engine] Sleeping for {cycle_delay}s...\n")
        await asyncio.sleep(cycle_delay)
        cycle += 1


async def main():
    cfg = Config()

    # Load tickers
    with open(cfg.TICKERS_FILE) as f:
        tickers = [t.strip() for t in f.readlines() if t.strip()]

    # Intervals override (comma separated, e.g. "1m,5m,15m")
    env_intervals = _parse_intervals(os.getenv("INTERVALS", ""))
    if env_intervals:
        intervals = env_intervals
    else:
        intervals = ["1m", "5m", "15m", "1h", "4h"]

    # Concurrency (default = 6)
    concurrency = int(os.getenv("CONCURRENCY", "6"))

    # Delay between cycles (in seconds) — defaults to 300s (5 minutes)
    cycle_delay = int(os.getenv("CYCLE_DELAY", "300"))

    print(f"[Engine] Starting continuous evaluation loop")
    print(f"[Engine] Intervals: {intervals}")
    print(f"[Engine] Concurrency: {concurrency}")
    print(f"[Engine] Cycle delay: {cycle_delay}s")

    timeout = aiohttp.ClientTimeout(total=cfg.TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        feed = BinanceFuturesTradeFeed(session)
        output = CSVTradeOutput(cfg.CSV_FILE)
        engine = TradingEngine(feed, output, concurrency=concurrency)

        await run_forever(engine, tickers, intervals, cycle_delay)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Engine] Stopped by user.")
