def orderbook_computation(bids, asks, depth=10):
    """Compute basic order book metrics from top levels."""
    bids = bids[:depth]
    asks = asks[:depth]

    bid_vol = sum(float(qty) for _, qty in bids)
    ask_vol = sum(float(qty) for _, qty in asks)

    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    spread = best_ask - best_bid

    bid_wavg = sum(float(p) * float(q) for p, q in bids) / (bid_vol + 1e-9)
    ask_wavg = sum(float(p) * float(q) for p, q in asks) / (ask_vol + 1e-9)
    micro_price = (ask_vol * best_bid + bid_vol * best_ask) / (bid_vol + ask_vol + 1e-9)

    imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-9)
    pressure = "buy" if imbalance > 0 else "sell" if imbalance < 0 else "neutral"

    return {
        "depth": depth,
        "bid_volume": bid_vol,
        "ask_volume": ask_vol,
        "imbalance": imbalance,
        "pressure": pressure,
        "spread": spread,
        "bid_weighted": bid_wavg,
        "ask_weighted": ask_wavg,
        "micro_price": micro_price,
    }
