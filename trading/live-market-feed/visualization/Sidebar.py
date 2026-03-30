# visualization/Sidebar.py

import streamlit as st
from logic.core.Registry import list_registered_exchanges
from logic.core.Config import AppConfig


def render_sidebar(manager):
    """
    Streamlit sidebar for managing live feeds and tickers.
    """
    st.sidebar.header("⚙️ Live Feed Controls")

    # --- exchange selection ---
    exchanges = list_registered_exchanges()
    exchange = st.sidebar.selectbox("Exchange", exchanges, index=0)

    # --- market type selection ---
    market = st.sidebar.selectbox("Market", ["futures", "spot"], index=0)

    # --- feed type ---
    feed_type = st.sidebar.selectbox("Feed Type", ["orderbook", "klines"], index=0)

    # --- ticker input ---
    symbol = st.sidebar.text_input("Ticker Symbol", value=AppConfig.default_symbol)

    # --- add/remove controls ---
    col1, col2 = st.sidebar.columns(2)
    if col1.button("➕ Add Feed"):
        st.session_state["action"] = ("add", exchange, market, feed_type, symbol)
    if col2.button("➖ Remove Feed"):
        st.session_state["action"] = ("remove", exchange, market, feed_type, symbol)

    # --- active contexts display ---
    st.sidebar.markdown("### Active Exchanges")
    active = manager.list_contexts()
    if not active:
        st.sidebar.info("No active exchanges yet.")
    else:
        for ctx_id in active:
            st.sidebar.write(f"✅ {ctx_id}")

    # --- concurrency info ---
    st.sidebar.markdown("---")
    st.sidebar.caption(
        f"Max Exchanges: {manager.config.max_concurrent_exchanges} | Active: {len(active)}"
    )
