# visualization/Dashboard.py

import streamlit as st
import asyncio
import json
from logic.core.Manager import LiveFeedManager


async def display_live_feed(manager: LiveFeedManager):
    """
    Continuously pulls messages from all active feeds and displays live updates.
    """
    st.markdown("## 📡 Live Market Insights")

    placeholder = st.empty()

    while True:
        for ctx_id, ctx in manager.exchange_contexts.items():
            # for simplicity, we just show status per symbol
            for symbol, task in ctx.symbol_feeds.items():
                placeholder.write(f"🟢 **{ctx_id}** → {symbol}")
        await asyncio.sleep(2)


def render_dashboard(manager):
    """
    Renders the main dashboard UI.
    """
    st.title("💹 Live Market Feed Dashboard")
    st.markdown("View real-time order book & kline insights across exchanges.")
    st.divider()

    if not manager.exchange_contexts:
        st.info("Start by adding a feed from the sidebar 👈")
        return

    # Stream all current unified messages
    st.subheader("📊 Active Feeds")
    for ctx_id, ctx in manager.exchange_contexts.items():
        st.markdown(f"### {ctx_id}")
        for symbol in ctx.symbol_feeds.keys():
            st.write(f"🟢 `{symbol}` running...")

    # Optional: placeholder for future metrics, charts, etc.
    st.divider()
    st.caption("Insights stream live once feeds emit unified messages.")
