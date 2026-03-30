# visualization/App.py
import sys, os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import streamlit as st
import asyncio
import logic.exchanges
from logic.core.Manager import LiveFeedManager
from logic.core.Config import AppConfig
from visualization.Sidebar import render_sidebar
from visualization.Dashboard import render_dashboard


st.set_page_config(page_title="Live Market Feed", layout="wide")
st.title("📈 Live Market Feed")
st.caption("Multi-exchange real-time market data & insights dashboard")

# --- setup manager ---
if "manager" not in st.session_state:
    st.session_state["manager"] = LiveFeedManager(AppConfig())

manager = st.session_state["manager"]

# --- handle sidebar ---
render_sidebar(manager)

# --- handle user actions ---
if "action" in st.session_state:
    action = st.session_state.pop("action")
    if action[0] == "add":
        _, exchange, market, feed_type, symbol = action
        asyncio.run(manager.add_ticker(symbol, exchange, market, feed_type))
    elif action[0] == "remove":
        _, exchange, market, feed_type, symbol = action
        asyncio.run(manager.remove_ticker(symbol))

# --- render dashboard ---
render_dashboard(manager)
import sys, os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
