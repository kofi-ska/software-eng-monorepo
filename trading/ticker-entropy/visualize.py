# visualize.py
import asyncio
import time
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

# import EntropyMonitor + entropy_history from monitor.py
from monitor import EntropyMonitor, entropy_history

# -------------------- Dash App --------------------
app = dash.Dash(__name__)
app.title = "Real-Time Market Entropy Monitor"

tickers = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "adausdt"]

app.layout = html.Div(
    [
        html.H2(
            "Real-Time Market Structural Entropy",
            style={"textAlign": "center", "marginBottom": "20px"},
        ),
        dcc.Interval(id="update", interval=3000, n_intervals=0),
        html.Div(
            [dcc.Graph(id=f"graph-{sym}", style={"height": "320px"}) for sym in tickers]
        ),
    ],
    style={"backgroundColor": "#f8f9fa", "padding": "10px"},
)


# -------------------- Dash Callbacks --------------------
@app.callback(
    [Output(f"graph-{sym}", "figure") for sym in tickers],
    Input("update", "n_intervals"),
)
def update_graphs(_):
    figs = []
    now = time.time()

    for sym in tickers:
        data = list(entropy_history[sym])
        if not data:
            figs.append(
                go.Figure(
                    layout=go.Layout(
                        title=f"{sym.upper()} — Waiting for live data...",
                        xaxis=dict(visible=False),
                        yaxis=dict(visible=False),
                        annotations=[
                            dict(
                                text="⏳ Waiting for live data feed...",
                                xref="paper",
                                yref="paper",
                                showarrow=False,
                                font=dict(size=16, color="gray"),
                            )
                        ],
                    )
                )
            )
            continue

        x = [d["t"] for d in data]
        y = [d["MSE"] for d in data]
        figs.append(
            go.Figure(
                data=[
                    go.Scatter(
                        x=x,
                        y=y,
                        mode="lines",
                        line=dict(color="#00cc96", width=2),
                        name="MSE",
                    )
                ],
                layout=go.Layout(
                    title=f"{sym.upper()}  (Entropy over time)",
                    xaxis=dict(title="Time", tickformat="%H:%M:%S"),
                    yaxis=dict(title="MSE", range=[0, max(1.5, max(y) + 0.2)]),
                    margin=dict(l=50, r=10, t=40, b=40),
                    paper_bgcolor="#ffffff",
                    plot_bgcolor="#f0f0f0",
                ),
            )
        )
    return figs


# -------------------- Background Monitor Runner --------------------
async def run_monitor():
    tickers = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "adausdt"]
    monitor = EntropyMonitor(tickers)
    await monitor.run()


def start_background_monitor():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(run_monitor())
    loop.run_forever()


# -------------------- Main Entry --------------------
if __name__ == "__main__":
    # Start monitor in background thread-safe manner
    import threading

    monitor_thread = threading.Thread(target=start_background_monitor, daemon=True)
    monitor_thread.start()

    # Launch Dash
    app.run(debug=True)
