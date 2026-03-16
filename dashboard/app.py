import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

DB_URL = "postgresql+psycopg2://postgres:123456@postgres:5432/postgres"


@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


def load_latest_trades(engine):
    query = """
    SELECT trade_id, symbol, price, quantity, trade_time, processed_at
    FROM market_trade_clean
    ORDER BY trade_time DESC
    LIMIT 50
    """
    return pd.read_sql(query, engine)


def load_kline_data(engine):
    query = """
    SELECT window_start, window_end, symbol,
           open_price, high_price, low_price, close_price,
           total_volume, trade_count, processed_at
    FROM btc_kline_5s
    ORDER BY window_start DESC
    LIMIT 100
    """
    return pd.read_sql(query, engine)


def main():
    st.set_page_config(page_title="Real-time BTC Kline Dashboard", layout="wide")

    # 每 2 秒自動刷新一次
    st_autorefresh(interval=2000, key="btc_dashboard_refresh")

    st.title("Real-time BTC Kline Dashboard")

    engine = get_engine()

    trades_df = load_latest_trades(engine)
    kline_df = load_kline_data(engine)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Symbol", "BTCUSDT")

    if not trades_df.empty:
        latest_price = float(trades_df.iloc[0]["price"])
        latest_trade_time = trades_df.iloc[0]["trade_time"]

        with col2:
            st.metric("Latest Price", f"{latest_price:,.2f}")

        with col3:
            st.metric("Latest Trade Time", str(latest_trade_time))
    else:
        with col2:
            st.metric("Latest Price", "No Data")
        with col3:
            st.metric("Latest Trade Time", "No Data")

    st.subheader("Latest Trades")
    st.dataframe(trades_df, use_container_width=True)

    st.subheader("5-Second Kline Data")
    st.dataframe(kline_df, use_container_width=True)

    if not kline_df.empty:
        chart_df = kline_df.sort_values("window_start").copy()

        fig = go.Figure(data=[
            go.Candlestick(
                x=chart_df["window_start"],
                open=chart_df["open_price"],
                high=chart_df["high_price"],
                low=chart_df["low_price"],
                close=chart_df["close_price"],
                name="BTCUSDT"
            )
        ])

        fig.update_layout(
            title="BTCUSDT 5-Second Candlestick",
            xaxis_title="Time",
            yaxis_title="Price",
            xaxis_rangeslider_visible=False,
            height=600
        )

        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Volume / Trade Count")

        col4, col5 = st.columns(2)

        with col4:
            vol_fig = go.Figure()
            vol_fig.add_bar(
                x=chart_df["window_start"],
                y=chart_df["total_volume"],
                name="Volume"
            )
            vol_fig.update_layout(
                title="Total Volume per 5-Second Window",
                xaxis_title="Time",
                yaxis_title="Volume"
            )
            st.plotly_chart(vol_fig, use_container_width=True)

        with col5:
            count_fig = go.Figure()
            count_fig.add_bar(
                x=chart_df["window_start"],
                y=chart_df["trade_count"],
                name="Trade Count"
            )
            count_fig.update_layout(
                title="Trade Count per 5-Second Window",
                xaxis_title="Time",
                yaxis_title="Trades"
            )
            st.plotly_chart(count_fig, use_container_width=True)


if __name__ == "__main__":
    main()