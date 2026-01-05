import streamlit as st
import pandas as pd
import psycopg2
import pydeck as pdk
import time

st.set_page_config(page_title="UrbanFlow: Live Traffic", layout="wide")


def get_db_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port="5435",
        database="urbanflow",
        user="admin",
        password="password"
    )


def load_data():
    conn = get_db_connection()
    query = """
            SELECT taxi_id, lat, lon, speed, status, event_time
            FROM traffic_data
            ORDER BY event_time DESC LIMIT 2000 \
            """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


st.title("🚦 UrbanFlow: Real-Time Traffic Analysis")

placeholder = st.empty()

while True:
    try:
        df = load_data()
    except Exception as e:
        st.error(f"Database error: {e}")
        time.sleep(5)
        continue

    with placeholder.container():
        if df.empty:
            st.warning("Data stream has not started yet. Are Producer and Spark running?")
            st.info("Tip: You might need to restart the Spark job since the DB was reset.")
            time.sleep(2)
            continue

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Active Taxis", df['taxi_id'].nunique())
        col2.metric("Avg Speed", f"{df['speed'].mean():.1f} km/h")
        col3.metric("Traffic Jams", len(df[df['status'] == 'TRAFFIC_JAM']))

        last_time = df['event_time'].max()
        time_str = str(last_time.time())[:8] if pd.notnull(last_time) else "--:--:--"
        col4.metric("Latest Data Time", time_str)

        df['color'] = df['status'].apply(
            lambda x: [200, 30, 0, 160] if x == 'TRAFFIC_JAM' else [0, 200, 30, 160]
        )

        layer = pdk.Layer(
            "ScatterplotLayer",
            df,
            get_position=["lon", "lat"],
            get_color="color",
            get_radius=30,
            pickable=True,
            opacity=0.8,
            filled=True,
            radius_scale=6,
            radius_min_pixels=3,
            radius_max_pixels=10,
        )

        view_state = pdk.ViewState(
            latitude=41.0422,
            longitude=29.0060,
            zoom=11,
            pitch=45,
        )

        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "Taxi: {taxi_id}\nSpeed: {speed} km/h\nStatus: {status}"}
        ))

        with st.expander("Detailed Data Flow"):
            st.dataframe(df[['event_time', 'taxi_id', 'speed', 'status']].head(10))

    time.sleep(2)