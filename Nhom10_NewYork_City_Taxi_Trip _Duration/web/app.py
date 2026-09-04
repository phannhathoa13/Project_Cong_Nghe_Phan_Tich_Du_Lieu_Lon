import streamlit as st
import pandas as pd
import psycopg2
import pydeck as pdk


# 1. Cấu hình Web

st.set_page_config(
    page_title="Traffic Flow Analysis",
    layout="wide"
)

st.title("Traffic Flow Analysis")

st.write(
    "Real-time traffic monitoring and "
    "1-hour traffic prediction."
)


# 2. Refresh

if st.button("Refresh Data"):
    st.rerun()


# 3. Kết nối PostgreSQL

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="traffic_db",
    user="postgres",
    password="123"
)


# 4. Đọc dữ liệu mới nhất

query = """
SELECT
    window_start,
    window_end,
    grid_id,
    trip_count,
    avg_speed,
    traffic_level,
    predicted_speed,
    predicted_traffic_level,
    latitude,
    longitude
FROM traffic_data
WHERE latitude IS NOT NULL
AND longitude IS NOT NULL
AND window_start = (
    SELECT MAX(window_start)
    FROM traffic_data
)
ORDER BY trip_count DESC;
"""


df = pd.read_sql(
    query,
    conn
)

conn.close()


# 5. Kiểm tra dữ liệu

if df.empty:

    st.warning(
        "Chưa có dữ liệu giao thông."
    )

    st.stop()


# 6. Thời gian mới nhất

latest_time = df[
    "window_start"
].max()

st.write(
    "Latest traffic window:",
    latest_time
)


# 7. Metrics

avg_speed = df[
    "avg_speed"
].mean()

predicted_avg_speed = df[
    "predicted_speed"
].mean()


high_count = len(
    df[
        df["traffic_level"] == "HIGH"
    ]
)

predicted_high_count = len(
    df[
        df["predicted_traffic_level"]
        == "HIGH"
    ]
)


col1, col2, col3, col4, col5, col6 = st.columns(6)


col1.metric(
    "Total Grids",
    len(df)
)

col2.metric(
    "Current Avg Speed",
    f"{avg_speed:.2f} km/h"
)

col3.metric(
    "Predicted Avg Speed",
    f"{predicted_avg_speed:.2f} km/h"
)

col4.metric(
    "Current HIGH",
    high_count
)

col5.metric(
    "Predicted HIGH",
    predicted_high_count
)

col6.metric(
    "Records",
    len(df)
)


# 8. Chọn Current / Predicted

st.subheader(
    "Traffic View"
)


view_mode = st.selectbox(
    "View Mode",
    [
        "CURRENT",
        "PREDICTED"
    ]
)


if view_mode == "CURRENT":

    level_column = "traffic_level"
    speed_column = "avg_speed"

else:

    level_column = (
        "predicted_traffic_level"
    )

    speed_column = (
        "predicted_speed"
    )


# 9. Filter Traffic Level

traffic_filter = st.selectbox(
    "Traffic Level",
    [
        "ALL",
        "HIGH",
        "MEDIUM",
        "LOW"
    ]
)


df_filtered = df.copy()


if traffic_filter != "ALL":

    df_filtered = df_filtered[
        df_filtered[level_column]
        == traffic_filter
    ]


# 10. Bảng dữ liệu

st.subheader(
    "Traffic Data"
)


st.dataframe(
    df_filtered[
        [
            "grid_id",
            "trip_count",
            "avg_speed",
            "traffic_level",
            "predicted_speed",
            "predicted_traffic_level",
            "window_start"
        ]
    ],
    use_container_width=True
)


# 11. Màu Traffic

def traffic_color(level):

    if level == "HIGH":

        return [
            255,
            0,
            0,
            180
        ]

    elif level == "MEDIUM":

        return [
            255,
            165,
            0,
            180
        ]

    else:

        return [
            0,
            200,
            0,
            180
        ]


df_filtered[
    "color"
] = df_filtered[
    level_column
].apply(
    traffic_color
)


# 12. Bản đồ

st.subheader(
    "Traffic Map"
)


layer = pdk.Layer(
    "ScatterplotLayer",

    data=df_filtered,

    get_position=[
        "longitude",
        "latitude"
    ],

    get_fill_color="color",

    get_radius=120,

    pickable=True
)


view_state = pdk.ViewState(
    latitude=40.75,
    longitude=-73.98,
    zoom=10
)


tooltip = {
    "html":
        "<b>Grid:</b> {grid_id}<br>"
        "<b>Current Speed:</b> {avg_speed}<br>"
        "<b>Current Traffic:</b> {traffic_level}<br>"
        "<b>Predicted Speed:</b> {predicted_speed}<br>"
        "<b>Predicted Traffic:</b> {predicted_traffic_level}"
}


deck = pdk.Deck(
    layers=[
        layer
    ],

    initial_view_state=view_state,

    tooltip=tooltip
)


st.pydeck_chart(
    deck,
    use_container_width=True
)