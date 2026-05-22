import streamlit as st
import pandas as pd
import pymysql
import time
from streamlit_echarts import st_echarts

# =====================================================
# 1. 页面配置
# =====================================================
st.set_page_config(
    page_title="空气质量实时监控平台",
    layout="wide"
)

# =====================================================
# 2. 页面样式
# =====================================================
st.markdown("""
<style>
.main {
    background-color: #0f172a !important;
}
h1 {
    color: white;
    text-align: center;
}
.block-container {
    padding-top: 1rem;
}
[data-testid="stMetricValue"] {
    color: #00f5ff;
}
.stDataFrame {
    background-color: #111827;
}
</style>
""", unsafe_allow_html=True)

# =====================================================
# 3. 标题
# =====================================================
st.title("🌍 城市空气质量实时监控大屏")
st.markdown(
    "<h4 style='text-align:center;color:#94a3b8;'>"
    "Kafka + Flink + MySQL + Streamlit 实时分析平台"
    "</h4>",
    unsafe_allow_html=True
)


# =====================================================
# 4. MySQL读取 (保持缓存)
# =====================================================
@st.cache_data(ttl=60)
def load_data():
    conn = pymysql.connect(
        host="192.168.198.129",
        user="root",
        password="",
        database="air_quality"
    )

    aggregation = pd.read_sql("SELECT * FROM aggregation_6h LIMIT 100", conn)
    alert = pd.read_sql("SELECT * FROM alert_record LIMIT 100", conn)
    anomaly = pd.read_sql("SELECT * FROM anomaly_detection LIMIT 100", conn)
    trend = pd.read_sql("SELECT * FROM sliding_window_trend LIMIT 100", conn)
    source = pd.read_sql("SELECT * FROM source_contribution LIMIT 100", conn)
    weather = pd.read_sql("SELECT * FROM weather_correlation LIMIT 100", conn)

    conn.close()
    return aggregation, alert, anomaly, trend, source, weather


aggregation, alert, anomaly, trend, source, weather = load_data()

# =====================================================
# 5. session_state 初始化
# =====================================================
if "play" not in st.session_state:
    st.session_state.play = False

if "index" not in st.session_state:
    st.session_state.index = 5

max_rows = max(len(aggregation), len(alert), len(anomaly), len(trend), len(source), len(weather))

# =====================================================
# 6. 控制栏
# =====================================================
c1, c2, c3, c4 = st.columns([1, 1, 2, 2])

with c1:
    if st.button("▶ 开始", use_container_width=True):
        st.session_state.play = True
        st.rerun()

with c2:
    if st.button("⏸ 暂停", use_container_width=True):
        st.session_state.play = False
        st.rerun()

with c3:
    speed = st.slider("生长速度(ms)", 100, 2000, 400)

with c4:
    chart_height = st.slider("图表高度", 300, 700, 400)

# =====================================================
# 7. 当前帧数据切片
# =====================================================
idx = st.session_state.index

aggregation_show = aggregation.iloc[:idx]
alert_show = alert.iloc[:idx]
anomaly_show = anomaly.iloc[:idx]
trend_show = trend.iloc[:idx]
source_show = source.iloc[:idx]
weather_show = weather.iloc[:idx]

# =====================================================
# 8. 指标卡渲染
# =====================================================
m1, m2, m3, m4 = st.columns(4)

latest_co = round(aggregation_show.iloc[-1]["co_avg"], 2) if len(aggregation_show) > 0 else 0
max_co = round(aggregation_show["co_max"].max(), 2) if len(aggregation_show) > 0 else 0
alert_count = len(alert_show)
latest_level = alert_show.iloc[-1]["alert_level"] if len(alert_show) > 0 else "NORMAL"

m1.metric("当前CO浓度", f"{latest_co} mg/m³")
m2.metric("最大CO浓度", f"{max_co} mg/m³")
m3.metric("告警数量", alert_count)
m4.metric("最新告警级别", latest_level)


# =====================================================
# 9. ECharts 通用样式配置 (优化了底部间距给滑动条让位)
# =====================================================
def common_style(title):
    return {
        "backgroundColor": "#111827",
        "title": {
            "text": title,
            "left": "center",
            "top": 10,
            "textStyle": {"color": "white", "fontSize": 16}
        },
        "legend": {
            "top": 35,
            "textStyle": {"color": "white"}
        },
        "grid": {
            "left": "6%",
            "right": "6%",
            "bottom": "24%",  # 增加底部间距，防止时间标签和滚动条重叠
            "containLabel": True
        }
    }


# 复用型 DataZoom 配置：默认显示最后 30% 的数据，允许滑动与滚轮缩放
zoom_config = [
    {
        "type": "slider",  # 外部滑动条
        "show": True,
        "realtime": True,
        "start": 70,  # 从 70% 开始展示
        "end": 100,  # 展示到 100%（即最新数据）
        "bottom": 10,  # 距离图表底部间距
        "height": 20,  # 滑动条高度
        "textStyle": {"color": "#ddd"},
        "borderColor": "#374151",
        "fillerColor": "rgba(0, 245, 255, 0.15)",  # 选区半透明青色
        "handleStyle": {"color": "#00f5ff"}  # 两端手柄颜色
    },
    {
        "type": "inside",  # 内部滚轮/触控缩放
        "realtime": True,
        "start": 70,
        "end": 100
    }
]

# =====================================================
# 10. 页面图表渲染
# =====================================================
r1c1, r1c2 = st.columns(2)
r2c1, r2c2 = st.columns(2)
r3c1, r3c2 = st.columns(2)

# 图1：CO趋势 (加入滑动查看)
with r1c1:
    opt1 = common_style("1.CO浓度动态趋势")
    opt1.update({
        "tooltip": {"trigger": "axis"},
        "dataZoom": zoom_config,
        "xAxis": {"type": "category", "data": aggregation_show["measurement_time"].astype(str).tolist(),
                  "axisLabel": {"color": "#ddd"}},
        "yAxis": {"type": "value", "name": "mg/m³", "axisLabel": {"color": "#ddd"}},
        "series": [{"name": "CO浓度", "data": aggregation_show["co_avg"].tolist(), "type": "line", "smooth": True,
                    "lineStyle": {"color": "#00f5ff"}, "itemStyle": {"color": "#00f5ff"},
                    "areaStyle": {"opacity": 0.2}}]
    })
    st_echarts(options=opt1, height=f"{chart_height}px", key="chart_1")

# 图2：告警柱状图 (加入滑动查看)
with r1c2:
    opt2 = common_style("2.告警事件分析")
    opt2.update({
        "tooltip": {"trigger": "axis"},
        "dataZoom": zoom_config,
        "xAxis": {"type": "category", "data": alert_show["alert_time"].astype(str).tolist(),
                  "axisLabel": {"color": "#ddd"}},
        "yAxis": {"type": "value", "axisLabel": {"color": "#ddd"}},
        "series": [{"name": "告警值", "data": alert_show["alert_value"].tolist(), "type": "bar",
                    "itemStyle": {"color": "#ff4d4f"}}]
    })
    st_echarts(options=opt2, height=f"{chart_height}px", key="chart_2")

# 图3：散点图 (加入滑动查看)
with r2c1:
    opt3 = common_style("3.异常检测分析")
    opt3.update({
        "tooltip": {},
        "dataZoom": zoom_config,
        "xAxis": {"type": "category", "data": anomaly_show["detect_time"].astype(str).tolist(),
                  "axisLabel": {"show": False}},
        "yAxis": {"type": "value", "axisLabel": {"color": "#ddd"}},
        "series": [
            {"name": "异常值", "data": anomaly_show["anomaly_value"].tolist(), "type": "scatter", "symbolSize": 12,
             "itemStyle": {"color": "#facc15"}}]
    })
    st_echarts(options=opt3, height=f"{chart_height}px", key="chart_3")

# 图4：面积图 (加入滑动查看)
with r2c2:
    opt4 = common_style("4.滑动窗口趋势")
    opt4.update({
        "tooltip": {"trigger": "axis"},
        "dataZoom": zoom_config,
        "xAxis": {"type": "category", "data": trend_show["window_end"].astype(str).tolist(),
                  "axisLabel": {"color": "#ddd"}},
        "yAxis": {"type": "value", "axisLabel": {"color": "#ddd"}},
        "series": [{"name": "滑动均值", "data": trend_show["co_avg"].tolist(), "type": "line", "smooth": True,
                    "areaStyle": {"opacity": 0.25}, "lineStyle": {"color": "#22c55e"},
                    "itemStyle": {"color": "#22c55e"}}]
    })
    st_echarts(options=opt4, height=f"{chart_height}px", key="chart_4")

# 图5：雷达图 (空间固定，无需滑动条)
with r3c1:
    latest_ratio = float(source_show.iloc[-1]["co_no2_ratio"]) if len(source_show) > 0 else 0.5
    opt5 = common_style("5.污染源贡献分析")
    opt5.update({
        "radar": {
            "indicator": [{"name": "工业污染", "max": 1}, {"name": "机动车", "max": 1}, {"name": "气象影响", "max": 1},
                          {"name": "空气风险", "max": 1}, {"name": "CO/NO2", "max": 1}],
            "axisName": {"color": "white"}},
        "series": [{"type": "radar", "data": [
            {"value": [0.8, 0.7, 0.6, 0.9, latest_ratio], "areaStyle": {"opacity": 0.4},
             "itemStyle": {"color": "#a855f7"}}]}]
    })
    st_echarts(options=opt5, height=f"{chart_height}px", key="chart_5")

# 图6：仪表盘 (空间固定，无需滑动条)
with r3c2:
    gauge_value = round(weather_show.iloc[-1]["temp_co_corr"] * 100, 2) if len(weather_show) > 0 else 0
    opt6 = common_style("6.气象相关性")
    opt6.update({
        "series": [{"name": "相关系数", "type": "gauge", "min": -100, "max": 100, "progress": {"show": True},
                    "detail": {"valueAnimation": True, "color": "white", "fontSize": 18, "offsetCenter": [0, '70%']},
                    "data": [{"value": gauge_value, "name": "相关度%"}]}]
    })
    st_echarts(options=opt6, height=f"{chart_height}px", key="chart_6")

# =====================================================
# 11. 告警历史表格
# =====================================================
st.subheader("🚨 最新告警记录")
if len(alert_show) > 0:
    st.dataframe(alert_show.tail(10), use_container_width=True)

# =====================================================
# 12. 核心控制循环
# =====================================================
if st.session_state.play:
    if st.session_state.index < max_rows:
        st.session_state.index += 1
        time.sleep(speed / 1000)
        st.rerun()
    else:
        st.session_state.play = False
        st.success("🎉 实时历史数据回放生长完毕！")
        st.rerun()