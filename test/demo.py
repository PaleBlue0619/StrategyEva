import streamlit as st
import pandas as pd
import numpy as np

# ================= 页面配置 =================
st.set_page_config(layout="wide", page_title="数据分析看板")

# ================= 模拟后端数据 =================
# 1. 模拟返回的 DataFrame (最左列为 TradeTime，其余为指标列)
dates = pd.date_range('2024-01-01', periods=30, freq='D')
np.random.seed(42)
data_df = pd.DataFrame({
    'TradeTime': dates,
    'close': np.random.randn(30).cumsum() + 100,
    'volume': np.random.randint(1000, 5000, 30),
    'turnover': np.random.rand(30) * 2
})

# 2. 模拟返回的指标字典 (后端返回的原始数据)
backend_metrics = {
    "close": 101.25,
    "volume": 3421,
    "turnover": 1.56
}

# 3. 前端指标名称映射 (键 -> 显示名)
metric_name_mapping = {
    "close": "📈 最新收盘价",
    "volume": "📊 成交量 (手)",
    "turnover": "🔄 换手率 (%)"
}

# 4. 图表字段映射 (DataFrame列名 -> 图表显示名)
chart_field_mapping = {
    "close": "收盘价走势",
    "volume": "成交量走势",
    "turnover": "换手率走势"
}

# ================= 构建前端展示 =================
st.title("📉 金融数据分析看板")

# 创建左右两列布局 (比例: 左侧图表区占70%，右侧指标区占30%)
left_col, right_col = st.columns([7, 3], gap="medium")

# ----------------- 左侧: 绘制多个独立折线图 -----------------
with left_col:
    st.markdown("### 📈 趋势图表")

    # 获取所有需要绘制的指标列 (除了 TradeTime)
    chart_columns = [col for col in data_df.columns if col != 'TradeTime']

    # 为每个指标列独立绘制一个折线图
    for col in chart_columns:
        # 准备该指标的数据 (设置 TradeTime 为索引)
        chart_data = data_df.set_index('TradeTime')[[col]]

        # 获取图表显示名称，若无映射则使用原列名
        display_name = chart_field_mapping.get(col, col)

        # 使用 st.line_chart 绘制 (简单高效)
        st.markdown(f"**{display_name}**")
        st.line_chart(chart_data, use_container_width=True)
        st.divider()  # 添加分隔线，区分不同图表

# ----------------- 右侧: 展示指标卡片 -----------------
with right_col:
    st.markdown("### 🎯 核心指标")

    # 遍历后端返回的指标字典，进行前端映射并展示
    for key, value in backend_metrics.items():
        # 获取前端显示名称，若无映射则使用原键名
        display_name = metric_name_mapping.get(key, key)

        # 格式化数值显示 (根据指标类型添加单位)
        if key == "turnover":
            formatted_value = f"{value:.2f}%"
        elif key == "volume":
            formatted_value = f"{value:,.0f}"
        else:
            formatted_value = f"{value:.2f}"

        # 使用 st.metric 展示指标卡片
        st.metric(
            label=display_name,
            value=formatted_value,
            delta=None,  # 可以传入增长率，例如 "1.2%"
            help=f"后端原始键名: {key}"
        )

    # 可选：添加一些额外的说明信息
    with st.expander("ℹ️ 数据说明"):
        st.caption("""
        - **收盘价**: 当日最终成交价格
        - **成交量**: 当日成交总手数
        - **换手率**: 当日转手买卖的频率
        """)