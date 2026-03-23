import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="MSIDE Dashboard", layout="wide")

# ==============================
# LOAD DATA
# ==============================
@st.cache_data
def load_data():
    df = pd.read_csv("data/output/master_data.csv")
    daily = pd.read_csv("data/output/analytics/daily_revenue.csv")
    products = pd.read_csv("data/output/analytics/top_products.csv")
    sources = pd.read_csv("data/output/analytics/source_performance.csv")
    growth = pd.read_csv("data/output/analytics/growth.csv")

    return df, daily, products, sources, growth


df, daily, products, sources, growth = load_data()

# ==============================
# HEADER
# ==============================
st.title("📊 Marketplace Sales Dashboard (MSIDE)")
st.caption("Real-time Business Insight Engine")

# ==============================
# 🔥 KPI SECTION (MAHAL)
# ==============================
col1, col2, col3 = st.columns(3)

total_revenue = df["revenue"].sum()
total_orders = len(df)
avg_order = df["revenue"].mean()

col1.metric("💰 Total Revenue", f"{total_revenue:,.0f}")
col2.metric("🧾 Total Orders", total_orders)
col3.metric("📦 Avg Order Value", f"{avg_order:,.0f}")

# ==============================
# 🔥 DAILY TREND
# ==============================
st.subheader("📈 Revenue Trend")

fig = px.line(daily, x="date", y="revenue", title="Daily Revenue")
st.plotly_chart(fig, use_container_width=True)

# ==============================
# 🔥 TOP PRODUCTS
# ==============================
st.subheader("🏆 Top Products")

fig2 = px.bar(products, x="product", y="revenue", title="Top Products")
st.plotly_chart(fig2, use_container_width=True)

# ==============================
# 🔥 SOURCE PERFORMANCE
# ==============================
st.subheader("📊 Source Performance")

fig3 = px.pie(sources, names="source", values="revenue", title="Revenue by Source")
st.plotly_chart(fig3, use_container_width=True)

# ==============================
# 🔥 GROWTH ANALYSIS
# ==============================
st.subheader("📉 Growth Analysis")

fig4 = px.line(growth, x="date", y="growth", title="Revenue Growth %")
st.plotly_chart(fig4, use_container_width=True)

# ==============================
# 🔥 FILTER (LEVEL MAHAL)
# ==============================
st.sidebar.header("🔍 Filter")

selected_source = st.sidebar.multiselect(
    "Select Source",
    options=df["source"].unique(),
    default=df["source"].unique()
)

filtered_df = df[df["source"].isin(selected_source)]

st.subheader("📋 Filtered Data")
st.dataframe(filtered_df)