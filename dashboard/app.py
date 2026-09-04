import os
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# ==========================================
# 1. CẤU HÌNH KẾT NỐI
# ==========================================
load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ==========================================
# 2. HÀM TẢI DỮ LIỆU TỪ BIGQUERY
# @st.cache_data giúp lưu cache, không phải query lại nhiều lần tốn tiền
# ==========================================
@st.cache_data
def load_data():
    client = bigquery.Client.from_service_account_json(KEY_PATH)
    query = f"""
        SELECT 
            order_id, 
            is_delayed, 
            predicted_is_delayed, 
            delay_probability 
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_delivery_prediction`
    """
    return client.query(query).to_dataframe()

# ==========================================
# 3. THIẾT KẾ GIAO DIỆN STREAMLIT
# ==========================================
# Cài đặt trang rộng ra cho đẹp
st.set_page_config(page_title="Delivery Dashboard", layout="wide")

st.title("🚀 Real-Time Delivery Dashboard")
st.markdown("Hệ thống dự đoán nguy cơ trễ đơn hàng bằng Logistic Regression")

# Tải dữ liệu
with st.spinner("Đang tải dữ liệu từ BigQuery..."):
    df = load_data()

# --- KHU VỰC 1: CÁC THẺ KPI ---
st.subheader("📊 Bức tranh toàn cảnh")
col1, col2, col3, col4 = st.columns(4)

col1.metric("Tổng số đơn", f"{len(df):,}")
col2.metric("Đơn thực tế trễ", f"{df['is_delayed'].sum():,}")
col3.metric("AI Dự đoán trễ", f"{df['predicted_is_delayed'].sum():,}")
col4.metric("Xác suất trễ trung bình", f"{df['delay_probability'].mean() * 100:.2f}%")

st.divider() # Kẻ 1 đường ngang phân cách

# --- KHU VỰC 2: BẢNG TRA CỨU ĐƠN HÀNG ---
st.subheader("🔍 Tra cứu chi tiết đơn hàng")
search_id = st.text_input("Nhập mã đơn hàng (order_id) vào đây để kiểm tra:")

if search_id:
    # Lọc dataframe theo mã đơn
    result = df[df['order_id'] == search_id]
    if not result.empty:
        st.success("Đã tìm thấy đơn hàng!")
        st.dataframe(result, use_container_width=True)
    else:
        st.warning("Không tìm thấy đơn hàng này trong hệ thống!")
else:
    # Nếu không tìm kiếm, hiển thị 100 dòng đầu tiên
    st.markdown("*Hiển thị 100 đơn hàng gần nhất:*")
    st.dataframe(df.head(100), use_container_width=True)