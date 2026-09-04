import os
import pandas as pd
import pandas_gbq
from dotenv import load_dotenv
from google.cloud import bigquery
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score


# CẤU HÌNH KẾT NỐI BIGQUERY

load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# khởi tạo client
client = bigquery.Client.from_service_account_json(KEY_PATH)

def run_ml_pipeline():
    # B1. Extract - Lấy dữ liệu từ warehouse
    print("Đang tải dữ liệu về từ BigQuery...!")
    query = f"""
        select
            f.order_id,
            f.price,
            f.freight_value,
            p.product_weight_g,
            p.product_volume,
            f.is_delayed
        from `{PROJECT_ID}.{DATASET_ID}.fact_delivery` f
        join `{PROJECT_ID}.{DATASET_ID}.dim_product` p
        on f.product_id = p.product_id
        where f.is_delayed is not null
    """
    df = client.query(query).to_dataframe()
    print(f"==> Đã tải thành công {len(df):,} dòng dữ liệu huấn luyện.")
    print(df.head())


    # B2. PRE-PROCESSING
    print("Đang làm sạch dữ liệu ...!")

    # 1. Khai báo đặc trưng
    features = ['price', 'freight_value', 'product_weight_g', 'product_volume']

    # 2. Loại bỏ các null/nan
    df_clean = df.dropna(subset=features).copy()

    # 3. Tách tập dữ liệu thành ma trận X(features) và vector y (target)
    X = df_clean[features]
    y = df_clean['is_delayed']

    print(f"==> Số dòng dữ liệu hợp lệ sau làm sạch: {len(df_clean):,}")


    # B3. TRAINING
    print("Đang training model...!")

    # 1. StandardScaler
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 2. Khởi tạo và train mô hình
    model = LogisticRegression(random_state=42, class_weight='balanced')
    model.fit(X_scaled, y)

    # 3. Dự đoán thử
    y_pred = model.predict(X_scaled)
    y_prob = model.predict_proba(X_scaled)[:, 1]

    # 4. Đánh giá
    acc = accuracy_score(y, y_pred)
    print(f" ==> Mô hình huấn luyện xong!")
    print(f" ==> Độ chính xác: {acc*100:.2f}%")


    # B4. ĐẨY KẾT QUẢ DỰ ĐOÁN VỀ KHO
    print("Đang đẩy kết quả dự đoán lên BigQuery...")

    # 1. Gắn kết quả dự đoán vào dataframe đã làm sạch
    df_clean['predicted_is_delayed'] = y_pred
    df_clean['delay_probability'] = y_prob

    # 2. Trích xuất các cột cho bảng Fact Prediction
    df_output = df_clean[['order_id', 'is_delayed', 'predicted_is_delayed', 'delay_probability']]

    target_table_id = f"{PROJECT_ID}.{DATASET_ID}.fact_delivery_prediction"
    print(f"==> Đang nạp dữ liệu vào bảng: {target_table_id}")

    # 3. Đẩy lên BigQuery
    pandas_gbq.to_gbq(
        df_output,
        target_table_id,
        project_id = PROJECT_ID,
        if_exists = "replace",
        credentials = client._credentials
    )

    print(f"DONE!")


if __name__ == "__main__":
    run_ml_pipeline()