import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas_gbq

# 1. Config
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")


# Kiểm tra biến môi trường
if not all([PROJECT_ID, DATASET_ID, KEY_PATH, AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET]):
    raise ValueError("Thiếu biến môi trường! Check lại!")

# Khởi tạo client kết nối BigQuery bằng service account
client = bigquery.Client.from_service_account_json(KEY_PATH)
# Cấu hình key aws 
storage_options = {
    "key": AWS_ACCESS_KEY,
    "secret": AWS_SECRET_KEY
}
print(f"Kết nối thành công tới BigQuery Project: {PROJECT_ID}")

# 2. LOAD

def load_table(table_name):
    print(f"Đang xử lý bảng: {table_name} ...")
    # B1. Đọc file parquet từ S3
    s3_path = f"s3://{S3_BUCKET}/curated/{table_name}/"
    print(f"==> Đọc dữ liệu từ: {s3_path}")
    df = pd.read_parquet(s3_path, storage_options=storage_options)
    # B2. Write truncate vào bigquery
    target_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    print(f"==> Đang đẩy {len(df):,} dòng vào BigQuery: {target_table_id}")

    pandas_gbq.to_gbq(
        df,
        target_table_id,
        project_id=PROJECT_ID,
        if_exists="replace",
        credentials=client._credentials
    )
    print(f"Đã nạp thành công bảng {table_name}")

# 3. MAIN
if __name__ == "__main__":
    curated_tables = [
        "dim_customer",
        "dim_seller",
        "dim_product",
        "dim_location",
        "fact_delivery"
    ]

    for table in curated_tables:
        try:
            load_table(table)
        except Exception as e:
            print(f"Lỗi khi nạp bảng: {table} -- {e}")

    print("DONE!")