import os
import yaml
import joblib
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery
from ml.predict_delay.features.build_features import FeatureProcessor


load_dotenv()
def load_config(config_path="ml/predict_delay/config/model_config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

class InferencePipeline:
    def __init__(self, config):
        self.config = config
        self.project_id = config['gcp']['project_id']
        self.dataset_id = config['gcp']['dataset_id']
        self.client = bigquery.Client(project=self.project_id)
        
        # Load model và preprocessor từ file artifact
        model_path = config['model']['save_path']
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Không tìm thấy model tại {model_path}.")
        
        print("Đang load model và preprocessor ...!")
        saved_objects = joblib.load(model_path)
        self.preprocessor = saved_objects['preprocessor']
        self.model = saved_objects['model']
        self.fp = FeatureProcessor(numerical_cols=config['data']['numerical_cols'])

    def fetch_new_data(self) -> pd.DataFrame:
        """Lấy các đơn hàng chưa có dự đoán từ BigQuery"""
        query = f"""
            SELECT 
                f.order_id,
                f.price, 
                f.freight_value, 
                p.product_weight_g, 
                p.product_volume,
                lc.latitude AS cust_lat, 
                lc.longitude AS cust_lon,
                ls.latitude AS seller_lat, 
                ls.longitude AS seller_lon
            FROM `{self.project_id}.{self.dataset_id}.fact_delivery` f
            JOIN `{self.project_id}.{self.dataset_id}.dim_product` p 
                ON f.product_id = p.product_id
            JOIN `{self.project_id}.{self.dataset_id}.dim_customer` c 
                ON f.customer_id = c.customer_id
            JOIN `{self.project_id}.{self.dataset_id}.dim_location` lc 
                ON c.customer_zip_code_prefix = lc.zip_code_prefix
            JOIN `{self.project_id}.{self.dataset_id}.dim_seller` s 
                ON f.seller_id = s.seller_id
            JOIN `{self.project_id}.{self.dataset_id}.dim_location` ls 
                ON s.seller_zip_code_prefix = ls.zip_code_prefix
            -- giả sử lấy các đơn hàng mới nhất chưa có ngày giao thực tế
            WHERE f.order_delivered_customer_date IS NULL
        """
        print("Đang lấy dữ liệu đơn hàng mới từ BigQuery...")
        return self.client.query(query).to_dataframe()

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Thực hiện dự đoán số ngày trễ"""
        if df.empty:
            print("Không có dữ liệu mới để dự đoán.")
            return df
            
        print("Đang xử lý đặc trưng và dự đoán ...!")
        # 1. Feature Engineering (tính khoảng cách)
        df_features = self.fp.add_custom_features(df)
        
        # 2. Preprocessing (Impute, Scale)
        X = self.preprocessor.transform(df_features)
        
        # 3. Predict số ngày trễ (Regression)
        predicted_delay_days = self.model.predict(X)
        
        # 4. Chuẩn bị dữ liệu output để ghi vào fact_delivery_prediction
        output_df = pd.DataFrame({
            'order_id': df['order_id'],
            # Do bảng fact_delivery_prediction yêu cầu predicted_is_delayed, 
            # map từ kết quả regression: > 0 ngày thì coi là trễ (1), ngược lại là (0)
            'predicted_is_delayed': (predicted_delay_days > 0).astype(float),
            # giả lập delay_probability dựa trên số ngày dự đoán (càng trễ lâu xác suất càng cao)
            'delay_probability': [min(max(delay / 10.0, 0.0), 1.0) for delay in predicted_delay_days],
            'predicted_delay_days': predicted_delay_days # Output gốc của mô hình Regression
        })
        return output_df

    def save_predictions_to_bq(self, predictions_df: pd.DataFrame):
        """Lưu kết quả dự đoán vào Data Warehouse"""
        if predictions_df.empty:
            return
            

        final_df = predictions_df[['order_id', 'predicted_is_delayed', 'delay_probability']]
        
        table_id = f"{self.project_id}.{self.dataset_id}.fact_delivery_prediction"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", # Thêm data vào bảng có sẵn
        )
        
        print(f"Đang ghi {len(final_df)} dòng kết quả vào {table_id} ...!")
        job = self.client.load_table_from_dataframe(final_df, table_id, job_config=job_config)
        job.result() 
        print("Hoàn tất ghi dữ liệu!")

def run_inference_pipeline():
    config = load_config()
    pipeline = InferencePipeline(config)
    
    new_data = pipeline.fetch_new_data()
    predictions = pipeline.predict(new_data)
    pipeline.save_predictions_to_bq(predictions)

if __name__ == "__main__":
    run_inference_pipeline()