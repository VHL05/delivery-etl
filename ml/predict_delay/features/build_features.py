import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer

def distance(lat1, lon1, lat2, lon2):
    R = 6371.0 # bán kính trái đất tính bằng km

    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

class FeatureProcessor:
    def __init__(self, numerical_cols):
        self.numerical_cols = numerical_cols
        self.preprocessor = None

    def add_custom_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Thêm các đặc trưng tự tính toán"""
        df_copy = df.copy()
        
        # 1. Khoảng cách địa lý
        df_copy['distance_km'] = distance(
            df_copy['cust_lat'], df_copy['cust_lon'],
            df_copy['seller_lat'], df_copy['seller_lon']
        )
        
        # 2. Đặc trưng giao hàng liên tỉnh (1: Khác bang/tỉnh, 0: Cùng bang/tỉnh)
        df_copy['is_cross_state'] = (df_copy['customer_state'] != df_copy['seller_state']).astype(float)
        
        # 3. Tỷ lệ phí ship / giá sản phẩm (Cộng 1e-5 để tránh lỗi chia cho 0)
        df_copy['freight_ratio'] = df_copy['freight_value'] / (df_copy['price'] + 1e-5)
        
        return df_copy

    def build_pipeline(self):
        numeric_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ])
        self.preprocessor = ColumnTransformer(
            transformers = [
                ('num', numeric_transformer, self.numerical_cols)
            ],
            remainder = 'drop'
        )
        return self.preprocessor