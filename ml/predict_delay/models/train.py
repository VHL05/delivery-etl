from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
import joblib
import numpy as np # Thêm thư viện numpy

class DelayPredictionModel:
    def __init__(self, model_params: dict = None):
        self.model = XGBClassifier(random_state=42, eval_metric='logloss')

    def train(self, X_train, y_train):
        print("Đang tìm kiếm tham số tối ưu (GridSearch) cho Classification...")
        
        # Tự động tính toán tỷ lệ mất cân bằng (Class 0 / Class 1)
        ratio = float(np.sum(y_train == 0)) / np.sum(y_train == 1)
        print(f"Tỷ lệ mất cân bằng dữ liệu đang là: {ratio:.2f}")

        param_grid = {
            'n_estimators': [100, 200, 300],
            'learning_rate': [0.01, 0.05, 0.1],
            'max_depth': [5, 7, 9],
            # Cung cấp trọng số xung quanh tỷ lệ thực tế để model phạt nặng khi đoán sai class 1
            'scale_pos_weight': [ratio * 0.8, ratio, ratio * 1.2] 
        }
        
        grid_search = GridSearchCV(
            estimator=self.model, 
            param_grid=param_grid, 
            scoring='roc_auc', 
            cv=3, 
            verbose=1,
            n_jobs=-1
        )
        
        grid_search.fit(X_train, y_train)
        print(f"Tham số tốt nhất: {grid_search.best_params_}")
        self.model = grid_search.best_estimator_

    def predict(self, X):
        return self.model.predict(X)
        
    def predict_proba(self, X):
        return self.model.predict_proba(X)[:, 1]

    def save_model(self, pipeline, path: str):
        full_pipeline = {'preprocessor': pipeline, 'model': self.model}
        joblib.dump(full_pipeline, path)
        print(f"Mô hình đã được lưu tại {path}")