import json
import joblib
import numpy as np


def sigmoid(x, scale=1.0):
    return 1 / (1 + np.exp(-x / scale))


def load_artifacts(cfg):
    model = joblib.load(cfg["paths"]["model_file"])
    preprocessor = joblib.load(cfg["paths"]["preprocessor_file"])
    try:
        with open(cfg["paths"]["metadata_file"]) as f:
            metadata = json.load(f)
    except FileNotFoundError:
        metadata = {}
    return model, preprocessor, metadata


def predict(df, cfg):
    numeric_cols = cfg["features"]["numeric"]
    categorical_cols = cfg["features"]["categorical"]

    model, preprocessor, metadata = load_artifacts(cfg)
    X = df[numeric_cols + categorical_cols]
    X_t = preprocessor.transform(X)

    predicted_delay_days = model.predict(X_t)
    predicted_is_delayed = (predicted_delay_days > 0).astype(int)

    scale = metadata.get("residual_std", 2.0)
    delay_probability = sigmoid(predicted_delay_days, scale=scale)

    result = df[["order_id"]].copy()
    result["is_delayed"] = None  # chưa có nhãn thật tại thời điểm dự đoán
    result["predicted_is_delayed"] = predicted_is_delayed
    result["delay_probability"] = delay_probability
    return result