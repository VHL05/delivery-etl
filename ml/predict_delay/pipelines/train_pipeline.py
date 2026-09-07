import yaml
import os
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from ml.predict_delay.data.bq_client import BigQueryDataLoader
from ml.predict_delay.features.build_features import FeatureProcessor
from ml.predict_delay.models.train import DelayPredictionModel
from ml.predict_delay.models.evaluate import evaluate_classification

load_dotenv()
def load_config(config_path = "ml/predict_delay/config/model_config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def run_training_pipeline():
    # 1. load cáu hình
    config = load_config()

    # 2. lấy dữ liệu
    bq_loader = BigQueryDataLoader(
        project_id = config['gcp']['project_id'],
        dataset_id = config['gcp']['dataset_id']
    )
    df = bq_loader.fetch_training_data()

    # 3. Feature engineering
    fp = FeatureProcessor(numerical_cols=config['data']['numerical_cols'])
    df = fp.add_custom_features(df)

    X = df.drop(columns=[config['data']['target_col']])
    y = df[config['data']['target_col']]
    # chia tập train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=config['data']['test_size'],
        random_state=config['data']['random_state']
    )

    # build và fit preprocessor pipeline
    preprocessor = fp.build_pipeline()
    X_train_processed = preprocessor.fit_transform(X_train)
    X_test_processed = preprocessor.transform(X_test)

    # 4. Train model
    model = DelayPredictionModel(model_params=config['model']['params'])
    model.train(X_train_processed, y_train)

    # 5. Evaluate
    y_pred = model.predict(X_test_processed)
    y_prob = model.predict_proba(X_test_processed)
    evaluate_classification(y_test, y_pred, y_prob)

    # 6. Save model
    os.makedirs(os.path.dirname(config['model']['save_path']), exist_ok=True)
    model.save_model(preprocessor, config['model']['save_path'])


if __name__ == "__main__":
    run_training_pipeline()