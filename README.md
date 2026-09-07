# Real-Time Delivery Data Platform & Delay Prediction

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Processing-orange.svg)
![Google BigQuery](https://img.shields.io/badge/Google%20BigQuery-Data%20Warehouse-blue.svg)
![XGBoost](https://img.shields.io/badge/XGBoost-Machine%20Learning-green.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red.svg)

## Project Overview

This project is an end-to-end data engineering and machine learning platform for monitoring e-commerce delivery operations and predicting potential delivery delays.

The system simulates a logistics data pipeline in which delivery events are ingested, processed, transformed into an analytical data warehouse, and used to generate machine learning predictions.

The main objective is to provide operational teams with structured delivery data, predictive insights, and early warnings for potentially delayed orders.

The project covers the complete workflow:

```text
Data Ingestion
      |
      v
Kafka
      |
      v
PySpark Processing
      |
      v
BigQuery Data Warehouse
      |
      +----------------------+
      |                      |
      v                      v
Feature Engineering     Analytics / BI
      |                      |
      v                      v
XGBoost Model           Streamlit Dashboard
      |
      v
Delay Prediction
      |
      v
BigQuery Prediction Table
```

## Objectives

The project was developed with the following objectives:

* Build an end-to-end data pipeline for delivery events.
* Process and transform raw logistics data using PySpark.
* Design a Star Schema data warehouse in Google BigQuery.
* Engineer features relevant to delivery delay prediction.
* Train a machine learning classification model using XGBoost.
* Handle class imbalance during model training.
* Store prediction results back into BigQuery.
* Provide an operational dashboard using Streamlit.
* Separate data processing, feature engineering, model training, and inference into modular components.

## System Architecture

### 1. Data Ingestion

Raw delivery events are simulated and ingested through Apache Kafka.

Kafka is used as the event ingestion layer to represent a logistics environment where delivery information can arrive continuously.

The ingestion layer is responsible for:

* Producing delivery events.
* Sending events to Kafka topics.
* Providing structured input for downstream processing.

### 2. Data Processing

PySpark is used to process and transform incoming delivery data.

The processing layer performs tasks such as:

* Data cleaning.
* Data type conversion.
* Missing value handling.
* Data transformation.
* Delivery-related calculations.
* Preparation of analytical datasets.

The processed data is then loaded into Google BigQuery.

### 3. Data Warehouse

The project uses Google BigQuery as the analytical data warehouse.

The warehouse follows a Star Schema design consisting of fact and dimension tables.

The dimensional model separates transactional delivery information from descriptive entities such as:

* Customers.
* Sellers.
* Products.
* Orders.
* Dates.
* Geographic information.

This structure allows analytical queries to be performed efficiently while keeping the data model organized and extensible.

### 4. Feature Engineering

The machine learning pipeline extracts predictive features from the warehouse data.

Examples include:

* Delivery distance.
* Geographic coordinates.
* Order-related attributes.
* Seller-related attributes.
* Customer-related attributes.
* Time-based features.
* Delivery history and operational characteristics.

Geographical distance is calculated using the Haversine formula to estimate the distance between relevant locations.

The feature engineering layer is implemented separately from model training to make the prediction pipeline easier to maintain and extend.

### 5. Machine Learning

The project uses XGBoost through `XGBClassifier` for binary delivery delay classification.

The model predicts whether a delivery is likely to be delayed.

The training pipeline includes:

* Data loading from BigQuery.
* Feature preparation.
* Missing value handling.
* Train/test splitting.
* Class imbalance handling.
* Model training.
* Model evaluation.
* Model serialization.

Because delayed deliveries represent a minority class, the training process uses `scale_pos_weight` based on the class distribution to give greater importance to the minority class.

The trained model is saved as:

```text
artifacts/delay_classifier.pkl
```

## Model Performance

The current model was evaluated using several classification metrics:

| Metric    |  Score |
| --------- | -----: |
| Accuracy  | 75.62% |
| Precision | 14.49% |
| Recall    | 56.37% |
| F1-Score  | 23.05% |
| ROC-AUC   | 72.56% |

The ROC-AUC of 0.7256 indicates that the model has a reasonable ability to distinguish between delayed and on-time deliveries.

The relatively low precision indicates that the model produces a significant number of false positive delay predictions. This is an important consideration for an operational alerting system because excessive false alarms can reduce the usefulness of the predictions.

The current model therefore prioritizes identifying potential delayed deliveries rather than maximizing precision.

Future iterations can further improve the precision-recall trade-off through decision-threshold tuning, feature engineering, and model optimization.

## Prediction Pipeline

The inference pipeline loads the trained model and generates predictions for new delivery records.

```text
BigQuery
   |
   v
New Delivery Data
   |
   v
Feature Engineering
   |
   v
Trained XGBoost Model
   |
   v
Delay Probability
   |
   v
Delay Classification
   |
   v
BigQuery Prediction Table
```

Prediction results are written to the following BigQuery table:

```text
delivery_dw.fact_delivery_prediction
```

This allows downstream analytics and dashboard applications to consume the latest prediction results.

## Dashboard

The Streamlit dashboard provides an operational view of delivery performance and model predictions.

The dashboard can be used to monitor:

* Delivery KPIs.
* Delivery status.
* Predicted delays.
* High-risk deliveries.
* Prediction statistics.
* Operational trends.

The dashboard retrieves prediction and analytical data from BigQuery, providing a centralized interface for monitoring delivery operations.

## Project Structure

```text
real-time-delivery-platform/
|
├── analytics/
│   └── SQL queries for analytics and BI
|
├── dashboard/
│   └── Streamlit application
|
├── ingestion/
│   └── Kafka data ingestion scripts
|
├── ml/
│   └── predict_delay/
│       ├── config/
│       │   └── YAML configuration files
│       │
│       ├── data/
│       │   └── BigQuery data loaders
│       │
│       ├── features/
│       │   └── Feature engineering
│       │
│       ├── models/
│       │   └── XGBoost training and evaluation
│       │
│       └── pipelines/
│           ├── train_pipeline.py
│           └── inference_pipeline.py
|
├── processing/
│   └── PySpark ETL scripts
|
├── warehouse/
│   └── BigQuery schema configurations
|
├── artifacts/
│   └── Trained machine learning models
|
├── .gitignore
├── requirements.txt
└── README.md
```

## Technology Stack

### Data Engineering

* Python
* Apache Kafka
* PySpark
* Google BigQuery

### Data Warehouse

* BigQuery
* Star Schema
* Fact and Dimension tables
* SQL

### Machine Learning

* Scikit-learn
* XGBoost
* Pandas
* NumPy
* Haversine distance
* Binary classification

### Application

* Streamlit

### Configuration and Environment

* YAML
* Python environment variables
* Google Cloud service account authentication

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/real-time-delivery-platform.git

cd real-time-delivery-platform
```

### 2. Create a Virtual Environment

Windows:

```bash
python -m venv venv

venv\Scripts\activate
```

Linux / macOS:

```bash
python3 -m venv venv

source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```env
GCP_PROJECT_ID=your-project-id
BQ_DATASET_ID=delivery_dw
GOOGLE_APPLICATION_CREDENTIALS=gcp-service-account.json
```

Make sure the credentials file and `.env` are included in `.gitignore`.

Do not commit Google Cloud credentials or other sensitive configuration files to the repository.

### 5. Run the Training Pipeline

```bash
python -m ml.predict_delay.pipelines.train_pipeline
```

The pipeline loads training data, performs feature engineering, trains the XGBoost classifier, evaluates the model, and saves the trained model.

The trained model is saved to:

```text
artifacts/delay_classifier.pkl
```

### 6. Run the Inference Pipeline

```bash
python -m ml.predict_delay.pipelines.inference_pipeline
```

The inference pipeline generates predictions for new delivery data and writes the results to BigQuery.

### 7. Run the Dashboard

```bash
streamlit run dashboard/app.py
```

The exact dashboard entry point may vary depending on the project configuration.

## Data Warehouse Design

The warehouse follows a dimensional modeling approach.

A simplified representation is:

```text
                    dim_customer
                         |
                         |
dim_date ---- fact_delivery ---- dim_seller
                         |
                         |
                    dim_product
                         |
                         |
                    dim_order
```

The fact table contains measurable delivery and operational information, while dimension tables provide descriptive attributes used for analysis.

This design supports analytical queries such as:

* Delivery performance by date.
* Delivery performance by customer location.
* Delivery performance by seller.
* Delivery performance by product.
* Delay rates across different operational segments.

## Machine Learning Considerations

### Class Imbalance

Delivery delays are typically less frequent than successful on-time deliveries.

To address this imbalance, the training pipeline calculates the class distribution and uses `scale_pos_weight` in XGBoost.

This allows the model to assign greater importance to the minority delay class during training.

### Evaluation Strategy

Accuracy alone is not sufficient for evaluating this problem because it can be misleading when the target classes are imbalanced.

The project therefore evaluates the model using:

* Precision
* Recall
* F1-Score
* ROC-AUC
* Accuracy

Recall is particularly relevant because missing a genuinely delayed delivery may prevent the operations team from taking proactive action.

Precision is also monitored because excessive false positives can lead to unnecessary operational interventions.

## Current Limitations

The current version has several areas that can be improved:

1. The model's precision is relatively low, resulting in a high number of false positive delay predictions.

2. The current decision threshold can be further optimized to achieve a better precision-recall trade-off.

3. Additional delivery-specific features could improve predictive performance.

4. Model calibration and probability analysis can be introduced to make predicted delay probabilities more reliable.

5. More systematic monitoring of model performance over time could be added.

6. The streaming architecture can be further developed toward a fully production-oriented real-time inference workflow.

## Future Improvements

Potential improvements include:

* Decision threshold optimization.
* Precision-Recall curve analysis.
* PR-AUC evaluation.
* Hyperparameter optimization.
* Additional temporal and geographic features.
* Feature importance and SHAP analysis.
* Probability calibration.
* Model monitoring.
* Data quality monitoring.
* Automated model retraining.
* Containerization with Docker.
* CI/CD for data and ML pipelines.
* Production-grade streaming inference.
* Alerting for high-risk deliveries.

## Key Learning Outcomes

This project demonstrates practical experience across both data engineering and machine learning workflows.

The main technical outcomes include:

* Designing an end-to-end data pipeline.
* Working with event-based data ingestion.
* Processing data with PySpark.
* Designing a Star Schema data warehouse.
* Querying and managing analytical data in BigQuery.
* Building reusable feature engineering components.
* Training and evaluating a machine learning classification model.
* Handling imbalanced classification problems.
* Separating training and inference pipelines.
* Persisting machine learning predictions in a data warehouse.
* Building a data-driven operational dashboard.
