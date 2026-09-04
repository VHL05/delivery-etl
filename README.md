# 🚚 Real-Time Delivery Data Platform & Delay Prediction

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-Data_Processing-orange.svg)
![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-Data_Warehouse-blue)
![Scikit-Learn](https://img.shields.io/badge/Scikit_Learn-Machine_Learning-yellow)
![Streamlit](https://img.shields.io/badge/Streamlit-Web_App-red)

## 📌 Project Overview

This project is an **end-to-end Data Engineering & Machine Learning pipeline** designed for an e-commerce logistics system.

It processes raw delivery data, transforms it into a structured **Star Schema** in a Data Warehouse, and utilizes a Machine Learning model to predict whether an upcoming order will be delayed.

The ultimate goal is to provide business operations with a **real-time monitoring dashboard** and **early warnings for high-risk deliveries**.

---

## 🏗️ Architecture & Tech Stack

The system is built with a scalable cloud-based architecture:

1. **Data Ingestion & Lake**
   Simulated event streaming stored in cloud storage (**S3 / Local simulation**).

2. **Data Processing (ETL)**
   **PySpark** is used to clean, transform, and structure raw delivery data.

3. **Data Warehouse**
   **Google BigQuery** serves as the analytical database, optimized with a **Star Schema** consisting of Fact and Dimension tables.

4. **Machine Learning Pipeline**
   **Scikit-Learn** (`LogisticRegression` + `StandardScaler`) is used to predict delivery delays.

   The model handles imbalanced data using:

   ```python
   class_weight="balanced"
   ```

   This helps prioritize the detection of high-risk delayed orders.

5. **BI Dashboard**
   A Streamlit web application pulls data from BigQuery for operational monitoring and KPI tracking.

---

## 🗂️ Project Structure

```
real-time-delivery-platform/
│
├── data/                   # Raw and processed data files (ignored in git)
├── ingestion/              # Data ingestion scripts
├── processing/             # PySpark ETL scripts (Module 4)
├── warehouse/              # BigQuery schema & load scripts (Module 5)
├── analytics/              # SQL queries for BI (Module 6)
├── ml/                     # ML training & prediction pipeline (Module 7)
├── dashboard/              # Streamlit web app (Module 8)
│
├── .env                    # Environment variables
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

---

## 🚀 Key Business Impacts

### 💰 Cost Optimization

Implemented a Full Replace load strategy with the Google Cloud Sandbox, reducing cloud infrastructure costs to $0 during the development phase.

### 🚚 Proactive Logistics

The Machine Learning model identifies potentially delayed deliveries based on features such as:

- Product volume
- Product weight
- Freight value
- Other delivery-related attributes

This allows the operations team to intervene proactively before delays occur.

---

## 💻 How to Run Locally

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/real-time-delivery-platform.git
cd real-time-delivery-platform
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup Google Cloud Credentials

Place your GCP Service Account JSON key in the root directory and configure the `.env` file:

```
GCP_PROJECT_ID=your-project-id
BQ_DATASET_ID=delivery_dw
GOOGLE_APPLICATION_CREDENTIALS=your-key.json
```

⚠️ **Important:** Do not commit your `.env` file or GCP Service Account JSON key to GitHub.

### 4. Run the Streamlit Dashboard

```bash
streamlit run dashboard/app.py
```

---

## 🔄 End-to-End Data Flow

```
Raw Delivery Events
        │
        ▼
┌─────────────────────┐
│   Data Ingestion     │
│  Kafka / Simulation  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│     Data Lake        │
│    S3 / Local        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      PySpark         │
│    ETL Processing    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Google BigQuery    │
│     Star Schema      │
└──────────┬──────────┘
           │
      ┌────┴────┐
      ▼         ▼
┌──────────┐ ┌──────────────┐
│ BI / SQL │ │ ML Prediction│
└────┬─────┘ └──────┬───────┘
     │              │
     └──────┬───────┘
            ▼
   ┌──────────────────┐
   │    Streamlit      │
   │  Operations       │
   │    Dashboard      │
   └──────────────────┘
```

---

## 📊 Main Modules

| Module   | Description             | Technology     |
|----------|--------------------------|-----------------|
| Module 1 | Data Source / Dataset    | CSV / JSON      |
| Module 2 | Event Generation         | Python           |
| Module 3 | Data Ingestion           | Kafka            |
| Module 4 | ETL & Data Processing    | PySpark          |
| Module 5 | Data Warehouse           | Google BigQuery  |
| Module 6 | Analytics & BI Queries   | SQL              |
| Module 7 | Delay Prediction         | Scikit-Learn     |
| Module 8 | Monitoring Dashboard     | Streamlit        |

---

## 🛠️ Technologies

- Python 3.9+
- Apache Kafka
- PySpark
- Amazon S3 / Local Storage
- Google BigQuery
- Scikit-Learn
- Pandas
- SQL
- Streamlit
- Google Cloud Platform

---

## 🎯 Project Goals

The project demonstrates practical knowledge of:

- Real-time data ingestion
- Data Lake architecture
- ETL/ELT pipelines
- Distributed data processing with PySpark
- Dimensional data modeling
- Star Schema design
- Cloud Data Warehousing
- SQL analytics
- Machine Learning for operational prediction
- Handling imbalanced classification problems
- Business intelligence dashboards
- End-to-end Data Engineering & Machine Learning architecture
