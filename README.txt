# Telco Customer Churn Prediction 📞📉

## Project Overview
A machine learning pipeline to predict customer churn in the telecom industry, enabling targeted retention strategies.

## Key Features ✨
- **ETL Pipeline**: Medallion Architecture (Bronze → Silver → Gold)
- **High Accuracy**: XGBoost model achieves 95% prediction accuracy
- **Automation**: Monthly batch processing via Airflow DAG
- **Monitoring**: Tracks data drift using PSI scores
- **Deployment**: Blue-green deployment strategy

## Dataset 📊
| Property       | Value                          |
|----------------|--------------------------------|
| Source         | Kaggle Telco Customer Churn    |
| Time Period    | Jan 2024 - Jul 2024            |
| Total Records  | 26,067 (7,443 unique customers)|
| Format         | CSV → Processed Parquet        |

## Technical Stack 💻
```mermaid
graph LR
    A[Python] --> B[scikit-learn]
    A --> C[XGBoost]
    D[PySpark] --> E[ETL]
    F[Airflow] --> G[Orchestration]
    H[Docker] --> I[Containerization]
```

## Pipeline Components ⚙️
### 1. Data Processing
- **Bronze Layer**: Raw partitioned CSVs
- **Silver Layer**: Cleaned/validated Parquet files
- **Gold Layer**: Joined feature/label stores

### 2. Machine Learning
- Feature engineering (tenure groups)
- Model comparison (Logistic Regression vs XGBoost)
- Stratified 70-30 train-test split

### 3. Deployment
- Monthly batch predictions
- Model versioning (.pkl files)
- Blue-green deployment

## Usage 🚀
```bash
# Run ETL pipeline
python scripts/data_processing.py

# Train model
python scripts/train_model.py

# Deploy DAG
airflow dags trigger churn_prediction
```

## Model Governance 🔍
| Metric          | Threshold | Action                          |
|-----------------|-----------|---------------------------------|
| ROC AUC         | < 0.65    | Trigger model retraining        |
| PSI Score       | > 0.25    | Investigate data drift          |

