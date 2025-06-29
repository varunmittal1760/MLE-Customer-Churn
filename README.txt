# Telco Customer Churn Prediction

## Project Overview
Machine learning pipeline to predict customer churn in telecom industry. Identifies at-risk customers for targeted retention strategies.

## Key Features
- Medallion Architecture ETL pipeline (Bronze→Silver→Gold)
- XGBoost model with 95% prediction accuracy
- Monthly batch processing via Airflow DAG
- Model monitoring for data drift (PSI scores)

## Dataset
- Source: Kaggle Telco Customer Churn
- Period: Jan-Apr 2024 to Jul 2024
- Records: 26,067 (7,443 unique customers)
- Format: CSV → Parquet processed

## Technical Stack
- Python (scikit-learn, XGBoost)
- PySpark for ETL
- Airflow for orchestration
- Docker for containerization

## Pipeline Components
1. Data Processing:
   - Bronze: Raw partitioned CSVs
   - Silver: Cleaned/validated Parquet
   - Gold: Joined feature/label stores

2. ML Development:
   - Feature engineering (tenure groups)
   - Logistic Regression + XGBoost
   - Stratified 70-30 train-test split

3. Deployment:
   - Monthly batch predictions
   - Model versioning (.pkl)
   - Blue-green deployment

## Usage
1. Run ETL pipeline:
   `python scripts/data_processing.py`

2. Train model: 
   `python scripts/train_model.py`

3. Deploy DAG:
   `airflow dags trigger churn_prediction`

## Model Governance
- Retrain monthly
- Alert if ROC_AUC < 0.65
- Investigate PSI > 0.25

## Contributors
[Your Name/Team]

## License
MIT
