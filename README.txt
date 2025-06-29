# Telco Customer Churn Prediction Project

## Overview
This project aims to predict customer churn in the telecom industry using machine learning. The goal is to identify at-risk customers early, enabling targeted retention strategies to reduce revenue loss and improve customer satisfaction.

## Business Problem
- Telecom industry churn rates range between 15-25% annually
- Customer churn leads to significant revenue loss and increased marketing costs
- Predictive modeling helps uncover patterns and design targeted retention campaigns

## Dataset
**Telco Customer Churn Dataset** from Kaggle:
- Time period: 01/04/2024 to 01/07/2024
- 26,067 records, 7,443 unique customers
- 21 features (3 continuous, 18 categorical)
- Binary target variable: Churn (Yes/No)

## Key Findings from EDA
- Longer tenure and contracts reduce churn likelihood
- Lack of services (online security, tech support) increases churn
- Higher monthly charges correlate with higher churn
- Demographic features show minimal predictive power

## Technical Implementation

### Data Pipeline (Medallion Architecture)
1. **Bronze Layer**: Raw CSV files partitioned by month
2. **Silver Layer**: 
   - Data cleaning and validation
   - Datatype enforcement
   - Conversion to Parquet format
3. **Gold Layer**: 
   - Feature and label tables joined
   - Stored in feature_store and label_store

### Machine Learning
- **Models**: Logistic Regression and XGBoost
- **Feature Engineering**: Created tenure groups via custom binning
- **Evaluation**: ROC AUC, classification reports
- **Top Features**: Tenure, contract type, service subscriptions

### Deployment
- **Airflow DAG** for monthly batch processing
- **Model Monitoring**: 
  - Track PSI scores for data drift
  - Monitor AUC performance
  - Blue-green deployment strategy

## Repository Structure
```
├── data/
│   ├── bronze/         # Raw partitioned CSV files
│   ├── silver/         # Cleaned Parquet files
│   └── gold/           # Processed feature and label stores
├── models/             # Serialized model artifacts
├── notebooks/          # Jupyter notebooks for EDA and training
├── scripts/            # Python modules for pipeline
├── airflow/            # DAG definitions for orchestration
└── docs/               # Documentation and presentations
```

## Getting Started
1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run the Airflow pipeline: `airflow dags trigger telco_churn_pipeline`

