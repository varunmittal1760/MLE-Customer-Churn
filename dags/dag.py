from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag',
    default_args=default_args,
    description='data pipeline run once a month',
    schedule_interval='0 0 1 * *',  # At 00:00 on day-of-month 1
    start_date=datetime(2024, 4, 1),
    end_date=datetime(2024, 7, 1),
    catchup=True,
) as dag:

    # data pipeline

    # --- label store ---
    dep_check_source_label_data = DummyOperator(task_id="dep_check_source_label_data")

    bronze_label_store = BashOperator(
        task_id='run_bronze_label',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 bronze_label.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    silver_label_store = BashOperator(
        task_id='run_silver_label',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 silver_label.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    gold_label_store =  BashOperator(
        task_id='run_gold_label_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 gold_label_store.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    label_store_completed = DummyOperator(task_id="label_store_completed")

    # Define task dependencies to run scripts sequentially
    dep_check_source_label_data >> bronze_label_store >> silver_label_store >> gold_label_store >> label_store_completed
 
 
    # --- feature store ---
    dep_check_source_data_bronze_feature = DummyOperator(task_id="dep_check_source_data_bronze_feature")

    dep_check_source_data_bronze_meta = DummyOperator(task_id="dep_check_source_data_bronze_meta")


    bronze_feature = BashOperator(
        task_id='run_bronze_feature',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 bronze_feature.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )
    
    bronze_meta = BashOperator(
        task_id='run_bronze_meta',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 bronze_meta.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    silver_feature = BashOperator(
        task_id='run_silver_feature',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 silver_feature.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )
    
    silver_meta = BashOperator(
        task_id='run_silver_meta',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 silver_meta.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )


    gold_feature_store = BashOperator(
        task_id='run_gold_feature_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 gold_feature_store.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    feature_store_completed = DummyOperator(task_id="feature_store_completed")
    
    # Define task dependencies to run scripts sequentially
    dep_check_source_data_bronze_feature >> bronze_feature >> silver_feature >> gold_feature_store
    dep_check_source_data_bronze_meta >> bronze_meta >> silver_meta >> gold_feature_store
    gold_feature_store >> feature_store_completed


    # --- model inference ---
    model_inference_start = DummyOperator(task_id="model_inference_start")

    model_1_inference = DummyOperator(task_id="model_1_inference")

    model_2_inference = DummyOperator(task_id="model_2_inference")

    model_inference_completed = DummyOperator(task_id="model_inference_completed")
    
    # Define task dependencies to run scripts sequentially
    feature_store_completed >> model_inference_start
    model_inference_start >> model_1_inference >> model_inference_completed
    model_inference_start >> model_2_inference >> model_inference_completed


    # --- model monitoring ---
    model_monitor_start = DummyOperator(task_id="model_monitor_start")

    model_1_monitor = DummyOperator(task_id="model_1_monitor")

    model_2_monitor = DummyOperator(task_id="model_2_monitor")

    model_monitor_completed = DummyOperator(task_id="model_monitor_completed")
    
    # Define task dependencies to run scripts sequentially
    model_inference_completed >> model_monitor_start
    model_monitor_start >> model_1_monitor >> model_monitor_completed
    model_monitor_start >> model_2_monitor >> model_monitor_completed


    # --- model auto training ---

    model_automl_start = DummyOperator(task_id="model_automl_start")
    
    model_1_automl = DummyOperator(task_id="model_1_automl")

    model_2_automl = DummyOperator(task_id="model_2_automl")

    model_automl_completed = DummyOperator(task_id="model_automl_completed")
    
    # Define task dependencies to run scripts sequentially
    feature_store_completed >> model_automl_start
    label_store_completed >> model_automl_start
    model_automl_start >> model_1_automl >> model_automl_completed
    model_automl_start >> model_2_automl >> model_automl_completed