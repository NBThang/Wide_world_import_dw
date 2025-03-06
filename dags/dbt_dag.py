import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping


# Cấu hình profile cho dbt trên BigQuery
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",  # Kết nối Airflow tới BigQuery
        profile_args={"project": "learn-build-dw", "dataset": "wide_world_importers_dwh"},
    )
)

# Tạo DAG chạy dbt trên BigQuery
dbt_bigquery_dag = DbtDag(
    dag_id="dbt_bigquery_dag",
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/wide_world_import_pipeline"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    operator_args={"install_deps": True},
)
