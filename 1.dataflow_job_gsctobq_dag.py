from airflow import models
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

from datetime import timedelta

# Constants
PROJECT_ID = "rare-tome-458105-n0"
LOCATION = "us-central1"
GCS_TEMPLATE_PATH = "gs://my-flex-template-bucket2/beam_python_script.json"

with models.DAG(
    dag_id="dataflow_flex_template_input_output",
    schedule_interval=None,  # Trigger manually or set cron
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["example", "dataflow", "flex-template"],
) as dag:

    start_flex_template_job = DataflowStartFlexTemplateOperator(
        task_id="run_dataflow_flex_template",
        project_id=PROJECT_ID,
        location=LOCATION,
        body={
            "launchParameter": {
                "jobName": "flex-job-{{ ds_nodash }}",
                "containerSpecGcsPath": GCS_TEMPLATE_PATH,
                "parameters": {
                    "input": "gs://my-flex-template-bucket2/emp8_data.csv",
                    "output": "rare-tome-458105-n0:dataflow.emp_stage_table_2",
                },
            }
        },
    )
