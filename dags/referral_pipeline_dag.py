from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="referral_data_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["referral", "data-engineering"],
) as dag:
     
    set_permissions = BashOperator(
        task_id="set_permissions",
        bash_command="""
            chmod -R 777 /opt/airflow/output
        """
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/src/transform.py",
    )

  

    set_permissions >> transform 
