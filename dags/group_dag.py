from airflow import DAG
from airflow.operators.bash import BashOperator
# import subdag operator for working with additional dags
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_downloads import subdag_downloads
from subdags.subdag_transforms import subdag_transforms

from datetime import datetime

with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
    # add args with schedule of working with subdag
    args = {
        'start_date': dag.start_date,
        'schedule_interval': dag.schedule_interval,
        'catchup': dag.catchup
    }

    downloads = SubDagOperator(
        task_id='downloads',
        subdag=subdag_downloads(dag.dag_id, 'downloads', args)
    )
    
    transforms = SubDagOperator(
        task_id='transforms',
        subdag=subdag_transforms(dag.dag_id, 'transforms', args)
    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    downloads >> check_files >> transforms