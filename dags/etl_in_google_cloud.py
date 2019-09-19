from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

import logging
log = logging.getLogger(__name__)


default_args = {
    'owner': 'developer',
    'depends_on_past': True,
    'start_date': datetime(2014, 12, 3),
    'path_name': '',
    'pattern_name': 'restaurant_data*json',
    'download_url': '',
    'email': ['developer@gmail.com'],
    'google_project_id': '',
    'google_bucket_name': '',
    'google_sub_path': '',
    'email_on_failure': False,
    'email_on_retry': False,
    'spark_job': 'pyspark gs://gcp_stoage/script/Load_Star_Schema_Restaurant.py',
    'region_name': 'us-east1',
    'cluster_name' : 'dataproc-pipeline',
    'data_path' : 'gs://gcp_storage/data/in_gcp',
    'data_path_bq' : 'gs://gcp_storage/data/to_bq'
    }

def process_file_in_gcp(**context):
    execution_date = context['execution_date']
    log.info('execution_date: ' + str(execution_date))


dag = DAG('opendata_process_file_in_cloud', default_args=default_args,schedule_interval="@daily")


t1 = BashOperator(
    task_id='Timeout - Process Files',
    bash_command='sleep .5',
    dag=dag)


t2 = BashOperator(
    task_id='Run Spark Job',
    bash_command='gcloud dataproc jobs submit ' + default_args['spark_job'] + ' --region=' + default_args['region_name'] + ' --cluster=' + default_args['cluster_name'] + ' -- -data_path=' + default_args['data_path'] + ' -data_path_bq=' + default_args['data_path_bq'],
    dag=dag)

t1 >> t2
