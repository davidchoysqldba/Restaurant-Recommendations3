from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

import logging
log = logging.getLogger(__name__)


default_args = {
    'owner': 'developer',
    'depends_on_past': True,
    'start_date': datetime(2014, 12, 3), # back fill date
    'path_name': '/data',
    'pattern_name': 'restaurant_data*json',
    'download_url': 'https://data.cityofnewyork.us/resource/9w7m-hzhe.json?inspection_date=',
    'email': ['developer@gmail.com'],
    'google_project_id': '',
    'google_bucket_name': '',
    'email_on_failure': False,
    'email_on_retry': False,
    'days_go_back': 3
    }


def download_file_into_gcp(**context):
    import requests
    from google.cloud import storage

    execution_date = context['execution_date']
    file_run_date = execution_date - timedelta(days=3)
    log.info('execution_date: ' + str(execution_date) + ', file_run_date: ' + str(file_run_date ))

    url = default_args['download_url'] + file_run_date.strftime('%Y-%m-%d')
    # fileName = default_args['path_name'] + '/' + default_args['pattern_name'].split('*')[0] + '_' + file_run_date.strftime('%Y_%m_%d') + '.' + default_args['pattern_name'].split('*')[1]
    fileName = default_args['pattern_name'].split('*')[0] + '_' + file_run_date.strftime('%Y_%m_%d') + '.' + default_args['pattern_name'].split('*')[1]
    # download_content = req.get(url)
    r = requests.get(url, stream=True)

    client = storage.Client(project=default_args['google_project_id'])
    bucket = client.get_bucket(default_args['google_bucket_name'])

    blob = bucket.blob(fileName)
    blob.upload_from_string(r.text)


    # gcphook.upload(object=download_content, bucket=)


def test_task(**context):
    # print(context['execution_date'])
    # log.info('execution_date: ' + str(context['execution_date']))
    execution_date = context['execution_date']
    file_run_date = execution_date - timedelta(days=3)
    # log.info('execution_date: ' + str(execution_date) + ', file_run_date: ' + str(file_run_date ))


dag = DAG('opendata_download_file_to_cloud', default_args=default_args,schedule_interval="@daily")

#dag = DAG('test_backfill_dag', default_args=default_args,schedule_interval="@daily")


t1 = BashOperator(
    task_id='Timeout',
    bash_command='sleep 3',
    dag=dag)

t2 = PythonOperator(
        task_id='DownloadFileToCloud',
        python_callable=download_file_into_gcp,
        provide_context=True,
        dag=dag)


t1 >> t2
