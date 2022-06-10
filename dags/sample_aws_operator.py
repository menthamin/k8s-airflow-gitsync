"""
    AWS Operator 테스트
"""
# [START tutorial]
# [START import_module]
import boto3
import time
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.hooks.glue import AwsGlueJobHook
from airflow.utils.dates import days_ago
from airflow.models import Variable



# [SET variables & functions]
# hook=AwsBaseHook("octopus_aws_conn")
# AwsGlueJobOperator.ui_color = "#F3D5C5"
# aws_id = Variable.get("aws_access_key_id")
# aws_secret = Variable.get("aws_secret_access_key")
# aws_glue_conn = AwsGlueJobHook(aws_conn_id="octopus_aws_conn")

def run_glue(job_name: str):
    
    glue_client = boto3.client('glue', region_name='ap-northeast-1')

    print(f"Start {job}")
    job = glue_client.start_job_run(JobName=job_name)

    while True:
        status = glue_client.get_job_run(JobName=job_name, RunId=job['JobRunId'])
        if status['JobRun']['JobRunState'] == 'SUCCEEDED':
            print(f"End {job}")
            break
    
    time.sleep(1)

    return f"Success {job}"

# name과 스크립트 locate를 얻어서 실행한다.

def print_aws_access_info():
    print("aws_id:", aws_id)
    print("aws_secret:", aws_secret)

    return [aws_id, aws_secret]

# [END import_module]



# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'aws-operator-sample',
    default_args=default_args,
    description='AWS operator test dag',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['octopus'],
) as dag:
    # [END instantiate_dag]

    # [START basic_task]
    t1 = S3ListOperator(
        task_id='s3_list_operator',
        bucket="tokotalk-data-store",
        prefix="daily-order-details-report/",
        # delimiter='/',
        aws_conn_id="octopus_aws_conn"
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t3 = PythonOperator(
        task_id="python_test",
        provide_context=True,
        python_callable=print_aws_access_info,
        op_kwargs=None
    )

    t4 = PythonOperator(
        task_id="glue_test",
        provide_context=True,
        python_callable=run_glue,
        op_kwargs=None
    )

    # t4 = AwsGlueJobHook(
    #     task_id="glue_job_test",
    #     aws_conn_id="octopus_aws_conn",
    #     region_name="ap-southeast-1",
    #     job_name="ongkir_cities",
    #     num_of_dpus=3,
    #     concurrent_run_limit=1
    # )
    # [END basic_task]

    t1 >> t2 >> t3 >> t4
# [END tutorial]