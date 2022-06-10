"""
Operator example
"""

# [Import_module]
import boto3
import time
import json
from datetime import timedelta
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).absolute().parent.parent))

from scripts.fail_alarm import slack_on_failure_alert
from scripts.python_sample import print_input_msg

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook  # 모든 AWS hook, operator는 .aws/credentials 필요
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator  # Create glue job & 실행
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["sj.hong@codebrick.co", "jeon@codebrick.co"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_on_failure_alert
}

# [START instantiate_dag]
with DAG(
    "Operators-sample",
    default_args=default_args,
    description="Codebrick Operator Samples, AWS, Jenkins, ..., etc.",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["octopus"],
) as dag:

    # [START basic_task]
    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    with TaskGroup(group_id="Basic-operators") as tg1:
        bash_sample = BashOperator(
            task_id="bash-operator",
            depends_on_past=False,
            bash_command="sleep 5",
        )

        python_sample = PythonOperator(
            task_id="python-operator",
            provide_context=True,
            python_callable=print_input_msg,
            op_kwargs={"msg": "This is Python Operator!!!"}
        )

        bash_sample >> python_sample

    grp1_end = DummyOperator(
        task_id='group1-end',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    start >> tg1 >> grp1_end

    with TaskGroup(group_id="Provider-operators") as tg2:
        job_name = "airflow-glueoperator-test"
        glue_sample = AwsGlueJobOperator(
            task_id="gluejob-operator",
            job_name=job_name,
            region_name="ap-southeast-1",
            wait_for_completion=True,
            script_location=f"s3://aws-glue-assets-468760750604-ap-southeast-1/scripts/dummy.py",  # Dummy
            run_job_kwargs={"NumberOfWorkers": 3, "WorkerType": "G.1X"}
            # run_job_kwargs에 사용 가능한 파라미터: JobName, JobRunId, Arguments, AllocatedCapacity, Timeout, MaxCapacity, SecurityConfiguration, NotificationProperty, WorkerType, NumberOfWorkers
            # when calling the StartJobRun operation: Please do not set Max Capacity if using Worker Type and Number of Workers.
        )

        jenkins_sample = JenkinsJobTriggerOperator(
            task_id="jenkins-operator",
            jenkins_connection_id ="codebrick-jenkins",
            job_name="airflow-operator"
        )

        # Lambda event parameter
        SAMPLE_EVENT = json.dumps({"x": 10, "y": 10, "action": "plus"})
        LAMBDA_FUNCTION_NAME = "airflow-lambda-operator-test"
        AwsLambdaInvokeFunctionOperator.ui_color = '#ffffff'

        lambda_sample = AwsLambdaInvokeFunctionOperator(
            task_id='lambda-operator',
            function_name=LAMBDA_FUNCTION_NAME,
            payload=SAMPLE_EVENT
            # invocation_type="Event",  # Invode a function asynchronously / Default = None, sysnchronously
        )
        
        [jenkins_sample, lambda_sample] >> glue_sample

    end = DummyOperator(
        task_id='all-tasks-end',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    grp1_end >> tg2 >> end