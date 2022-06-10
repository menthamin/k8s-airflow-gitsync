"""
Glue Job 트리거 모음: boto3, Airflow GlueJobHook 2가지 버전
"""

import boto3
import time

from airflow.providers.amazon.aws.hooks.glue import GlueJobHook  # 모든 AWS hook, operator는 .aws/credentials 필요

def run_glue_boto3(job_name: str, region_name: str):
    """
    region, job name을 입력 받아 boto3 모듈로 glue job 실행
    """    
    glue_client = boto3.client('glue', region_name=region_name)

    print(f"Start glue job: {job_name}")
    job = glue_client.start_job_run(JobName=job_name)

    while True:
        status = glue_client.get_job_run(JobName=job_name, RunId=job['JobRunId'])
        if status['JobRun']['JobRunState'] == 'SUCCEEDED':  # 실패 시 액션 추가 필ㅇ
            print(f"End glue job: {job_name}")
            break
    
    time.sleep(10)  # Exceed running 방지용

    return f"Success glue job: {job_name}"

def run_glue_hook(job_name: str, region_name: str):
    """
    region, job name을 입력 받아 Airflow hook 모듈로 glue job 실행
    """    

    glue_hook = GlueJobHook(job_name=job_name, region_name=region_name)
    
    print(f"Start glue job: {job_name}")  
    run_job_info = glue_hook.initialize_job()
    glue_hook.job_completion(job_name=job_name, run_id=run_job_info["JobRunId"])
    print(f"End glue job: {job_name}")

    time.sleep(10)

    return f"Success glue job: {job_name}"