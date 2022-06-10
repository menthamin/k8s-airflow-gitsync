"""
Airflow Task 실패시 Slack 채널에 메시지 발송
    - Reference: https://data-engineer-tech.tistory.com/29
"""

from airflow.providers.slack.operators.slack import SlackAPIPostOperator 
from dateutil.relativedelta import relativedelta

# 클래스로 보완?
def slack_on_failure_alert(context): 

    channel = '#alert-octopus' 
    # token = 'xoxb-31642232595-3430165285989-nLEapKJ8HNLM29q7JFJpsoYf'  # app_name: OctopusAlert
    token = 'xoxb-31642232595-3568510611267-mQ1j4CkEZYlEaSbXe5fxj3Gt'  # app_name: airflow-bot

    task_instance = context.get('task_instance') 
    task_id = task_instance.task_id 
    dag_id = task_instance.dag_id 

    execution_date = (context.get('execution_date') + relativedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S') 

    text = f'''*[AIRFLOW ERROR REPORT]* 
    ■ DAG: _{dag_id}_ 
    ■ Task: _{task_id}_ 
    ■ Execution Date (KST): _{execution_date}_ ''' 
    
    alert = SlackAPIPostOperator(
        task_id='slack_on_failure_alert', 
        channel=channel, 
        token=token, 
        text=text
    )

    return alert.execute(context=context)