"""
Jenkins Connection
"""

from airflow.providers.jenkins.hooks.jenkins import JenkinsHook

def run_jenkins_hook():
    jenkins_hook = JenkinsHook(conn_id="codebrick-jenkins")

    print(jenkins_hook.get_jenkins_server())
    print(jenkins_hook.get_jenkins_server().get_version)
    print(jenkins_hook.jenkins_server.get_jobs())

    return jenkins_hook.get_jenkins_server().get_version