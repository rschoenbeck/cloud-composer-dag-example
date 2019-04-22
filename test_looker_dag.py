# This file does the same thing as test_snowflake_dag.py, but uses common DAG libraries for configuration
# Note that the libraries aren't part of this repo. See https://github.com/kiva/airflow-dags

# Basic airflow and utility imports
from datetime import datetime, timedelta
from os import environ
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# Kubernetes airflow components
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes import pod
# Custom libraries
from dag_libs.secrets import AFSecret
from dag_libs.volumes import AFVolume
from dag_libs.affinities import AFAffinity

looker_secrets = AFSecret('looker-api')
looker_secret_list = looker_secrets.get_secrets()

airflow_volume = AFVolume('persistent_disk')
volume = airflow_volume.get_volume()
volume_mount = airflow_volume.get_volume_mount()

looker_manifest_id = 'looker_test'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # start_date defines when a task starts -- important!
    # Be careful with this as if the time is set before now, Airflow will attempt to "catch up"
    # by default, by running the task many times; see https://airflow.apache.org/scheduler.html
    # This can be disabled by setting catchup=False (see below)
    'start_date': datetime(2019,4,18),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    # The DAG ID: try to version this as this is how the metadata database tracks it
    'looker_test_dag', 
    default_args=default_args,
    # Can be defined as an interval or cron; see documentation
    # Important, as this defines how often the task is run
    schedule_interval=timedelta(minutes=10),
    # Important! Catchup defaults to "true", which means airflow will attempt backfill runs
    # if the start date is before now (in UTC)
    catchup = False
)

looker_dl = KubernetesPodOperator(
              # I suggest using the "operator" namespace in our kube cluster for running the KubernetesPodOperator to keep things organized,
              # since the "default" namespace is taken up by the other airflow pods
              namespace='operator',
              # Image pull policy: IfNotPresent uses images if already presnet, otherwise "Always" will always pull a new Docker image
							image_pull_policy='Always',
                          # This pulls from the Google Container Registry attached to the same project as Cloud Composer
                          # If pulling from a different private registry, we need to add image pull secrets for the credentials (see documentation)
                          #
                          # The "snowsql-base" image is just snowsql running on Ubuntu, and can be used to download the results of any single query from Snowflake
                          # and save it to a file (in the `/files` directory, to which the persistent disk is mounted, which will pass along to the next task).
                          image="gcr.io/kiva-machine-learning/looker-api:latest",
                          # Below is the argument where you specify the command to run in the container.
                          # In the CMD invocation for the container, the `sql_query` variable contains the query string (defined above).
                          cmds=["python","looker_cmd.py","space","679",looker_manifest_id],
                          # Task name and ID
                          name="looker-dl",
                          task_id="looker-api-dl-task",
                          # Save logs for viewing in the Airflow web UI
                          get_logs=True,
                          dag=dag,
                          # List of secrets to use
                          secrets=looker_secret_list,
                          # List of volumes and mounts
                          #volumes = [volume],
                          #volume_mounts = [volume_mount],
                          # Good to default the following to "True" unless testing -- otherwise a kubernetes pod will stay
                          # around for every single time you run your task (!)
                          is_delete_operator_pod=True,
                          # Optional node affinity -- see above
                          #affinity=node_affinity,
                          # The default timeout is ~120 seconds, which may be too short to start some pods that need to pull longer images
                          startup_timeout_seconds = 420
)


