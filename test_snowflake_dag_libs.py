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

# Secrets
# ------------------
# There is a secret in the Cloud Composer kubernetes cluster called "ml-service-secret" which contains the following environment variables
# These can be used to connect to Snowflake using snowsql or similar methods

# This creates a list of secrets that can be passed to the KubernetesPodOperator
# In this case we use the deploy_type 'env' so that the secrets are injected as environment variables into
# the container run by KubernetesPodOperator
snowflake_secrets = AFSecret('snowflake')
snowflake_secret_list = snowflake_secrets.get_secrets()

# Volume mount example
# -----------------------
# "pv-airflow" is the name of a persistentvolume in the kubernetes cluster that runs Google Cloud Composer,
# which in turn points to a persistent disk in Google Compute Engine that can be used to move files between
# tasks in Airflow/Cloud Composer.
# The mount_path is the directory that will be mounted in running containers.
# Your containers will be able to read and write to this path in order to move files between tasks + containers.
airflow_volume = AFVolume('persistent_disk')
volume = airflow_volume.get_volume()
volume_mount = airflow_volume.get_volume_mount()

# Node affinity
# ----------------
# You can optionally set a node affinity to run a task in a different node pool in kubernetes
# than the one used by Google Cloud Composer.
# I've set aside an auto-scaling node pool of higher-memory instances, called "airflow-dev-working", for machine learning
# It adds pods in the 'operator' namespace of the cluster -- thus the name
airflow_affinity = AFAffinity('operator')
node_affinity = airflow_affinity.get_affinity()


# DAG definitions
# ------------------
# DAGs (Directed Acyclic Graphs) are the sequences that execute Airflow tasks
# This is just a generic example set of default Airflow DAG configurations
# See more info about writing DAGs and Airflow here: 
# http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/
# https://www.astronomer.io/guides/dag-best-practices/
# https://github.com/jghoman/awesome-apache-airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # start_date defines when a task starts -- important!
    # Be careful with this as if the time is set before now, Airflow will attempt to "catch up"
    # by default, by running the task many times; see https://airflow.apache.org/scheduler.html
    # This can be disabled by setting catchup=False (see below)
    'start_date': datetime(2019,4,1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    # The DAG ID: try to version this as this is how the metadata database tracks it
    'test_snowflake_dl_libs_err_v2', 
    default_args=default_args,
    # Can be defined as an interval or cron; see documentation
    # Important, as this defines how often the task is run
    schedule_interval=timedelta(minutes=10),
    # Important! Catchup defaults to "true", which means airflow will attempt backfill runs
    # if the start date is before now (in UTC)
    catchup = False
)

# These variables, a query and an output file, will be passed to the invocation of snowsql in the container
sql_query = "select * from loan limit 20;"
sql_output_file = "data.csv"

# This is an example task container (using a generic ubuntu image) that pulls a SELECT 20 of data  
# from the loan table from Snowflake using snowsql, and then writes the result to the persistent disk.
snowflake_dl = KubernetesPodOperator(
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
                          image="gcr.io/kiva-machine-learning/snowsql-base:latest",
                          # Below is the argument where you specify the command to run in the container.
                          # In the CMD invocation for the container, the `sql_query` variable contains the query string (defined above).
                          cmds=["/snow/snowsql","-q",sql_query,"-o","output_file=/files/" + sql_output_file,"-o","quiet=true","-o","output_format=csv",
                                  "-o","friendly=false","-o","exit_on_error=false"],
                          # Task name and ID
                          name="snowflake-dl",
                          task_id="snowflake-dl-task",
                          # Save logs for viewing in the Airflow web UI
                          get_logs=True,
                          dag=dag,
                          # List of secrets to use
                          secrets=snowflake_secret_list,
                          # List of volumes and mounts
                          volumes = [volume],
                          volume_mounts = [volume_mount],
                          # Good to default the following to "True" unless testing -- otherwise a kubernetes pod will stay
                          # around for every single time you run your task (!)
                          is_delete_operator_pod=True,
                          # Optional node affinity -- see above
                          affinity=node_affinity,
                          # The default timeout is ~120 seconds, which may be too short to start some pods that need to pull longer images
                          startup_timeout_seconds = 300
)

# This is a follow-up task that uses a python container to load the data from the persistent disk
# used in the previous task, loads the data in pandas, and then sums the "price" column of the loan 
# data and prints it
test_print = KubernetesPodOperator(namespace='operator',
							image_pull_policy='Always',
                          	image="gcr.io/kiva-machine-learning/test-python:latest",
                          	cmds=["python","test_script.py"],
                          	name="python-test",
                          	task_id="python-test-task",
                          	get_logs=True,
                          	dag=dag,
                          	volumes = [volume],
                          	volume_mounts = [volume_mount],
                          	is_delete_operator_pod=True,
                          	affinity=node_affinity,
                          	startup_timeout_seconds = 300
)

# This DummyOperator doesn't do anything; it passes if the previous tasks passed
# at the very end of the DAG
end = DummyOperator(task_id='end', dag=dag)

# This is an example of using bitshift operators to specify which tasks are downstream of which tasks
snowflake_dl >> test_print >> end
