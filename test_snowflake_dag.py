# Basic airflow and utility imports
from datetime import datetime, timedelta
from os import environ
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# Kubernetes airflow components
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator, Volume, VolumeMount
from airflow.contrib.kubernetes import secret, pod

# Secrets
# ------------------
# There is a secret in the Cloud Composer kubernetes cluster called "ml-service-secret" which contains the following environment variables
# These can be used to connect to Snowflake using snowsql or similar methods
snowflake_secret_names = ['SNOWSQL_USER','SNOWSQL_PWD','SNOWSQL_ACCOUNT','SNOWSQL_DATABASE','SNOWSQL_HOST','SNOWSQL_WAREHOUSE']
snowflake_secret_list = []

# This creates a list of secrets that can be passed to the KubernetesPodOperator
# In this case we use the deploy_type 'env' so that the secrets are injected as environment variables into
# the container run by KubernetesPodOperator
for s in snowflake_secret_names:
	snowflake_secret_list.append(secret.Secret(
	    deploy_type='env',
      # deploy_target indicates the enviro var name when deploy_type = 'env'
	    deploy_target=s,
      # 'key' refers to the key in the secrets file
	    key=s,
      # This should be the name of the kubernetes secret from which the info is loaded.
	    secret='ml-service-secret'))

# Volume mount example
# -----------------------
# "pv-airflow" is the name of a persistentvolume in the kubernetes cluster that runs Google Cloud Composer,
# which in turn points to a persistent disk in Google Compute Engine that can be used to move files between
# tasks in Airflow/Cloud Composer.
# The mount_path is the directory that will be mounted in running containers.
# Your containers will be able to read and write to this path in order to move files between tasks + containers.
volume_mount = VolumeMount('pv-airflow',
                           mount_path='/files',
                           sub_path=None,
                           read_only=False)
volume_config= {
    'persistentVolumeClaim':
    {
        'claimName': 'pv-claim-airflow'
    }
}
volume = Volume(name='pv-airflow', configs=volume_config) # The name here should match the volume mount.

# Node affinity
# ----------------
# You can optionally set a node affinity to run a task in a different node pool in kubernetes
# than the one used by Google Cloud Composer.
# I've set aside an auto-scaling node pool of higher-memory instances, called "airflow-dev-working", for machine learning
node_affinity={
        'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        'key': 'cloud.google.com/gke-nodepool',
                        'operator': 'In',
                        'values': [
                            'airflow-dev-working',
                        ]
                    }]
                }]
            }
        }
}


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
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    # The DAG ID: try to version this as this is how the metadata database tracks it
    'test_snowflake_dl_v5', 
    default_args=default_args,
    # Can be defined as an interval or cron; see documentation
    # Important, as this defines how often the task is run
    schedule_interval=timedelta(minutes=10)
)

# This is an example task container (using a generic ubuntu image) that pulls a SELECT 20 of data  
# from the loan table from Snowflake using snowsql, and then writes the result to the persistent disk.
snowflake_dl = KubernetesPodOperator(
              # I suggest using the "operator" namespace in our kube cluster for running the KubernetesPodOperator to keep things organized,
              # since the "default" namespace is taken up by the other airflow pods
              namespace='operator',
              # Image pull policy: IfNotPresent uses images if already presnet, otherwise "Always" will always pull a new Docker image
							image_pull_policy='Always',
                          # This currently points to a public DockerHub image -- need to add private repo
                          image="rschoenbeck/test-snowsql:latest",
                          # Specify the command run in the container
                          cmds=["/snow/snowsql","-f","query_file.sql","-o","output_file=/files/data.csv","-o","quiet=true","-o","output_format=csv","-o","friendly=false"],
                          # Task name and ID
                          name="snowflake-dl",
                          task_id="snowflake-dl-task-v4",
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
                          	image="rschoenbeck/test-python:latest",
                          	cmds=["python","test_script.py"],
                          	name="python-test",
                          	task_id="python-test-task-v4",
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
