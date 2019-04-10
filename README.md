# Cloud Composer DAG Example
This repo has the code for a very basic DAG that illustrates the use of the KubernetesPodOperator in Airflow, as well as secrets and volume mounts. It pulls some sample data from Snowflake using credentials provided in a secret, and writes the data to a persistent disk. The next task in the DAG loads the data from the persistent disk using Pandas and then outputs a simple sum.

The code depends on the existence of the indicated persistent disk and secret in the Cloud Composer kube cluster running Airflow.

## Structure
### DAG definition file
test_snowflake_dag.py contains the actual DAG definition used by Airflow. This should be put into the DAGs folder in Cloud Composer.

There is also a version of this DAG, test_snowflake_dag_libs.py, which uses the common configuration libraries (not included in this repo) to set secrets and other configuration. This is the recommended method, so that configuration details are not repeated across DAGs. See the [Kiva DAGs repo](https://github.com/kiva/airflow-dags/) for these.

### docker-snowflake
This contains the docker image that pulls the data from Snowflake using snowsql.

### docker-pyton
This contains the Python docker image and code that loads the data and prints out a sum.

Useful references:
* [GKE, Airflow, Cloud Composer, and Persistent Volumes](http://space.af/blog/2018/09/30/gke-airflow-cloud-composer-and-persistent-volumes/)
