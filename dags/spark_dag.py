from airflow import DAG
from datetime import datetime
# from airflow.operators. import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_argus = {'owner':'spark_ca', 'retries':0, 'catchup':False,'start_date':datetime(2023,1,1),}

dag = DAG(dag_id='SPARK_DAG_Example1', default_args=default_argus, schedule='@daily')

# start = DummyOperator(task_id="start", dag=dag)

spark_config = {'conn_id' :'spark_standalone', 'application':'/opt/airflow/dags/spark_example.py','driver_memory':'1g', 'executor_memory':'512m', 'executor_cores':3, 'num_executors':2}

spark_operator = SparkSubmitOperator(task_id='spark-submit' ,dag=dag, **spark_config)

# end = DummyOperator(task_id="end", dag=dag)

spark_operator