from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

myfile = Dataset("/tmp/myfile.txt")
myfile2 = Dataset("/tmp/myfile2.txt")
with DAG(dag_id='producer', schedule='@daily', start_date=datetime(2022,1,1), catchup=False) as dag:
    
    @task(outlets=[myfile])
    def update_dataset():
        with open(myfile.uri, "a+") as f:
            f.write("producer update")

    @task(outlets=[myfile2])
    def update_dataset2():
        with open(myfile2.uri, "a+") as f:
            f.write("producer update")

    update_dataset() >> update_dataset2()