from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

my_file = Dataset("/tmp/myfile.txt")
my_file2 = Dataset("/tmp/myfile2.txt")

with DAG(dag_id="consumer", schedule=[my_file,my_file2], start_date=datetime(2022,1,1), catchup=False):
    @task
    def read_dataset():
        with open(my_file.uri, 'r') as f:
            print(f.read())

    @task
    def read_dataset2():
        with open(my_file2.uri, 'r') as f:
            print(f.read())
        
    read_dataset() >> read_dataset2()
