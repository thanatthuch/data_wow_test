
import os
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator




# /* DECLARE argument */
sourcePath      : str  = "data_sample"
sourceTransPath : str  = "data_trans"
targetTable     : str  = "demo_table"
targetSchema    : dict = {
    "department_name" : "varchar(32)",
    "sensor_serial"   : "varchar(64)",
    "create_at"       : "timestamp",
    "product_name"    : "varchar(16)",
    "product_expire"  : "timestamp"
}
targetPartition :str = "create_at"


connection : str = "postgres_default"

defaultArgs : dict = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}
pg_hook_load = PostgresHook(postgres_conn_id=connection)



#################################################################################### Processing ####################################################################################

dag = DAG(
    'raw_ingestion',
    default_args=defaultArgs,
    description='ingest data to table',
    schedule_interval=None
)

#################################################################################### TABLE Creation
createCommand=f"""CREATE TABLE IF NOT EXISTS {targetTable} (
    department_name VARCHAR(32),
    sensor_serial VARCHAR(64),
    create_at TIMESTAMP,
    product_name VARCHAR(16),
    product_expire TIMESTAMP)"""

create_table = PostgresOperator(
    task_id="create_table",
    sql=createCommand,
    dag=dag
)

#################################################################################### TABLE Truncate
tuncateCommand=f"TRUNCATE TABLE {targetTable};"
truncate_table = PostgresOperator(
    task_id="truncate_table",
    sql=tuncateCommand,
    dag=dag
)


#################################################################################### FILE Transformation
def file_transform():
    data_files = os.listdir(sourcePath)
    data_files.sort()

    if not os.path.exists(sourceTransPath):
        os.mkdir(sourceTransPath)

    for file_name in data_files:
        new_file_name = file_name.replace("parquet", "csv")
        temp_df = pd.read_parquet(sourcePath +"/" +file_name)
        temp_df.to_csv(sourceTransPath +"/"+ new_file_name, index=False)
        print(f"----- ------ ------ -----`{new_file_name}` Transformed ----- ------ ------ -----")

transformation = PythonOperator(
    task_id = "parquet_to_csv",
    python_callable=file_transform,
    dag=dag
)

#################################################################################### Loading state
def data_loading():
    data_files = os.listdir(sourceTransPath)
    data_files.sort()
    total_ingest_file = 0
    for file_name in data_files:
        sql_command = f"""COPY {targetTable} (department_name, sensor_serial, create_at, product_name, product_expire)
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """
        file_path = f"{sourceTransPath}/{file_name}"
        # Open and execute the COPY command using the pg_hook
        with open(file_path, 'r') as file:
            pg_hook_load.copy_expert(sql=sql_command, filename=file_path)
        print(f"======================== `{file_name}` has loaded ========================")
        total_ingest_file += 1

    print(f"********************************** All files ingest SUCCEEDED  {total_ingest_file}**********************************")


loading = PythonOperator(
    task_id='load_csv_into_table',
    python_callable=data_loading,
    dag=dag,
)


#################################################################################### PIPELINE ####################################################################################


start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)
start >> transformation >> create_table >> truncate_table >> loading >> end
