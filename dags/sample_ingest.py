
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
    product_expire TIMESTAMP)
    PARTITION BY RANGE (create_at);
    
    CREATE TABLE IF NOT EXISTS {targetTable}_01 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2023-01-02 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_02 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-02 00:00:00') TO ('2023-01-03 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_03 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-03 00:00:00') TO ('2023-01-04 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_04 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-04 00:00:00') TO ('2023-01-05 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_05 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-05 00:00:00') TO ('2023-01-06 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_06 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-06 00:00:00') TO ('2023-01-07 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_07 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-07 00:00:00') TO ('2023-01-08 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_08 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-08 00:00:00') TO ('2023-01-09 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_09 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-09 00:00:00') TO ('2023-01-10 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_10 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-10 00:00:00') TO ('2023-01-11 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_11 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-11 00:00:00') TO ('2023-01-12 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_12 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-12 00:00:00') TO ('2023-01-13 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_13 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-13 00:00:00') TO ('2023-01-14 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_14 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-14 00:00:00') TO ('2023-01-15 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_15 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-15 00:00:00') TO ('2023-01-16 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_16 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-16 00:00:00') TO ('2023-01-17 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_17 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-17 00:00:00') TO ('2023-01-18 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_18 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-18 00:00:00') TO ('2023-01-19 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_19 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-19 00:00:00') TO ('2023-01-20 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_20 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-20 00:00:00') TO ('2023-01-21 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_21 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-21 00:00:00') TO ('2023-01-22 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_22 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-22 00:00:00') TO ('2023-01-23 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_23 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-23 00:00:00') TO ('2023-01-24 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_24 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-24 00:00:00') TO ('2023-01-25 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_25 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-25 00:00:00') TO ('2023-01-26 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_26 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-26 00:00:00') TO ('2023-01-27 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_27 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-27 00:00:00') TO ('2023-01-28 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_28 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-28 00:00:00') TO ('2023-01-29 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_29 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-29 00:00:00') TO ('2023-01-30 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_30 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-30 00:00:00') TO ('2023-01-31 00:00:00');
    CREATE TABLE IF NOT EXISTS {targetTable}_31 PARTITION OF {targetTable} FOR VALUES FROM ('2023-01-31 00:00:00') TO ('2023-02-01 00:00:00');"""

create_table = PostgresOperator(
    task_id="create_table",
    sql=createCommand,
    dag=dag
)

#################################################################################### TABLE Truncate
tuncateCommand=f"TRUNCATE TABLE IF NOT EXISTS {targetTable};"
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
start >> create_table >> truncate_table >> transformation >> loading >> end
