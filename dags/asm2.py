from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from google_drive_downloader import GoogleDriveDownloader as gdd
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime
from config_asm2 import *
import os

# Tạo một hàm cho branch
def branch_file_exist():
    if os.path.exists(QUESTION_FILE_PATH) and os.path.exists(ANSWER_FILE_PATH):
        return 'end'
    
    return 'clear_file'

# Tạo hàm để download file Answers và Questions
def download_question_file():
    gdd.download_file_from_google_drive(file_id=QUESTION_DRIVER_ID,
                                        dest_path=QUESTION_FILE_PATH)
def download_answer_file():
    gdd.download_file_from_google_drive(file_id=ANSWER_DRIVER_ID,
                                        dest_path=ANSWER_FILE_PATH)
    

with DAG(
    dag_id='dep303x_asm2',
    start_date=datetime(2024,1,1),
    schedule_interval='@daily',
    catchup=False ) as dag:
 
    # Yêu cầu 1: 
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end', trigger_rule='one_success')
    
    # Yêu cầu 2:
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=branch_file_exist,
        provide_context=True,
        dag=dag 
    )
    
    # Yêu cầu 3:
    clear_file = BashOperator(
        task_id='clear_file',
        bash_command=f'rm -f {QUESTION_FILE_PATH} && rm -f {ANSWER_FILE_PATH}',
        dag=dag
    )
    
    
    # Yêu cầu 4:
    download_question_file_task = PythonOperator(
        task_id = 'download_question_file_task',
        python_callable=download_question_file,
        dag=dag
    )
    
    download_answer_file_task = PythonOperator(
        task_id = 'download_answer_file_task',
        python_callable=download_answer_file,
        dag=dag
    )

    # Yêu cầu 5: import dữ liệu vào mongodb
    import_answer_into_mongo = BashOperator(
        task_id = 'import_answers_into_mongo',
        bash_command=f'mongoimport -u root -p root --host mongodb --authenticationDatabase admin --type csv -d dep303x -c answers --headerline {ANSWER_FILE_PATH}'
    )

    import_questions_into_mongo = BashOperator(
        task_id = 'import_questions_into_mongo',
        bash_command=f'mongoimport -u root -p root --host mongodb --authenticationDatabase admin --type csv -d dep303x -c questions --headerline {QUESTION_FILE_PATH}'
    )
    
    # Yêu cầu 6: 
    spark_process = SparkSubmitOperator(
        task_id='spark_process',
        conn_id='spark_local',
        packages='org.mongodb.spark:mongo-spark-connector_2.12:3.0.1',
        application=SPARK_CODE_PATH,
        dag=dag
    )

    # Yêu cầu 7:
    import_output_mongo = BashOperator(
        task_id = 'import_output_mongo',
        bash_command=f'cat {OUTPUT_FILE_PATH}/*.csv | mongoimport -u root -p root --host mongodb --authenticationDatabase admin --type csv -d dep303x -c output --headerline'
    )

    # Yêu cầu 8:
    start >> branching
    branching >> end
    branching >> clear_file
    clear_file >> [download_question_file_task, download_answer_file_task]
    download_question_file_task >> import_questions_into_mongo 
    download_answer_file_task >> import_answer_into_mongo

    [import_questions_into_mongo, import_answer_into_mongo] >> spark_process
    spark_process >> import_output_mongo
    import_output_mongo >> end

    
    


