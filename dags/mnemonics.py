from  datetime  import  datetime , timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from  dag_scripts . local_to_blob  import  file_upload
from  dag_scripts . scraping  import  mnemonic_scraper_all
from  dag_scripts . remove_from_local  import  remove_local_file



default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
}


dag = DAG(
    dag_id = 'mnemonics',
    default_args=default_args,
    description='Creating a Mnemonics Scraper DAG',
    schedule_interval = None,
    catchup = False
)

start_of_data_pipeline  =  DummyOperator ( task_id = 'start_of_data_pipeline' , dag = dag )

Mnemonic_Scraper = PythonOperator(
    task_id='Mnemonic_Scraper',
    python_callable=mnemonic_scraper_all,
    op_kwargs = {
            'page_quantity' : 1 ,
        },
    dag=dag
)


Mnemonic_Scraper_File_To_Blob = PythonOperator(
    task_id='Mnemonic_Scraper_File_To_Blob',
    python_callable=file_upload,
    op_kwargs = {
            'container_name' : '' ,# Container Name
            'blob_name'      : '' ,# Blob Name
            'file_path'      : '', # Local Path 
        },
    dag=dag
)

Mnemonic_Scraper_Remove_From_Local = PythonOperator(
    task_id='Mnemonic_Scraper_Remove_From_Local',
    python_callable=remove_local_file,
    dag=dag
)

end_of_data_pipeline  =  DummyOperator ( task_id = 'end_of_data_pipeline' , dag = dag )

# Defining the execution pattern
start_of_data_pipeline  >>  Mnemonic_Scraper  >>  Mnemonic_Scraper_File_To_Blob  >>  Mnemonic_Scraper_Remove_From_Local  >>  end_of_data_pipeline