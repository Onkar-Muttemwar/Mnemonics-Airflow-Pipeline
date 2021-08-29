#from airflow.contrib.hooks.wasb_hook import WasbHook
#from airflow.contrib.hooks.wasb_hook import WasbHook
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import glob

def file_upload(container_name, blob_name, file_path):
    #connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    connect_str = '' # Connection String
    service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = service_client.get_container_client(container_name)
    #blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    for filename in glob.glob(file_path):
        file_path_on_azure = os.path.join(blob_name,filename).replace('/usr/local/airflow/dags/','')
        #blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client = container_client.get_blob_client(file_path_on_azure)
        with open(os.path.join(os.getcwd(), filename), 'rb') as f:
            blob_client.upload_blob(f)
