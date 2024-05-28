# blob_utils.py
import logging
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobClient, ContainerClient, BlobServiceClient, BlobSasPermissions, generate_blob_sas

def get_latest_file_name_in_blob(container, connection_string):
    """
    Retrieves the file name of latest file in a specific blob in Azure Blob store.
    """
    container_client = ContainerClient.from_connection_string(conn_str=connection_string, container_name=container)
    
    file_date_df = pd.DataFrame(columns=['blob_name', 'last_modified'])
    count = 0

    for blob in container_client.list_blobs():
        file_date_df.loc[count] = [f'{blob.name}', f'{blob.last_modified}']
        count += 1

    file_date_df['last_modified'] = pd.to_datetime(file_date_df['last_modified'], format='%Y-%m-%d %H:%M:%S')

    return file_date_df.loc[file_date_df['last_modified'] == file_date_df['last_modified'].max()]['blob_name']

def get_blob_df(blob_list, account_name, account_key, container_name):
    blob_df_list = []

    for blob_name in blob_list:
        logging.info('Generating blob SAS token')
        sas = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )

        try:
            blob_url = f'https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas}'
            blob_df_list.append(pd.read_excel(blob_url, sheet_name="Connections"))
            logging.info(f"{blob_name} retrieved.")
        except Exception as e:
            logging.error(f"Exception while fetching blob {blob_name}: {e}")
            continue

    if blob_df_list:
        return pd.concat(blob_df_list)
    return pd.DataFrame()

def upload_workbook(local_path, blob_container_name, blob_name, conn_str):
    """
    Upload workbook to blob storage.
    """
    BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(conn_str)
    blob_client = BLOB_SERVICE_CLIENT.get_blob_client(container=blob_container_name, blob=blob_name)
    
    try:
        with open(local_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True, connection_timeout=600)
            logging.info(f'{blob_name} uploaded to container: {blob_container_name} successfully')
    except Exception as e:
        logging.error(f"Error uploading {blob_name}: {e}")
