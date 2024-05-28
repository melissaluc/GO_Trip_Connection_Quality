# main.py
import logging
import json
import sys
from datetime import datetime, timedelta
import azure.functions as func
import pandas as pd
import tempfile

from config import CONNECTIONSTRING, CONTAINERNAME, STORAGEACCOUNTKEY, ACCOUNTNAME
from blob_utils import get_latest_file_name_in_blob, get_blob_df, upload_workbook
from data_utils import process_data

def main(msg: func.QueueMessage) -> None:
    if msg is not None:
        body = json.loads(msg.get_body().decode('utf-8'))
        logging.info(f"Queue message contents: {body}")

        base_date_value = body.get('date', datetime.now() - timedelta(days=2))

        if isinstance(base_date_value, str):
            base_date = datetime.strptime(base_date_value, '%Y-%m-%d')
        else:
            base_date = base_date_value
    else:
        base_date = datetime.now() - timedelta(days=1)

    logging.info(f'Processing data for: {base_date.strftime("%Y-%m-%d")}')

    blob_preffix = 'output/transit_connections'
    blob_name_list = [f"{blob_preffix}-{base_date.strftime('%Y%m%d')}.xlsx"]

    logging.info(f'Processing file: {blob_name_list[0]}')
    
    blob_df = get_blob_df(blob_name_list, ACCOUNTNAME, STORAGEACCOUNTKEY, CONTAINERNAME)

    if blob_df.empty:
        logging.info(f'{blob_name_list} could not be retrieved')
        sys.exit('Ending routine.')
    
    logging.info(f'Finished getting blobs for: {blob_name_list}')

    processed_df = process_data(blob_df)
    processed_df.to_csv(f'{tempfile.gettempdir()}/trip-connection-grading.csv', index=False)
    logging.info('Data processing completed')

    conn_quality_blobname = f'trip_connection_quality-{base_date.strftime("%Y%m%d")}.csv'
    logging.info('Uploading blob')

    upload_workbook(f'{tempfile.gettempdir()}/trip-connection-grading.csv', 'trip-connection-grading', conn_quality_blobname, CONNECTIONSTRING)
    logging.info("Successfully uploaded workbook.")
