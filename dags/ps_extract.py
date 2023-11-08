import os
from datetime import datetime
from io import StringIO, BytesIO

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def extract(**kwargs):
    directory_path = os.environ.get("directory_path", "/opt/airflow/data/")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'playstudios-landing-data'  # replace with your S3 bucket name
    job_timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    # Define the column sets for each dataset
    loyalty_columns = {'date', 'userId', 'country', 'total_lp_earned'}
    purchases_columns = {'date', 'userId', 'revenue', 'transaction_id'}

    list_raw_leh = []
    list_raw_purchases = []
    # Process each Excel file in the directory
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.xlsx'):
            file_path = os.path.join(directory_path, file_name)
            # Load the entire Excel file
            xls = pd.ExcelFile(file_path)

            # Iterate through each sheet in the Excel file
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)

                # Check if the sheet has the columns for the loyalty dataset
                if loyalty_columns.issubset(set(df.columns.tolist())):
                    loyalty_csv_path = f'Loyalty_Earned_Hourly_Data_Set_{job_timestamp}.csv'
                    loyalty_key = f'Loyalty_Earned_Hourly_Data_Set/data_{job_timestamp}.csv'
                    df.to_csv(loyalty_csv_path, index=False)
                    s3_hook.load_file(filename=loyalty_csv_path, key=loyalty_key, bucket_name=bucket_name, replace=True)

                    list_raw_leh.append(loyalty_csv_path)
                # Check if the sheet has the columns for the purchases dataset
                elif purchases_columns.issubset(df.columns):
                    purchases_csv_path = f'Purchases_Data_Set_{job_timestamp}.csv'
                    purchases_key = f'Purchases_Data_Set/data_{job_timestamp}.csv'
                    df.to_csv(purchases_csv_path, index=False)
                    s3_hook.load_file(filename=purchases_csv_path, key=purchases_key, bucket_name=bucket_name,
                                      replace=True)

                    list_raw_purchases.append(purchases_csv_path)

    kwargs['ti'].xcom_push(key='list_raw_purchases', value=list_raw_purchases)
    kwargs['ti'].xcom_push(key='list_raw_leh', value=list_raw_leh)
    kwargs['ti'].xcom_push(key='job_timestamp', value=job_timestamp)

