import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_datetime64_any_dtype

leh_schema = {
    'date': is_datetime64_any_dtype,
    'userId': is_string_dtype,
    'country': is_string_dtype,
    'total_lp_earned': is_numeric_dtype
}


# Function to validate the dataset
def validate_dataset_leh(dataset, schema, job_timestamp):
    errors = []
    # Check for column count
    if len(dataset.columns) != len(schema):
        errors.append(f"Column count mismatch. Expected {len(schema)}, found {len(dataset.columns)}.")

    # Check for column names and types
    for column, check_function in schema.items():
        if column not in dataset.columns:
            errors.append(f"Missing expected column: {column}")
        else:
            if not check_function(dataset[column]):
                errors.append(f"Column '{column}' has incorrect data type. Expected {check_function.__name__}.")

    # If there are errors, write them to a CSV file
    if errors:
        error_log_path = f'error_sv_leh_{job_timestamp}.csv'
        pd.DataFrame({'error': errors}).to_csv(error_log_path, index=False)
        print(f"Errors found. Details are written to {error_log_path}.")
        return error_log_path
    else:
        print("No errors found. The dataset is valid according to the schema.")
        return None


purchases_schema = {
    'date': is_datetime64_any_dtype,
    'userId': is_string_dtype,
    'revenue': is_string_dtype,
    'transaction_id': is_string_dtype
}


# Function to validate the dataset
def validate_dataset_purchases(dataset, schema, job_timestamp):
    errors = []
    # Check for column count
    if len(dataset.columns) != len(schema):
        errors.append(f"Column count mismatch. Expected {len(schema)}, found {len(dataset.columns)}.")

    # Check for column names and types
    for column, check_function in schema.items():
        if column not in dataset.columns:
            errors.append(f"Missing expected column: {column}")
        else:
            if not check_function(dataset[column]):
                errors.append(f"Column '{column}' has incorrect data type. Expected {check_function.__name__}.")

    if errors:
        error_log_path = f'error_sv_purchases_{job_timestamp}.csv'
        pd.DataFrame({'error': errors}).to_csv(error_log_path, index=False)
        print(f"Errors found. Details are written to {error_log_path}.")
        return error_log_path
    else:
        print("No errors found. The dataset is valid according to the schema.")
        return None


def data_schema_validation(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'playstudios-error-data'
    error_files_leh = []
    error_files_purchases = []

    # Get Files from context
    list_raw_purchases = kwargs['ti'].xcom_pull(task_ids='ps_extract', key='list_raw_purchases')
    list_raw_leh = kwargs['ti'].xcom_pull(task_ids='ps_extract', key='list_raw_leh')
    job_timestamp = kwargs['ti'].xcom_pull(task_ids='ps_extract', key='job_timestamp')

    for file in list_raw_leh:
        df_loyalty_earned_hourly = pd.read_csv(file)
        # Validate the loyalty earned hourly data
        leh_error_log_path = validate_dataset_leh(df_loyalty_earned_hourly, leh_schema, job_timestamp)
        if leh_error_log_path:
            error_files_leh.append(leh_error_log_path)

    for file in list_raw_purchases:

        df_purchases = pd.read_csv(file)
        # Validate the purchases data
        purchases_error_log_path = validate_dataset_purchases(df_purchases, purchases_schema, job_timestamp)
        if purchases_error_log_path:
            error_files_purchases.append(purchases_error_log_path)

    if error_files_leh:
        for error_file in error_files_leh:
            leh_key = f'Loyalty_Earned_Hourly_Data_Set/{error_file}'

            s3_hook.load_file(filename=error_file, key=leh_key, bucket_name=bucket_name, replace=True)

    if error_files_purchases:
        for error_file in error_files_purchases:
            purchases_key = f'Purchases_Data_Set/{error_file}'

            s3_hook.load_file(filename=error_file, key=purchases_key, bucket_name=bucket_name, replace=True)
