from pathlib import Path

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def process_loyalty_earned_hourly(df, job_timestamp):
    """
    Processes the Loyalty Earned Hourly data.

    Parameters:
    df (DataFrame): The input DataFrame containing the loyalty data.

    Returns:
    Tuple[str, str]: Paths to the processed data CSV file and errors CSV file.
    """
    processed_file_path = Path(f'processed_leh_{job_timestamp}.csv')
    errors_file_path = Path(f'errors_dq_leh_{job_timestamp}.csv')

    try:
        # Separate records with missing values
        errors_df_list = [df[df.isnull().any(axis=1)].copy()]
        df.dropna(inplace=True)

        # Check for userId format
        user_id_pattern = r'^[A-Za-z]{2}\d{2}[A-Za-z]{3}$'
        invalid_user_ids = ~df['userId'].str.match(user_id_pattern)
        errors_df_list.append(df[invalid_user_ids].copy())
        df = df[~invalid_user_ids]

        # Validate country codes
        invalid_countries = ~df['country'].isin(['US', 'CA'])
        errors_df_list.append(df[invalid_countries].copy())
        df = df[~invalid_countries]

        # Convert total_lp_earned to numeric and check for negative values
        df['total_lp_earned'] = pd.to_numeric(df['total_lp_earned'], errors='coerce')
        invalid_lp_earned = df['total_lp_earned'] < 0
        errors_df_list.append(df[invalid_lp_earned].copy())
        df = df[~invalid_lp_earned]

        # Save the records that passed all checks
        df.to_csv(processed_file_path, index=False)
        if errors_df_list:
            # Combine all error records and save
            errors_df = pd.concat(errors_df_list, ignore_index=True)
            errors_df.to_csv(errors_file_path, index=False, mode='a', header=not Path(errors_file_path).exists())

            return str(processed_file_path), str(errors_file_path)


    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle the error or re-raise the exception
        # For example, you could return error messages or status codes

    return str(processed_file_path), None


def process_purchases(df, job_timestamp):
    """
    Processes the Purchases data.

    Parameters:
    df (DataFrame): The input DataFrame containing the purchases data.

    Returns:
    Tuple[str, str]: Paths to the processed data CSV file and errors CSV file.
    """
    processed_file_path = Path(f'processed_purchases_{job_timestamp}.csv')
    errors_file_path = Path(f'errors_dq_purchases_{job_timestamp}.csv')
    try:
        # Separate records with missing values
        errors_df_list = [df[df.isnull().any(axis=1)].copy()]
        df.dropna(inplace=True)

        # Ensure the dates are in the correct format like Loyalty Earned Hourly
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Check for userId format
        user_id_pattern = r'^[A-Za-z]{2}\d{2}[A-Za-z]{3}$'
        invalid_user_ids = ~df['userId'].str.match(user_id_pattern)
        errors_df_list.append(df[invalid_user_ids].copy())
        df = df[~invalid_user_ids]

        # Extract and convert the revenue field to numeric
        df['revenue'] = df['revenue'].str.replace('PriceInUSD=', '', regex=False)
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        # Append non-numeric and non-positive revenue records to errors
        errors_df_list.append(df[(df['revenue'].isnull()) | (df['revenue'] <= 0)].copy())
        # Keep only rows with positive revenue
        df = df[df['revenue'] > 0]

        # Check for duplicate records and remove them
        df = df.drop_duplicates(keep='first')

        # Save the records that passed all checks
        df.to_csv(processed_file_path, index=False)
        if errors_df_list:
            # Combine all error records and save
            errors_df = pd.concat(errors_df_list, ignore_index=True)
            errors_df.to_csv(errors_file_path, index=False, mode='a', header=not errors_file_path.exists())
            return str(processed_file_path), str(errors_file_path)
    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle the error or re-raise the exception
        # For example, you could return error messages or status codes

    return str(processed_file_path), None


def transform_data(**kwargs):
    """
    Processes the raw data.

    Parameters:
    df_loyalty_earned_hourly (DataFrame): The input DataFrame containing the loyalty data.
    df_purchases (DataFrame): The input DataFrame containing the purchases data.

    Returns:
    Tuple[str, str]: Paths to the processed data CSV file and errors CSV file.
    """

    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_error_name = 'playstudios-error-data'
    bucket_processed_name = 'playstudios-processed-data'
    error_files_leh = []
    error_files_purchases = []

    processed_files_leh = []
    processed_files_purchases = []

    processed_files_s3_leh = []
    processed_files_s3_purchases = []

    # Get Files from context
    list_raw_purchases = kwargs['ti'].xcom_pull(task_ids='ps_extract', key='list_raw_purchases')
    list_raw_leh = kwargs['ti'].xcom_pull(task_ids='ps_extract', key='list_raw_leh')
    job_timestamp = kwargs['ti'].xcom_pull(task_ids='ps_extract', key='job_timestamp')

    print('list_raw_leh', list_raw_leh)
    print('list_raw_purchases', list_raw_purchases)
    print('job_timestamp', job_timestamp)

    for file in list_raw_leh:
        df_loyalty_earned_hourly = pd.read_csv(file)
        # Process the loyalty data
        processed_loyalty_earned_hourly_path, errors_loyalty_earned_hourly_path = process_loyalty_earned_hourly(
            df_loyalty_earned_hourly, job_timestamp)

        if errors_loyalty_earned_hourly_path:
            error_files_leh.append(errors_loyalty_earned_hourly_path)

        if processed_loyalty_earned_hourly_path:
            processed_files_leh.append(processed_loyalty_earned_hourly_path)

    for file in list_raw_purchases:
        df_purchases = pd.read_csv(file)
        # Process the purchases data
        processed_purchases_path, errors_purchases_path = process_purchases(df_purchases, job_timestamp)

        if errors_purchases_path:
            error_files_purchases.append(errors_purchases_path)

        if processed_purchases_path:
            processed_files_purchases.append(processed_purchases_path)

    # Upload the processed files to S3
    for file in processed_files_leh:
        leh_key = f'Loyalty_Earned_Hourly_Data_Set/{file}'

        s3_hook.load_file(
            filename=file,
            key=leh_key,
            bucket_name=bucket_processed_name,
            replace=True
        )

        processed_files_s3_leh.append(leh_key)

    for file in processed_files_purchases:
        purchases_key = f'Purchases_Data_Set/{file}'

        s3_hook.load_file(
            filename=file,
            key=purchases_key,
            bucket_name=bucket_processed_name,
            replace=True
        )

        processed_files_s3_purchases.append(purchases_key)


    # Upload the error files to S3
    for file in error_files_leh:
        leh_key = f'Loyalty_Earned_Hourly_Data_Set/{file}'

        s3_hook.load_file(
            filename=file,
            key=leh_key,
            bucket_name=bucket_error_name,
            replace=True
        )

    for file in error_files_purchases:
        purchases_key = f'Purchases_Data_Set/{file}'

        s3_hook.load_file(
            filename=file,
            key=purchases_key,
            bucket_name=bucket_error_name,
            replace=True
        )

    kwargs['ti'].xcom_push(key='processed_files_s3_leh', value=processed_files_s3_leh)
    kwargs['ti'].xcom_push(key='processed_files_s3_purchases', value=processed_files_s3_purchases)

