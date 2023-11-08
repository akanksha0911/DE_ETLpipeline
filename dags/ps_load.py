from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'snowflake_conn_id': 'your_snowflake_conn_id'
}


def run_snowflake_load_sql(**kwargs):
    # Instantiate a SnowflakeHook object
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default', database='PS_STAGING_DB')

    # Write the SQL commands you want to execute
    create_table_command = """
         CREATE SCHEMA IF NOT EXISTS STAGING;
         USE SCHEMA STAGING;
         
         CREATE OR REPLACE TABLE LOYALTY_EARNED_HOURLY (
            DATE TIMESTAMP,
            USER_ID VARCHAR(20),
            COUNTRY CHAR(2),
            TOTAL_LP_EARNED INT
         );

         CREATE OR REPLACE TABLE PURCHASES (
            DATE TIMESTAMP,
            USER_ID VARCHAR(20),
            REVENUE DECIMAL(10, 2),
            TRANSACTION_ID VARCHAR(36)
         );
    """

    # Use the SnowflakeHook to run the SQL commands
    snowflake_hook.run(create_table_command)

    processed_files_s3_leh = kwargs['ti'].xcom_pull(task_ids='ps_transform', key='processed_files_s3_leh')
    processed_files_s3_purchases = kwargs['ti'].xcom_pull(task_ids='ps_transform', key='processed_files_s3_purchases')

    aws_hook = S3Hook(aws_conn_id='aws_default')
    credentials = aws_hook.get_credentials()
    aws_key_id = credentials.access_key
    aws_secret_key = credentials.secret_key

    for file in processed_files_s3_leh:
        print('copy file', file)
        copy_into_command = f"""
            USE SCHEMA STAGING;
            
            COPY INTO LOYALTY_EARNED_HOURLY
            FROM 's3://playstudios-processed-data/{file}' credentials=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
        """

        snowflake_hook.run(copy_into_command)

    for file in processed_files_s3_purchases:
        print('copy file', file)

        copy_into_command = f"""
            USE SCHEMA STAGING;
            
            COPY INTO PURCHASES
            FROM 's3://playstudios-processed-data/{file}' credentials=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
        """

        snowflake_hook.run(copy_into_command)


    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default', database='PS_PROD_DB')
    transformation_command = """
            CREATE OR REPLACE SCHEMA PROD;

            USE SCHEMA PROD;

            CREATE OR REPLACE TABLE Hourly_Daily_Summary (
                app_date TIMESTAMP,
                user_Id VARCHAR(255),
                country VARCHAR(255),
                user_total_lp_earned INT,
                user_total_revenue DECIMAL(10,2),
                user_total_purchases INT,
                user_avg_revenue_per_purchase DECIMAL(10,2),
                total_daily_revenue DECIMAL(10,2)
            );

            INSERT INTO Hourly_Daily_Summary( WITH PurchasesHourly AS (
                SELECT
                    DATE_TRUNC('hour', date) AS date_hourly,
                    USER_ID,
                    REVENUE,
                    transaction_id
                FROM
                    PS_STAGING_DB.STAGING.PURCHASES

            ),
            LoyaltyHourly AS (
                SELECT
                    date,
                    user_Id,
                    country,
                    total_lp_earned
                FROM
                    PS_STAGING_DB.STAGING.LOYALTY_EARNED_HOURLY
            ),
            MergedData AS (
                SELECT
                    l.date AS app_date,
                    l.user_Id,
                    l.country,
                    l.total_lp_earned,
                    p.revenue,
                    p.transaction_id
                FROM
                    LoyaltyHourly l
                LEFT JOIN
                    PurchasesHourly p ON l.user_Id = p.user_Id AND l.date = p.date_hourly
            ),
            HourlySummary AS (
                SELECT
                    app_date,
                    user_Id,
                    country,
                    SUM(total_lp_earned) AS user_total_lp_earned,
                    SUM(COALESCE(revenue, 0)) AS user_total_revenue,
                    COUNT(transaction_id) AS user_total_purchases,
                    CASE WHEN COUNT(transaction_id) > 0
                         THEN ROUND(SUM(COALESCE(revenue, 0)) / COUNT(transaction_id), 2)
                         ELSE 0 END AS user_avg_revenue_per_purchase
                FROM
                    MergedData
                GROUP BY
                    app_date, user_Id, country
            ),
            DailyRevenue AS (
                SELECT
                    user_Id,
                    DATE(app_date) AS date_daily,
                    SUM(COALESCE(revenue, 0)) AS total_daily_revenue
                FROM
                    MergedData
                GROUP BY
                    user_Id, DATE(app_date)
            )
            SELECT
                h.app_date,
                h.user_Id,
                h.country,
                h.user_total_lp_earned,
                h.user_total_revenue,
                h.user_total_purchases,
                h.user_avg_revenue_per_purchase,
                d.total_daily_revenue
            FROM
                HourlySummary h
            LEFT JOIN
                DailyRevenue d ON h.user_Id = d.user_Id AND DATE(h.app_date) = d.date_daily
             )
    """
    snowflake_hook.run(transformation_command)
