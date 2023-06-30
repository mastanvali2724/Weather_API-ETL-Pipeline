import boto3
from dotenv import load_dotenv
import os
import psycopg2
from datetime import date


# credentials
load_dotenv()
# Specify below parameters in your .env file.
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = 'ap-south-1' # Specify the region where S3 bucket exists.
BUCKET_NAME = '<your-bucket-name>' # Specify your bucket name.
S3_JSON_OBJECT_KEY = f'weather_data_{str(date.today())}.csv'

def load():
    # boto3 client for Redshift
    redshift_client = boto3.client('redshift', aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                region_name=AWS_REGION)

    # Define the below redshift Connection details in .env file.
    redshift_endpoint = os.getenv("REDSHIFT_ENDPOINT")
    redshift_port = os.getenv("REDSHIFT_PORT")
    redshift_database = os.getenv("REDSHIFT_DATABASE")
    redshift_user = os.getenv("REDSHIFT_USER")
    redshift_password = os.getenv("REDSHIFT_PASSWORD")


    #setting up connection with redshift
    redshift_conn = psycopg2.connect(
        host=redshift_endpoint,
        port=redshift_port,
        database=redshift_database,
        user=redshift_user,
        password=redshift_password
    )


    # Define the table structure in your Redshift cluster to match the transformed data's schema.
    table_name = 'Weather' # Specify the table name to be created.

    create_table_query = """
    CREATE TABLE IF NOT EXISTS {} (
        Country VARCHAR(50),
        City_name VARCHAR(50),
        weather VARCHAR(50),
        Temperature VARCHAR(50),
        Wind VARCHAR(50),
        Sunrise VARCHAR(50),
        Sunset VARCHAR(50)
    );
    """.format(table_name)

    with redshift_conn.cursor() as cursor:
        cursor.execute(create_table_query)
        redshift_conn.commit()


    # Use the COPY command to load the transformed data from the CSV file into the destination table in Redshift.
    csv_file_path = f's3://{BUCKET_NAME}/{S3_JSON_OBJECT_KEY}'

    copy_query = """
    COPY {} FROM '{}' 
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' 
    CSV IGNOREHEADER 1;
    """.format(table_name, csv_file_path, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    with redshift_conn.cursor() as cursor:
        cursor.execute(copy_query)
        redshift_conn.commit()

    redshift_conn.close()
