import pandas as pd
import boto3
import json
from dotenv import load_dotenv
import os
from datetime import datetime,date

load_dotenv()

# Specify below parameters in your .env file.
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
API_KEY = os.getenv("API_KEY")

AWS_REGION = 'ap-south-1' # Specify the region where your S3 bucket exists.
BUCKET_NAME = '<your-bucket-name>' # Specify your bucket name.
S3_JSON_OBJECT_KEY = f'weather_data_{str(date.today())}.json'  # Replace with the S3 object key for the JSON data

def transform():
    def transform_json_to_csv(json_data):
        # Transform JSON data to pandas DataFrame
        raw_data = {
            'country': json_data['sys']['country'],
            'city_name': json_data['name'],
            'weather':json_data['weather'][0]['description'],
            'temperature': json_data['main']['temp'],
            'wind':json_data['wind']['speed'],
            'sunrise': json_data['sys']['sunrise'],
            'sunset': json_data['sys']['sunset']}

        df = pd.DataFrame([raw_data])
        # Perform any necessary data transformations on the DataFrame

        # changing unit of temp to Celcius
        df.loc[0,'temperature'] = df['temperature'].iloc[0]-273.15
        # changing wind unit from mps to kmph
        df.loc[0,'wind'] = df['wind'].iloc[0]*3.6
        # changing unix timestamp to datetime for sunrise and sunset time.
        ts = int(df['sunrise'].iloc[0])
        df.loc[0,'sunrise'] = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        ts = int(df['sunset'].iloc[0])
        df.loc[0,'sunset'] = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')



        # Convert DataFrame to CSV format
        csv_data = df.to_csv(index=False)

        return csv_data

    def upload_to_s3(csv_data):
        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                region_name=AWS_REGION)

        s3_object_key = f'weather_data_{str(date.today())}.csv'  # Replace with the desired S3 object key for the CSV data

        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_object_key,
                Body=csv_data,
                ContentType='text/csv'
            )
            print(f'Successfully uploaded transformed data to S3: {s3_object_key}')
        except Exception as e:
            print(f'Error uploading transformed data to S3: {str(e)}')

    # Connect to S3 and retrieve the raw JSON data
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_REGION)

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_JSON_OBJECT_KEY)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f'Error retrieving JSON data from S3: {str(e)}')
        json_data = None

    if json_data:
        # Transform JSON data to CSV
        csv_data = transform_json_to_csv(json_data)

        # Upload transformed CSV data to S3
        upload_to_s3(csv_data)
