import requests
import boto3
from dotenv import load_dotenv
import os
import json
from datetime import date

load_dotenv()
# Specify below parameters in your .env file.
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
API_KEY = os.getenv("API_KEY")

AWS_REGION = 'ap-south-1' # Specify the region where S3 bucket exists.
BUCKET_NAME = '<your-bucket-name>' # Specify your bucket name.
CITY = "Bangalore" # desired city name

def ingest_data():
    def fetch_weather_data():
        url = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}' # This is the URL of OpenWeatherMap API.
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            weather_data = response.json()
            
            return weather_data

        except requests.exceptions.RequestException as e:
            print(f'Error fetching weather data: {str(e)}')
            return None

    def upload_to_s3(weather_data):
        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                region_name=AWS_REGION)

        s3_object_key = f'weather_data_{str(date.today())}.json'  # Replace with desired S3 object key

        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_object_key,
                Body=json.dumps(weather_data),
                ContentType='application/json'
            )
            print(f'Successfully uploaded weather data to S3: {s3_object_key}')
        except Exception as e:
            print(f'Error uploading weather data to S3: {str(e)}')

    # Fetch weather data from OpenWeatherMap API
    weather_data = fetch_weather_data()

    if weather_data:
        # Upload weather data to S3
        upload_to_s3(weather_data)
