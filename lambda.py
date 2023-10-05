import json
import urllib.parse
import boto3
import pandas as pd
import io
import pandas

print('Loading function')
s3 = boto3.client('s3')

# Function to send a text message to the user's mobile number using SNS.
def send_sms(message):
    sns = boto3.client('sns')
    number = '<Phn.number>'
    sns.publish(PhoneNumber = number, Message=message )
    sns.publish(PhoneNumber = number, Message=message )
    

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()),header=0, delimiter=",", low_memory=False)
        print(df)
        message = ''
        message+="Hello, Today's Weather Update for you!!\n"
        message+="There weather in {} for today is: {}\n".format(df.loc[0,'city_name'],df.loc[0,'weather'])
        message+="Temperature: {}DEG CELSIUS\n".format(df.loc[0,'temperature'])
        message+="Wind Speed: {}kmph\n".format(df.loc[0,'wind'])
        print(message)
        
        send_sms(message)
        
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e