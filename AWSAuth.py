# Makes a new session in AWS
import boto3
import os
from dotenv import load_dotenv

def make_aws_session(env_file, region):
    load_dotenv(env_file)
    
    return boto3.Session(aws_access_key_id=os.getenv('AWS_ACCESS_KEY'), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"), region_name=region)
