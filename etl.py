#python packages used to execute this projects are listed thus:
import requests
import json
import pandas as pd
import os
import ast
from datetime import datetime
import boto3
from io import StringIO
import io
from dotenv import dotenv_values
dotenv_values()
from util import get_api_credentials, file_suffix, select_latest_file

#the url to connect to the data source i.e. rapidApi
url = "https://zoopla.p.rapidapi.com/properties/v2/list"

#the credentials used to access the data point
headers= get_api_credentials()[0]
querystring= get_api_credentials()[1]

s3_client = boto3.client('s3') #This is s3 client object
s3_resource = boto3.resource('s3') #This is s3 resource object
bucket_name = 'muyiibucket'#Bucket name in asw S3 
data_staging_path = 'stage_raw_data' # the folder in s3 bucket is used to stage the extracted data from the API
archive_path = 'archived_data' # the folder in S3 used to archive copy of raw data from API
transformed_data = 'transformed_data' # the folder to store transformed data before final load to Redshift

def get_data_from_api(header, querystring_):
    '''
    This function sends request and retrieve property detailed data for 
    houses for sales in London from rapidapi REST API and write the data
    to an external JSON file. 
    '''
    response = requests.get(url, headers=header, params=querystring_)
    raw_file = response.json() #response variable is converted to .json object
    json_data = json.dumps(raw_file)
    return json_data

def raw_property_data(s3_bucket, raw_data_folder):
    '''This function helps in staging the external JSON file into the 
    stage_raw_data folder in the S3 bucket with predefined naming convention. 
    After it has been staged, a copy is archived before transformation.'''
    raw_data = get_data_from_api(headers, querystring)
    file_suffix_ = file_suffix()
    file_name = f"raw_data_{file_suffix_}.json" #file name is defined per time
    bucket = s3_bucket #the S3 bucket which contains the raw_folder for staging raw data
    path = raw_data_folder #the folder that stores raw_data_file
    # using the upload operation to write the data into s3
    try:
        s3_client.put_object(Bucket=bucket, Key=f'{path}/{file_name}', Body= raw_data)
    except Exception as e:
        print(f"Error writting JSON data to S3: {str(e)}")
    print('raw data job is extracted from api and written to S3')
    # a copy of the raw_data is archived forthwith
    raw_data_key = f'{data_staging_path}/{file_name}'
    archive_key = f'{archive_path}/{file_name}'
    try:
        data_from_raw_folder = {'Bucket': bucket, 'Key': raw_data_key}
        s3_client.copy(data_from_raw_folder, bucket_name, archive_key )
    except Exception as e:
        print(f"An error occurred: {e}")
raw_property_data(bucket_name, data_staging_path)

