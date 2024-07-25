#python packages used to execute this projects are listed thus:
import requests
import json
import pandas as pd
import os
import ast
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
    This function sends request and retrieve property detailed data for houses for sales in London
    from rapidapi REST API and write the data to an external JSON file. 
    '''
    response = requests.get(url, headers=header, params=querystring_)
    raw_file = response.json() #response variable is converted to .json object
    json_data = json.dumps(raw_file)
    return json_data
get_data_from_api(headers, querystring)