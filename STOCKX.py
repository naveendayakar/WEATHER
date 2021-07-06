#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import csv
import pandas as pd
import os
import boto3
from botocore.exceptions import NoCredentialsError
import io
import pandas as pd
from pandas import DataFrame
import mysql.connector
from sqlalchemy import create_engine


# In[2]:


##mysql db credentials
host_name ="localhost"
user_name ="root"
pwd=""


# In[3]:


##DETROIT DATA
url= 'https://api.openweathermap.org/data/2.5/onecall?lat=42.33&lon=-83.04&units=metric&exclude=current,minutely,hourly,alerts&start=1624975200&end=1625493600&appid=2ba527521ddc5c340fab9f2ae45f9d06'
req_detroit = requests.get(url, allow_redirects=True)

##HERNDON DATA
url= 'https://api.openweathermap.org/data/2.5/onecall?lat=38.96&lon=-77.38&units=metric&exclude=current,minutely,hourly,alerts&start=1624975200&end=1625493600&appid=2ba527521ddc5c340fab9f2ae45f9d06'
req_herndon = requests.get(url, allow_redirects=True)


# In[4]:


def aws_session(region_name='us-east-1'):
    return boto3.session.Session(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                aws_secret_access_key=os.getenv('AWS_ACCESS_KEY_SECRET'),
                                region_name=region_name)
session = aws_session()
s3_resource = session.resource('s3')
bucket = 'navweatherdatastore'


# In[5]:


def dataframe_from_request(request,location):
    try:
#         print(request.json())
        print("Downloading Json From URL")
        print("+++++++++++++================+++++++++++++++")
        json_formatted_str = json.dumps(request.json(), indent=2)
        print("Formatting Json")
        print("+++++++++++++================+++++++++++++++")
#         print(json_formatted_str)
        daily_data = pd.json_normalize(request.json(),record_path =['daily'], meta=['lat', 'lon','timezone','timezone_offset'])
        weather_column = pd.json_normalize(request.json()["daily"], record_path='weather',record_prefix='weather_')
        daily_data=pd.concat([daily_data, weather_column],axis=1)
        daily_data=daily_data.drop("weather",axis=1)
        daily_data["location"]=location
        daily_data['dt']= pd.to_datetime(daily_data['dt'],unit='s').dt.date
        daily_data.rename(columns={'dt':'Date'}, inplace=True)
        print("Converting ['sunrise','sunset','moonrise','moonset'] to UTC time, all temperature in metric")
        print("+++++++++++++================+++++++++++++++")
        for i in ['sunrise','sunset','moonrise','moonset']:
            daily_data[i]=pd.to_datetime(daily_data[i],unit='s').dt.time
        print("Data Frame Created From Json")
        print("+++++++++++++================+++++++++++++++")
        return daily_data
    except FileNotFoundError:
        print("The file was not found")
        print("+++++++++++++================+++++++++++++++")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        print("+++++++++++++================+++++++++++++++")
        return False


# In[6]:


detroit_daily_data=dataframe_from_request(req_detroit,"Detroit")


# In[7]:


herndon_daily_data=dataframe_from_request(req_herndon,"Herndon")


# In[8]:


daily_data=pd.concat([detroit_daily_data, herndon_daily_data], ignore_index=True)


# In[9]:


daily_data.rename(columns = {'Date':'weather_date'}, inplace = True)


# In[10]:


##Uploading the csv to S3
s3_client = boto3.client(
    "s3",
aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
aws_secret_access_key=os.getenv('AWS_ACCESS_KEY_SECRET'),
)

with io.StringIO() as csv_buffer:
    daily_data.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=bucket, Key="daily_weather.csv", Body=csv_buffer.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
        print("+++++++++++++================+++++++++++++++")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")
        print("+++++++++++++================+++++++++++++++")


# In[11]:


#!pip install mysql-connector-python-rf

import mysql.connector as SQLC
# Establishing connection with the SQL
 
DataBase = SQLC.connect(
  host =host_name,
  user =user_name,
  password =pwd
)
# Cursor to the database
Cursor = DataBase.cursor()
 
Cursor.execute("CREATE DATABASE IF NOT EXISTS public")


print("public Data base is created")
print("+++++++++++++================+++++++++++++++")

## could create our own table like below, another option

# TableName ="CREATE TABLE IF NOT EXISTS public.weather_daily_table(Date DATE,Sunrise datetime,sunset datetime, moonrise datetime, moonset datetime,moonphase float,pressure int, humidity int,dew_point float, wind_speed float, wind_deg int, wind_gust float,clouds int,pop float, rain float, uvi float, temp_day float, temp_min float,temp_max float, temp_night float, temp_eve float, temp_morn float, feels_like_day float,feels_like_night float, feels_like_eve float, feels_like_morn float,lat float, lon float,timezone varchar(50), timezone_offset int, weather_id int, weather_main char(20),weather_description varchar(100), weather_icon char(10));"  
# Cursor.execute(TableName)
# print("weather_daily_table is created")

# engine = create_engine('mysql+mysqlconnector://root:@localhost', echo=False)

engine = create_engine('mysql+mysqlconnector://{user}:{passwd}@{host}'.format(
        host=host_name,
        user=user_name,
        passwd=pwd)) 

engine.execute("USE public")
engine.execute("DROP TABLE IF EXISTS weather_daily_table")
daily_data.to_sql(name='weather_daily_table', con=engine, if_exists = 'append', index=False)


print("data inserted into weather_daily_table ")
print("+++++++++++++================+++++++++++++++")
Cursor.execute("SELECT * FROM public.weather_daily_table ORDER BY weather_date,location")
print("Getting Results of  query --> SELECT * FROM public.weather_daily_table ORDER BY weather_date,location")
print("+++++++++++++================+++++++++++++++")
try:
#     records=Cursor.fetchall()
    names = [ x[0] for x in Cursor.description]
    rows = Cursor.fetchall()
    results=pd.DataFrame( rows, columns=names)
    for ind, row in results.iterrows(): 
        print(row.values)
finally:
    if DataBase:
        DataBase.close()
        print("The connection is closed")
        print("+++++++++++++================+++++++++++++++")
        
Cursor.close()


# In[12]:


print("Results exported as csv")
print("+++++++++++++================+++++++++++++++")
results.to_csv("results.csv")


# In[13]:


print("Results exported as excel")    
print("+++++++++++++================+++++++++++++++")
results.to_excel("results.xlsx")


# In[14]:


#Published to Tableau Public
print("Published Results to Tableau public")
print("https://public.tableau.com/app/profile/naveen.dayakar.velpur/viz/WeatherDataStore/Story1")


# In[15]:


##Uploading the csv to S3
s3_client = boto3.client(
    "s3",
aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
aws_secret_access_key=os.getenv('AWS_ACCESS_KEY_SECRET')
)

with io.StringIO() as csv_buffer:
    results.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=bucket, Key="Results.csv", Body=csv_buffer.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful. Posted Final Results to S3 . Status - {status}")
        print("+++++++++++++================+++++++++++++++")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")
        print("+++++++++++++================+++++++++++++++")


# In[16]:


print("Data exported to below AWS folder")
print("https://s3.console.aws.amazon.com/s3/buckets/navweatherdatastore?region=us-east-1&tab=objects#")

