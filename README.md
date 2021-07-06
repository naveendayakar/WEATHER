
## What the file does?
1. The python script will download weather data for detroit and another location.
2. Collate the data and store it in the given s3 location(Daily_weather.csv)
The s3 location is currently public.
3. You can upload the csv to database of your choice or else the script will do that for you into mysql database
4. The script performs sql queries to create database, create table and insert the values into the table.
5. The script will then perform the given select query and output the results to s3.

## Note
If you do not want the script to perform ETL to database, run the STOCKX-noDB.py file.
The script will perform steps 1 and 2 only.


## Steps:
1) Clone the STOCKX.py(STOCKX-noDB.py if not ETL needed) file
2) Enter the DB details by editing the STOCKX.py file.(if you have mysql and want the script to do the ETL to database do this step, else skip this if you are using STOCKX-noDB.py)
(Enter host_name ="localhost"
user_name ="root"
pwd="" for the mysql db)
3) Make sure the aws credentials file is located at ~/.aws/credentials on Linux or macOS, or at C:\Users\USERNAME\.aws\credentials on Windows
4) Run python STOCKX.py (or STOCKX-noDB.py if not ETL needed) from the downloaded location

## Files
Daily_weather.csv should have all the data downloaded using weather api for 2 locations.(detroit and herndon)
Results.csv will take all the data from the database, performing select statement as provided.
