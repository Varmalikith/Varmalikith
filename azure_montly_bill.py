import pandas as pd
import psycopg2
import csv
from azure.storage.blob import BlobServiceClient, ContainerClient
from datetime import datetime, timedelta
import os,re

# Define the account name, storage account URL, storage account key, container name, and blob name
account_name = "daywisebilling"
STORAGEACCOUNTURL = f"https://{account_name}.blob.core.windows.net"
STORAGEACCOUNTKEY = "jaCG7aC1Ilp7vpe9xDko8eMwqMTMiFNyY+ArzFTTkoutRQGkAt7KqkeB1HOX4d7cWyKKatRLLIxI+AStcecHHA=="
CONTAINERNAME = "day-wise-billing"
BLOBNAME_PREFIX = "dev/DailyBilling/"   

# Define the date range                          #Download the latest updated csv file within the date range
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 31)

# Define the local directory to save the blobs
local_directory = r"C:\Users\p.likith varma\Desktop\billing_data"

# Create a BlobServiceClient instance
blob_service_client_instance = BlobServiceClient(
account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

# Get a ContainerClient instance for the container
container_client_instance = blob_service_client_instance.get_container_client(CONTAINERNAME)

# Get the list of blobs in the container that match the date range and blob name prefix
blob_list = container_client_instance.list_blobs(name_starts_with=BLOBNAME_PREFIX)

# Iterate over the list of blobs
for blob in blob_list:
    # Extract the date part of the blob name
    date_part = blob.name.split("/")[-2]
    # Parse the date part into a datetime object
    match = re.match("(?P<Y>\d{4})(?P<M>\d{2})(?P<D>\d{2})", date_part)
    if match:
        year = match.group("Y")
        month = match.group("M")
        day = match.group("D")
        blob_date = datetime(int(year), int(month), int(day))
    else:
        continue
    # Check if the blob date falls within the specified date range
    if start_date <= blob_date <= end_date:
        # Get the latest blob by checking the modified time
        latest_blob = max(blob_list, key=lambda x: x.last_modified)
        # Get a BlobClient instance for the current blob
        blob_client_instance = container_client_instance.get_blob_client(latest_blob.name)
        # Download the contents of the blob to the local directory
        local_path = os.path.join(local_directory, latest_blob.name.split("/")[-1])
        with open(local_path, "wb") as f:
            f.write(blob_client_instance.download_blob().readall())


print("The latest obtained CSV file has been downloaded to the local directory.")

df = pd.read_csv(r"C:\Users\p.likith varma\Desktop\billing_data")
# replace NaN values
df = df.fillna("NULL")
# Connect to the PostgreSQL database
conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="Cescost#123",
        host="ces-cost-opt-01.c9girek67w2d.us-east-1.rds.amazonaws.com",
        port='5432'
    )
print("Successfully connected to the database to create a new table")
cursor = conn.cursor()
    # df=df.head(10)
    # Replace NaN values with NULL
df = df.fillna("NULL")
    # Or replace NaN values with a specific value
    # Get the data type of all columns
columns_data_type = df.dtypes
    # Create a table based on the data types of the dataframe
table_name = 'azure_data'
create_table_query = "CREATE TABLE " + table_name + "("
for column_name, column_data_type in columns_data_type.items():
        if str(column_data_type) == 'float64':
            column_data_type = 'REAL'
        elif str(column_data_type) == 'int64':
            column_data_type = 'BIGINT'
        elif str(column_data_type) == 'object':
            column_data_type = 'TEXT'
        create_table_query += column_name + " " + column_data_type + ","
create_table_query = create_table_query[:-1] + ")"
cursor.execute(create_table_query)
    # Copy the records from the dataframe to the table
for i, row in df.iterrows():
        insert_query = "INSERT INTO " + table_name + "(" + ",".join(df.columns) + ") VALUES ("
        for column_name in df.columns:
            if str(df[column_name].dtype) == 'float64':
                insert_query += str(row[column_name]) + ","
            elif str(df[column_name].dtype) == 'int64':
                insert_query += str(row[column_name]) + ","
            else:
                insert_query += "'" + str(row[column_name]) + "',"
        insert_query = insert_query[:-1] + ")"
        cursor.execute(insert_query)
    # Commit the changes
conn.commit()
    # Close the cursor and connection
cursor.close()
conn.close()
print("successfully created a table")

