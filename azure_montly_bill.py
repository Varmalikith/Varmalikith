# import pandas as pd
# import psycopg2
# import csv
# from azure.storage.blob import BlobServiceClient, ContainerClient
# from datetime import datetime, timedelta
# import os,re

# # Define the account name, storage account URL, storage account key, container name, and blob name
# account_name = "daywisebilling"
# STORAGEACCOUNTURL = f"https://{account_name}.blob.core.windows.net"
# STORAGEACCOUNTKEY = "jaCG7aC1Ilp7vpe9xDko8eMwqMTMiFNyY+ArzFTTkoutRQGkAt7KqkeB1HOX4d7cWyKKatRLLIxI+AStcecHHA=="
# CONTAINERNAME = "day-wise-billing"
# BLOBNAME_PREFIX = "dev/DailyBilling/"   

# # Define the date range                          #Download the latest updated csv file within the date range
# start_date = datetime(2023, 2, 1)
# end_date = datetime(2023, 2, 28)


# # Define the local directory to save the blobs
# local_directory = r"C:\Users\p.likith varma\Desktop\billing_data"

# # Create a BlobServiceClient instance
# blob_service_client_instance = BlobServiceClient(
# account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

# # Get a ContainerClient instance for the container
# container_client_instance = blob_service_client_instance.get_container_client(CONTAINERNAME)

# # Get the list of blobs in the container that match the date range and blob name prefix
# blob_list = container_client_instance.list_blobs(name_starts_with=BLOBNAME_PREFIX)

# # Iterate over the list of blobs
# for blob in blob_list:
#     # Extract the date part of the blob name
#     date_part = blob.name.split("/")[-2]
#     # Parse the date part into a datetime object
#     match = re.match("(?P<Y>\d{4})(?P<M>\d{2})(?P<D>\d{2})", date_part)
#     if match:
#         year = match.group("Y")
#         month = match.group("M")
#         day = match.group("D")
#         blob_date = datetime(int(year), int(month), int(day))
#     else:
#         continue
#     # Check if the blob date falls within the specified date range
#     if start_date <= blob_date <= end_date:
#         # Get the latest blob by checking the modified time
#         latest_blob = max(blob_list, key=lambda x: x.last_modified)
#         # Get a BlobClient instance for the current blob
#         blob_client_instance = container_client_instance.get_blob_client(latest_blob.name)
#         # Download the contents of the blob to the local directory
#         local_path = os.path.join(local_directory, latest_blob.name.split("/")[-1])
#         with open(local_path, "wb") as f:
#             f.write(blob_client_instance.download_blob().readall())


# print("The latest obtained CSV file has been downloaded to the local directory.")

# df = pd.read_csv(r"C:\Users\p.likith varma\Desktop\billing_data")
# # replace NaN values
# df = df.fillna("NULL")
# # Connect to the PostgreSQL database
# conn = psycopg2.connect(
#         database="postgres",
#         user="postgres",
#         password="Cescost#123",
#         host="ces-cost-opt-01.c9girek67w2d.us-east-1.rds.amazonaws.com",
#         port='5432'
#     )
# print("Successfully connected to the database to create a new table")
# cursor = conn.cursor()
#     # df=df.head(10)
#     # Replace NaN values with NULL
# df = df.fillna("NULL")
#     # Or replace NaN values with a specific value
#     # Get the data type of all columns
# columns_data_type = df.dtypes
#     # Create a table based on the data types of the dataframe
# table_name = 'azure_data'
# create_table_query = "CREATE TABLE " + table_name + "("
# for column_name, column_data_type in columns_data_type.items():
#         if str(column_data_type) == 'float64':
#             column_data_type = 'REAL'
#         elif str(column_data_type) == 'int64':
#             column_data_type = 'BIGINT'
#         elif str(column_data_type) == 'object':
#             column_data_type = 'TEXT'
#         create_table_query += column_name + " " + column_data_type + ","
# create_table_query = create_table_query[:-1] + ")"
# cursor.execute(create_table_query)
#     # Copy the records from the dataframe to the table
# for i, row in df.iterrows():
#         insert_query = "INSERT INTO " + table_name + "(" + ",".join(df.columns) + ") VALUES ("
#         for column_name in df.columns:
#             if str(df[column_name].dtype) == 'float64':
#                 insert_query += str(row[column_name]) + ","
#             elif str(df[column_name].dtype) == 'int64':
#                 insert_query += str(row[column_name]) + ","
#             else:
#                 insert_query += "'" + str(row[column_name]) + "',"
#         insert_query = insert_query[:-1] + ")"
#         cursor.execute(insert_query)
#     # Commit the changes
# conn.commit()
#     # Close the cursor and connection
# cursor.close()
# conn.close()
# print("successfully created a table")


# def get_columns():
#     connection = psycopg2.connect(
#     database="postgres",
#     user="postgres",
#     password="Cescost#123",
#     host="ces-cost-opt-01.c9girek67w2d.us-east-1.rds.amazonaws.com",
#     port='5432'
# )
#     try:
#         cursor = connection.cursor()
#         Query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'azure_data';"
#         cursor.execute(Query)
#         records = cursor.fetchall()
#         # connection.commit()
#         columns = []
#         for i in records:
#             columns.append(i[0])
#         # columns=columns
#         return columns
#     except:
#         print("check whether the database exits or not")




import calendar,os,datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import psycopg2
import datetime
from io import StringIO

account_name = "daywisebilling"
STORAGEACCOUNTURL = f"https://{account_name}.blob.core.windows.net"
STORAGEACCOUNTKEY = "jaCG7aC1Ilp7vpe9xDko8eMwqMTMiFNyY+ArzFTTkoutRQGkAt7KqkeB1HOX4d7cWyKKatRLLIxI+AStcecHHA=="
CONTAINERNAME = "day-wise-billing"
BLOBNAME_PREFIX = "dev/DailyBilling/"
# Define the storage account name and access key
account_name = "daywisebilling"
account_key = "jaCG7aC1Ilp7vpe9xDko8eMwqMTMiFNyY+ArzFTTkoutRQGkAt7KqkeB1HOX4d7cWyKKatRLLIxI+AStcecHHA=="



blob_service_client = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=account_key)
container_client = blob_service_client.get_container_client(CONTAINERNAME)

# local_directory = r"C:\Users\p.likith varma\Desktop\billing_data"

def extract_date_from_blob_path(blob_path):
    date_str = blob_path.split("/")[-2]
    start_date, end_date = date_str.split("-")
    return start_date, end_date 


now = datetime.datetime.now()
current_year = str(now.year)
current_month = str(now.month).zfill(2)
latest_file = None

blobs = container_client.list_blobs()

for blob in blobs:
    start_date_blob, end_date_blob = extract_date_from_blob_path(blob.name)
    start_month = start_date_blob[:6]
    if ".csv" in blob.name and start_month == current_year + current_month and (not latest_file or blob.last_modified > latest_file.last_modified):
        latest_file = blob
if latest_file is not None:
# Get a reference to the latest updated CSV file for this month
    blob_client = container_client.get_blob_client(latest_file.name)
    

def get_columns():
    connection = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="Cescost#123",
    host="ces-cost-opt-01.c9girek67w2d.us-east-1.rds.amazonaws.com",
    port='5432'
)
    try:
        cursor = connection.cursor()
        Query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'azure_data';"
        cursor.execute(Query)
        records = cursor.fetchall()
        # connection.commit()
        columns = []
        for i in records:
            columns.append(i[0])
        # columns=columns
        return columns
    except:
        print("check whether the database exits or not")
        
        
conn = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="Cescost#123",
    host="ces-cost-opt-01.c9girek67w2d.us-east-1.rds.amazonaws.com",
    port='5432'
)

cursor = conn.cursor()
stream = container_client.get_blob_client(latest_file.name).download_blob()
df = pd.read_csv(StringIO(stream.content_as_text()))
df.columns = map(str.lower, df.columns)

# Check if the table exists, if not, create it
cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'azure_data')")
exists = cursor.fetchone()[0]
if not exists:
    create_query = "CREATE TABLE azure_data ("
    for column_name, column_data_type in zip(df.columns, df.dtypes):
        if column_data_type == 'float64':
            column_data_column = 'REAL'
        elif column_data_type == 'int64':
            column_data_column = 'BIGINT'
        elif column_data_type == 'object':
            column_data_column = 'TEXT'
        create_query += f"{column_name} {column_data_column}, "
    create_query = create_query[:-2] + ")"
    cursor.execute(create_query)
    conn.commit()
else:
    query = "SELECT usagedatetime FROM azure_data"
    cursor.execute(query)
    existing_dates = [row[0] for row in cursor.fetchall()]

    # Filter the data in the CSV file to only include the dates that are not present in the database
    df = df[~df['usagedatetime'].isin(existing_dates)]

buffer = StringIO()
df.to_csv(buffer, index=False, sep="\t", header=False)
buffer.seek(0)

cursor.copy_from(buffer, "azure_data", sep="\t", null="")
print("Data is inserted.")

conn.commit()
            