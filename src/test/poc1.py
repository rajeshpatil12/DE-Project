import os
import logging
from azure.storage.blob import BlobServiceClient
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables from a .env file (if applicable)
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Azure Blob Storage and SQL Server configuration from environment variables
AZURE_CONNECTION_STRING = os.getenv('AZURE_CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
BLOB_NAME = os.getenv('BLOB_NAME')
SQL_SERVER = os.getenv('SQL_SERVER')
SQL_DATABASE = os.getenv('SQL_DATABASE')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')
SQL_DRIVER = 'ODBC Driver 17 for SQL Server'
TABLE_NAME = os.getenv('TABLE_NAME')

def download_blob(blob_service_client, container_name, blob_name, local_file_name):
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        with open(local_file_name, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        logging.info(f"Blob '{blob_name}' downloaded successfully.")
        return True
    except Exception as e:
        logging.error(f"Error downloading blob: {e}")
        return False

def transform_data(file_name):
    try:
        df = pd.read_csv(file_name)
        df.columns = [col.upper() for col in df.columns]  # Example transformation
        df_filtered = df[df['SOME_COLUMN'] == 'some_value']  # Example filter
        transformed_file = "transformed_data.csv"
        df_filtered.to_csv(transformed_file, index=False)
        logging.info("Data transformation completed.")
        return transformed_file
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return None

def load_data_to_sql(engine, transformed_file):
    try:
        df = pd.read_csv(transformed_file)
        df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
        logging.info(f"Data successfully loaded into SQL table '{TABLE_NAME}'.")
    except SQLAlchemyError as e:
        logging.error(f"Error loading data into SQL table: {e}")

def main():
    # Step 1: Connect to Azure Blob Storage and download the file
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    
    # Define the local file to store the blob
    local_file_name = BLOB_NAME.split('/')[-1]
    
    if not download_blob(blob_service_client, CONTAINER_NAME, BLOB_NAME, local_file_name):
        logging.error("Download failed. Exiting.")
        return

    # Step 2: Transform the data
    transformed_file = transform_data(local_file_name)
    if not transformed_file:
        logging.error("Data transformation failed. Exiting.")
        return

    # Step 3: Connect to SQL Server and load the data
    connection_string = f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver={SQL_DRIVER}"
    try:
        engine = create_engine(connection_string)
        load_data_to_sql(engine, transformed_file)
    except SQLAlchemyError as e:
        logging.error(f"Error connecting to the database: {e}")
        return

if __name__ == "__main__":
    main()
