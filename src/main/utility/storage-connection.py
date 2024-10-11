from azure.storage.blob import BlobServiceClient
import os

# Azure Blob Storage connection details
connection_string = "your_connection_string"
container_name = "your_container_name"
blob_name = "your_blob_name.csv"

# Create BlobServiceClient object to interact with Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Download the blob content
blob_client = container_client.get_blob_client(blob_name)
with open(blob_name, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())

print(f"{blob_name} downloaded successfully.")
