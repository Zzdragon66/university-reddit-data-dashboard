import argparse
from google.cloud import storage
from google.oauth2 import service_account
from pathlib import Path
import os
import json

def download_directory(storage_client, bucket_name, prefix, destination_dir):
    """Download all files in a directory from GCS bucket."""
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)  
    
    for blob in blobs:
        print(blob.name)
        destination_file_name = os.path.join(destination_dir, blob.name[len(prefix) + 1:]) 
        
        blob.download_to_filename(destination_file_name)
        print(f"Downloaded {blob.name} to {destination_file_name}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcp_key_path", type=str)
    parser.add_argument("--variable_path", type=str)
    
    args = parser.parse_args()
    # initialize the storage 
    if not Path(args.gcp_key_path).exists():
        raise FileNotFoundError("GCP Key File is not found")
    credentials = service_account.Credentials.from_service_account_file(args.gcp_key_path)
    storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
    # Get the variable json file at airflow
    try:
        with open(args.variable_path, "r") as json_file:
            json_dir = json.load(json_file)
    except:
        raise FileExistsError("Json file problem")
    print(json_dir["report_bucket"], json_dir["directory"])
    download_directory(storage_client, 
                       bucket_name=json_dir["report_bucket"],
                       prefix=json_dir["directory"],
                       destination_dir="data/")

if __name__ == "__main__":
    main()