# IDEA: there are k vm instances. this is i the vm. if the idx % k == i, scrape the image and put it into path
import argparse
import io
import requests
import pandas as pd 
import numpy as np
import os 
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account

GCP_JSON = "./gcp_key.json"

def initialize_storage_client(gcp_path):
    # initialize the storage client from the gcp_path
    credentials = service_account.Credentials.from_service_account_file(gcp_path)
    storage_client = storage.Client(credentials=credentials) 
    return storage_client

def scrape_image(storage_client, image_url : str, storage_bucket : str,
                 cloud_image_path : str, local_storage_dir : str):
    local_path = Path(local_storage_dir) / Path(cloud_image_path).name
    try:
        response = requests.get(image_url, timeout=15)
        if response.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(response.content)
            bucket = storage_client.bucket(storage_bucket)
            blob = bucket.blob(cloud_image_path)
            blob.upload_from_filename(str(local_path))
    except Exception:
        return None


def image_scrape_main(n_vm_instances : int, vm_idx : int, storage_bucket : str, directory : str, local_storage_dir = "images"):
    """
        Scrape the image given that meta data is stored at storage_bucket/combined/combined.parquet
    Args:
        n_vm_instances (int): total number of vm instances available
        vm_idx (int): the current vm index
        storage_bucket (str): storage bucket for the meta image data
        local_storage_dir (str, optional): the local storage path Defaults to "images".
    """
    # feaures image_path, image_url
    # scrape the image at image url and put it into image path
    # initialize the storage client
    storage_client = initialize_storage_client(GCP_JSON)
    bucket = storage_client.bucket(storage_bucket)
    # make the local image storage
    local_storage_dir = Path(local_storage_dir)
    if not local_storage_dir.exists():
        local_storage_dir.mkdir(parents=True)
    # get the meta data of the image
    blob = bucket.blob(f"{directory}/combined/combined.parquet") # hard code the file
    in_memory_file = io.BytesIO()
    blob.download_to_file(in_memory_file)
    in_memory_file.seek(0)
    df = pd.read_parquet(in_memory_file)
    n_rows = len(df)
    # assign the specified rows into the vm
    rows_idx = (np.arange(n_rows) % n_vm_instances) == vm_idx
    df = df.iloc[rows_idx, :]
    df.apply(lambda cur_row : scrape_image(storage_client,
                                           cur_row["image_url"],
                                           storage_bucket,
                                           cur_row["image_path"],
                                           str(local_storage_dir)), axis = 1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_vm_instances", type=int, required=True, help="Numer of instances in the cloud")
    parser.add_argument("--vm_idx", type=int, required=True, help = "the current vm index")
    parser.add_argument("--storage_bucket", type=str, required=True, help = "the storage bucket")
    parser.add_argument("--directory", type=str, required=True, help="the directory with format start_date-end_date")
    args = parser.parse_args()
    image_scrape_main(args.n_vm_instances, args.vm_idx, args.storage_bucket, args.directory)
    