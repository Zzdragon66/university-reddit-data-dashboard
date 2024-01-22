import os 
import argparse
import json
import time 
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import JobStatus
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from pathlib import Path

# This file is run on the vm machine with airflow

GCP_JSON = "gcp_key.json"

def get_project_id():
    "Get the project id from GCP JSON file"
    with open(GCP_JSON, 'r') as file:
        gcp_dict =json.load(file)
    return gcp_dict["project_id"]

def generate_credential():
    """Generate the credential from the gcp_path        
    """
    return service_account.Credentials.from_service_account_file(GCP_JSON)

def initialize_dataproc_client(credential, region):
    """Initialize the Dataproc client."""
    client_options = {
        'api_endpoint': f'{region}-dataproc.googleapis.com:443'
    }
    dataproc_client = dataproc_v1.JobControllerClient(
        credentials=credential,
        client_options=client_options
    )
    return dataproc_client

def initialize_storage(credential):
    """intialize the storage client"""
    storage_client = storage.Client(credentials=credential)
    return storage_client 

def check_blob_exists(storage_client, bucket_name, file_path):
    print(f"Check the storage: \nbucketname {bucket_name}\nfilepath {file_path}")
    blobs = list(storage_client.list_blobs(bucket_name, prefix = file_path))
    return len(blobs) > 0

def file_clean_up(storage_client, storage_bucket_name, direcotry, combine_file_dir = "combined"):
    """Remove the 

    Args:
        storage_client (_type_): the storage client 
        storage_bucket_name (str): the bucket name 
        direcotry (str): the directory under the file 
    """
    bucket = storage_client.bucket(storage_bucket_name)
    prefixes = f"{direcotry}/{combine_file_dir}"
    for blob in storage_client.list_blobs(storage_bucket_name, prefix = prefixes):
        file_names = blob.name.split("/")
        file_name = file_names[-1]
        file_names.pop()
        print(file_name)
        if len(file_name) == 0:
            continue
        if file_name[0] == "_":
            blob.delete()
        else:
            file_names.append("combined.parquet")
            destination_blob_name = "/".join(file_names) 
            print("the destination file name is ", destination_blob_name)
            blob_copy = bucket.copy_blob(
                blob, bucket, destination_blob_name
            )
            blob.delete() 
        

def submit_dataproc(dataproc_client, storage_client, project_id : str, region : str, cluster_name : str, 
                    storage_bucket_name : str, storage_directory : str, 
                    job_bucket_name : str, job_file_path : str, image_bucket_name : str):
    if not check_blob_exists(storage_client, storage_bucket_name, storage_directory):
        raise FileNotFoundError("the storage file is not found")
    if not check_blob_exists(storage_client, job_bucket_name, job_file_path):
        raise FileNotFoundError("the job file is not found")
    
    # if the combine directory exists, simply return
    combined_blob_path = str(Path(storage_directory) / "combined")
    if check_blob_exists(storage_client, storage_bucket_name, str(combined_blob_path)):
        print("Combined directory exists stop the job")
        return None
    # submit the job
    job = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': f"gs://{job_bucket_name}/{job_file_path}",
            'args' : ["--bucket_name", storage_bucket_name, 
                      "--directory", storage_directory,
                      "--image_bucket_name", image_bucket_name]
        }
    }
    print("the job configuratoin is: ", job)
    result = dataproc_client.submit_job(project_id=project_id, region=region, job=job)
    job_id = result.reference.job_id
    print(f"Submitted job ID {job_id}")
    # Wait for the job to complete
    while True:
        job_request = dataproc_v1.GetJobRequest(project_id=project_id, region=region, job_id=job_id)
        job_status = dataproc_client.get_job(request=job_request)
        if job_status.status.state in [JobStatus.State.ERROR, JobStatus.State.CANCELLED, JobStatus.State.DONE]:
            print(f"Job {job_id} finished with state: {job_status.status.state.name}")
            break
        else:
            print(f"Job {job_id} is in state: {job_status.status.state.name}")
            time.sleep(5)
    
    if job_status.status.state == JobStatus.State.ERROR:
        raise NotImplemented("PySpark did not work properly")
    # clean up the file inside the combined 
    file_clean_up(storage_client, storage_bucket_name, storage_directory) 
       

def dataproc_single_directory_main(cluster_name : str, region : str, storage_bucket_name : str, 
                                   storage_directory : str, job_bucket_name : str, job_file_path : str, image_bucket_name : str): 

    proj_id = get_project_id()
    credential = generate_credential()
    dataproc_client = initialize_dataproc_client(credential, region)
    storage_client = initialize_storage(credential)

    submit_dataproc(dataproc_client=dataproc_client, 
                    storage_client=storage_client,
                    project_id=proj_id, 
                    region = region, 
                    cluster_name=cluster_name,
                    storage_bucket_name=storage_bucket_name, 
                    storage_directory=storage_directory, 
                    job_bucket_name=job_bucket_name, 
                    job_file_path=job_file_path,
                    image_bucket_name = image_bucket_name)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit the spark job into dataproc")
    parser.add_argument("--cluster_name", type = str, required=True, help = "the cluster name ")
    parser.add_argument("--region", type=str, required=True, help="the region of the cluster") 
    parser.add_argument("--storage_bucket_name", type=str, required=True, help="the bucket name of the files")
    parser.add_argument("--storage_directory", type = str, required=True, help = "the directory under the files")
    parser.add_argument("--job_bucket_name", type = str, required=True, help = "the job file bucket")
    parser.add_argument("--job_file_path", type = str, required = True, help = "the job file path")
    parser.add_argument("--image_bucket_name", type=str, required=True, help = "the image bucket name")
    
    args = parser.parse_args()
    dataproc_single_directory_main(args.cluster_name, args.region, 
        args.storage_bucket_name, args.storage_directory, 
        args.job_bucket_name, args.job_file_path, args.if_image)