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

def check_blob_exists(storage_client, bucket_name, *args):
    file_path = "/".join(args)
    print(f"Check the storage: \nbucketname {bucket_name}\nfilepath {file_path}")
    blobs = list(storage_client.list_blobs(bucket_name, prefix = file_path))
    return len(blobs) > 0

def file_clean_up(storage_client, output_bucket_name : str, date_directory : str, output_directory : str, output_filename :str):
    """Remove the _SUCCESS and _Failure in the output_directory. Rename the parquet file in the output_directory to output_filename 

    Args:
        storage_client (_type_): the storage client 
        storage_bucket_name (str): the bucket name 
        direcotry (str): the directory under the file 
    """
    output_bucket = storage_client.bucket(output_bucket_name)
    prefixes = f"{date_directory}/{output_directory}"
    for blob in storage_client.list_blobs(output_bucket_name, prefix = prefixes):
        file_names = blob.name.split("/")
        file_name = file_names[-1]
        file_names.pop()
        print(file_name)
        if len(file_name) == 0:
            continue
        if file_name[0] == "_":
            blob.delete()
        else:
            file_names.append(output_filename)
            destination_blob_name = "/".join(file_names) 
            print("the destination file name is ", destination_blob_name)
            blob_copy = output_bucket.copy_blob(
                blob, output_bucket, destination_blob_name
            )
            blob.delete() 
        

def dataproc_merge_two_files_submit_main(region : str, cluster_name : str,
    bucket1 : str, bucket2 : str, file1_path : str, file2_path : str, date_directory : str,
    job_bucket_name : str, job_file_path : str,
    output_bucket : str, output_directory : str, output_filename : str,
    sql_statement : str):

    
    credential = generate_credential()
    dataproc_client = initialize_dataproc_client(credential, region)
    storage_client = initialize_storage(credential)
    project_id = get_project_id()

    if not check_blob_exists(storage_client, bucket1, date_directory, file1_path):
        raise FileNotFoundError("the storage file at bucket1 is not found")
    if not check_blob_exists(storage_client, bucket2, date_directory, file2_path):
        raise FileNotFoundError("the storage files at bucket2 is not found")
    if not check_blob_exists(storage_client, job_bucket_name, job_file_path):
        raise FileNotFoundError("the job file is not found")
    
    # if the combine directory exists, simply return
    if check_blob_exists(storage_client, output_bucket, output_directory):
        print("The combined file has already exists. Stop the job")
        return None
    # submit the job
    job = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': f"gs://{job_bucket_name}/{job_file_path}",
            'args' : [  "--bucket1",  bucket1,
                        "--bucket2", bucket2,
                        "--date_directory", date_directory,
                        "--file1_path",  file1_path,
                        "--file2_path", file2_path,
                        "--output_bucket", output_bucket,
                        "--output_directory", output_directory,
                        "--sql_statement", sql_statement
                    ]
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
        raise NotImplementedError("PySpark did not work properly")
    # clean up the file inside the combined 

    file_clean_up(storage_client, output_bucket, date_directory, output_directory, output_filename)
       


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit the spark job into dataproc")
    parser.add_argument("--cluster_name", type = str, required=True, help = "the cluster name ")
    parser.add_argument("--region", type=str, required=True, help="the region of the cluster")
    parser.add_argument("--date_directory", type=str, required=True, help="date directory")
    # two files path
    parser.add_argument("--bucket1", type=str, required=True, help="the bucket that stores the file 1")
    parser.add_argument("--bucket2", type=str, required=True, help="the bucket that stores file 2")
    parser.add_argument("--file1_path", type=str, required=True, help="the file1 path at the bucket1")
    parser.add_argument("--file2_path", type=str, required=True, help="the file2 path at the bucket2")
    # the output file path
    parser.add_argument("--output_bucket", type=str, required=True, help="the output bucket")
    parser.add_argument("--output_directory", type=str, required=True, help="the output directory")
    parser.add_argument("--output_filename", type=str, required=True, help="the output file name ")
    # the job file path
    parser.add_argument("--job_bucket_name", type = str, required=True, help = "the job file bucket")
    parser.add_argument("--job_file_path", type = str, required = True, help = "the job file path")
    # add the sql statement 
    parser.add_argument("--sql_statement", type=str, required=True, help="the sql statement")
    
    args = parser.parse_args()
    credential = generate_credential()
    dataproc_client = initialize_dataproc_client(credential, args.region)
    storage_client = initialize_storage(credential)
    proj_id = get_project_id()
    dataproc_merge_two_files_submit_main(
        cluster_name=args.cluster_name,
        region=args.region,
        date_directory=args.date_directory,
        bucket1=args.bucket1,
        bucket2=args.bucket2,
        file1_path=args.file1_path,
        file2_path=args.file2_path,
        output_bucket=args.output_bucket,
        output_directory=args.output_directory,
        output_filename=args.output_filename,
        job_bucket_name=args.job_bucket_name,
        job_file_path=args.job_file_path,
        sql_statement=args.sql_statement
    )

# Idea:
# the datproc two files will output a the combined file into the directory at `output_directory`
# use file cleanup to rename the combined filename