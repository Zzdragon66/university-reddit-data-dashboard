from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account
from airflow.models import Variable

# Define some constant values here
HOME = "/opt/airflow"
REDDIT_JSON = "reddit.json"
TERRAFORM_JSON = "terraform.json"
GCP_JSON = "gcp_key.json"
SSH_KEY_PATH = "/opt/airflow/reddit_ssh"

TERRAFORM_IPS = [
    "internal_ip_addresses",
]
TERRAFORM_STORAGES = [
    "image_bucket",
    "text_bucket",
    "meta_bucket"
]

SPARK_PYTHON_FILES = [
    f"{HOME}/scripts/etl/dataproc_merge_files.py",
    f"{HOME}/scripts/etl/dataproc_merge_two_files.py"
]


def read_json_file(json_file_name) -> dict:
    file_path = Path(HOME) / json_file_name
    if not file_path.exists():
        raise FileExistsError(f"json file {json_file_name} does not exist")
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

# def generate_reddit_credential() -> dict:
#     reddit_dict = read_json_file(REDDIT_JSON)
#     return reddit_dict

# def generate_terraform_data() -> dict:
#     terraform_dict = read_json_file(TERRAFORM_JSON)
#     terraform_data = {
#         ip_key : terraform_dict[ip_key]["value"] for ip_key in TERRAFORM_IPS
#     }
#     terraform_data.update(
#         {
#             store_key : terraform_dict[store_key]["value"] for store_key in TERRAFORM_STORAGES
#         }
#     )
#     return terraform_dataå

def generate_date() -> (str, str):
    '''Generate the start date and end date'''
    today = datetime.today()
    # end_date = "2024-01-17"
    # start_date = "2024-01-10"
    # return start_date, end_date
    end_date = today.date() - timedelta(days=1)
    start_date = today.date() - timedelta(days = 8)
    return str(start_date), str(end_date) 

def initialize_storage_client(gcp_path = GCP_JSON):
    gcp_path = Path(gcp_path)
    if not gcp_path.exists():
        raise FileNotFoundError(f"GCP file at {gcp_path} does not exists")
    credentials = service_account.Credentials.from_service_account_file(GCP_JSON)
    storage_client = storage.Client(credentials=credentials) 
    return storage_client

def upload_spark_job_file(storage_client, file_path : str, spark_job_bucket_name : str) :
    print(file_path)
    python_file_path = Path(file_path)
    if not python_file_path.exists():
        raise FileNotFoundError(f"The python file {file_path} is not found")
    # upload the file into the blob
    bucket = storage_client.bucket(spark_job_bucket_name)
    blob = bucket.blob(python_file_path.name)
    blob.upload_from_filename(file_path)

def project_init() -> dict:
    '''
    Generate the reddit credential and store it int
    Generate the terraform output data with keys in macros 
    Generate the start date and end date and use it as the direcotry under the bucket name 
    Generate the ssh_key path to have access to the 
    '''
    # List the directory
    print(os.listdir(HOME))
    # Get the terraform and reddit data
    ret_dict = {}
    # Generate the dates 
    start_date, end_date = generate_date()
    ret_dict["start_date"] = str(start_date)
    ret_dict["end_date"] = str(end_date)
    date_str = f"{start_date}-{end_date}"
    ret_dict["directory"] = date_str
    # Put the ssh_key path
    if not Path(SSH_KEY_PATH).exists():
        raise FileNotFoundError("The SSH key is not found under this path")
    ret_dict["ssh_key_path"] = SSH_KEY_PATH
    # upload the spark_job_file into the cloud machines
    storage_client = initialize_storage_client(GCP_JSON)
    spark_job_bucket_name = Variable.get("spark_bucket")
    for spark_job_file_path in SPARK_PYTHON_FILES:
        upload_spark_job_file(storage_client, spark_job_file_path, spark_job_bucket_name)
    return ret_dict
