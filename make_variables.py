import json
import os
from pathlib import Path
from argparse import ArgumentParser
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
# Terraform output variables 
TERRAFORM_BUCKETS = [
    "image_bucket",
    "text_bucket",
    "meta_bucket",
    "spark_bucket"
]
TERRAFORM_VM_SSHS = "internal_ip_addresses"  
TERRAFORM_CLUSTER_NAME = "cluster_name"
TERRAFORM_REGION = "region"
TERRAFORM_GPU_VM_NAME = "gpu_vm_name"
TERRAFORM_GPU_IP = "gpu_ip_address"
TERRAFORM_PROJ_ID = "project_id"
TERRAFORM_BIGQUERY_ID = "bigquery_dataset_id"
TERRAFORM_REPORT_BUCKET = "report_bucket"

def load_json(json_file_path : str):
    # Check the json file path and load it into the directory
    json_file_path = Path(json_file_path)
    if not json_file_path.exists():
        raise FileNotFoundError(f"The json file at {json_file_path} does not exist")
    with open(json_file_path, "r") as f:
        return_dict = json.load(f)
    return return_dict

def get_reddit_json(reddit_path : str) -> dict:
    return load_json(reddit_path)

def get_terraform_json(terraforn_json_path : str) -> dict:
    terraform_dict = load_json(terraforn_json_path)
    return_dict = {
        key : terraform_dict[key]["value"] for key in TERRAFORM_BUCKETS
    }
    return_dict[TERRAFORM_VM_SSHS] = terraform_dict[TERRAFORM_VM_SSHS]["value"]
    return_dict["n_ssh_connections"] = len(return_dict[TERRAFORM_VM_SSHS])
    return_dict[TERRAFORM_CLUSTER_NAME] = terraform_dict[TERRAFORM_CLUSTER_NAME]["value"] 
    return_dict[TERRAFORM_REGION] = terraform_dict[TERRAFORM_REGION]["value"]
    return_dict[TERRAFORM_GPU_VM_NAME] = terraform_dict[TERRAFORM_GPU_VM_NAME]["value"]
    return_dict[TERRAFORM_GPU_IP] = terraform_dict[TERRAFORM_GPU_IP]["value"]
    return_dict[TERRAFORM_PROJ_ID] = terraform_dict[TERRAFORM_PROJ_ID]["value"]
    return_dict[TERRAFORM_BIGQUERY_ID] = terraform_dict[TERRAFORM_BIGQUERY_ID]["value"]
    return_dict[TERRAFORM_REPORT_BUCKET] = terraform_dict[TERRAFORM_REPORT_BUCKET]["value"]
    return return_dict

def get_ssh_public_key(ssh_pubic_key_path : str) -> str:
    ssh_pubic_key_path = Path(ssh_pubic_key_path)
    if not ssh_pubic_key_path.exists():
        raise FileNotFoundError("the ssh key is not found")
    with open(ssh_pubic_key_path, "r") as f:
        fst_line = f.readline().strip()
    return fst_line

def generate_docker_variable():
    return {"docker_username" : os.environ.get("docker_username")}

def generate_date() -> (str, str):
    '''Generate the start date and end date'''
    today = datetime.today()
    # end_date = "2024-01-17"
    # start_date = "2024-01-10"
    # return start_date, end_date
    end_date = today.date() - timedelta(days=1)
    start_date = today.date() - timedelta(days = 8)
    return {"start_date" : str(start_date), 
            "end_date" : str(end_date),
            "directory" : f"{str(start_date)}-{str(end_date)}"}


def main():
    parser = ArgumentParser("Make variables for airflow with json output")
    parser.add_argument("--reddit_path", required=True, type=str, help="the reddit file path")
    parser.add_argument("--terraform_path", required=True, type=str, help="the terraform json path")
    parser.add_argument("--ssh_public_key_path", type=str, required=True, help="the ssh public key path")
    parser.add_argument("--output_path", required=True, type=str, help="the output file path")
    args = parser.parse_args()
    reddit_dict = get_reddit_json(args.reddit_path)
    terraform_dict = get_terraform_json(args.terraform_path)
    date_dir = generate_date()
    # HARDCODE SSH_KEY_PATH
    ssh_dict = {
        "ssh_private_key_path" : "/opt/airflow/reddit_ssh",
        "ssh_public_key" : get_ssh_public_key(args.ssh_public_key_path)
    }
    # Combine all of the dict and output the json file called `variables.json`
    json_dict = {}
    json_dict.update(reddit_dict)
    json_dict.update(terraform_dict)
    json_dict.update(ssh_dict)
    json_dict.update(date_dir)
    json_dict.update(generate_docker_variable())
    print(json_dict)
    with open(args.output_path, 'w') as json_file:
        json.dump(json_dict, json_file)

if __name__ == "__main__":
    main()