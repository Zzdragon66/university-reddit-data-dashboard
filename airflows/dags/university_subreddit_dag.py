# NOTE: Assume the number of conneciton is same as number of client id
from datetime import datetime
from pathlib import Path
import json
import os

from airflow import DAG
from airflow import settings
from airflow.models import Connection, Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

import sys
import json
sys.path.append("/opt/airflow/scripts/")

from etl.proj_init import project_init
from etl.dataproc_single_directory import dataproc_single_directory_main
from etl.dataproc_merge_two_files_submit import dataproc_merge_two_files_submit_main
from etl.gcs_to_bigquery import gcs_to_bigquery_main
from etl.generate_data_for_report import generate_data_main 

SUBREDDITS = ["ucla", "berkeley", "USC", "UCSD",
              "UCSantaBarbara", "UCDavis", "stanford", "Caltech", "UCI", "ucmerced"]
SSH_CONNECTION_ID_FOR_STR = "ssh_{id}"

TERRAFORM_STORAGES = [
    "image_bucket",
    "text_bucket",
    "meta_bucket"
]

DATA_META_COMBINED_PATH = "combined/combined.parquet"
IMAGE_CAPTION_PATH = "image_caption/image_caption.parquet"
# Merge image and text
IMAGE_TEXT_DIR = "image_text"
IMAGE_TEXT_FILENAME = "image_text.parquet"
# Sentiment Analaysis 
IMAGE_TEXT_SENTIMENT_PATH = "image_text_sentiment/image_text_sentiment.parquet"
# Merge Meta with text sentiment
META_TEXT_DIR = "meta_text_merge"
META_TEXT_FILENAME = "meta_text.parquet"

with open("/opt/airflow/subreddits.txt", "r") as f:
    SUBREDDITS = f.read().strip().split(" ")

def proj_init_wrapper(): 
    proj_init_result = project_init()
    for key, value in proj_init_result.items():
        if key != "n_ssh_connections" and key != "internal_ip_addresses": 
           Variable.set(key, value) 

def generate_ssh_hooks():
    """
        Generate the shh connection in connection variable
        connection id with format "ssh_{id}" where id is the index(ssh_0, ssh_1, ...)
        We only need n_ssh_connections to get those connections 
    """
    vm_ssh_connections = vm_ssh_connection_generator()
    gpu_ssh_connections = gpu_ssh_connection_generator()
    all_ssh_connections = vm_ssh_connections + gpu_ssh_connections 
    session = settings.Session()
    for conn in all_ssh_connections:
        if not (session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()):
            session.add(conn)
            session.commit()
            print(f"Added new connection with conn_id: {conn.conn_id}")
        else:
            print(f"Connection with conn_id: {conn.conn_id} already exists")

def vm_ssh_connection_generator():
    """Generate the SSH connections for VM Machines
    """
    internal_ip_addresses = json.loads(Variable.get("internal_ip_addresses"))
    ssh_private_key_path = Variable.get("ssh_private_key_path")
    n_vms = len(internal_ip_addresses) 
    conn_lst = []
    for idx in range(n_vms):
        internal_ip = internal_ip_addresses[idx]
        conn = Connection(
            conn_id=SSH_CONNECTION_ID_FOR_STR.format(id = idx),  
            conn_type='SSH',
            host=internal_ip,
            login='airflow',
            extra=json.dumps(
                {"key_file": ssh_private_key_path, 
                    "pulic_key" : Variable.get("ssh_public_key"),
                    "port": 22})
        )
        conn_lst.append(conn)
    return conn_lst

def gpu_ssh_connection_generator():
    """Generate the SSH Connection for the GPU Machine"""
    gpu_vm_name = Variable.get("gpu_vm_name")
    gpu_vm_ip = Variable.get("gpu_ip_address")
    ssh_private_key_path = Variable.get("ssh_private_key_path")
    return [Connection(
            conn_id= gpu_vm_name,  
            conn_type='SSH',
            host=gpu_vm_ip,
            login='airflow',
            extra=json.dumps(
                {"key_file": ssh_private_key_path, 
                 "pulic_key" : Variable.get("ssh_public_key"),
                 "port": 22})
        )]

def pull_vm_dockers():
    """
    Pull the docker image from Dockerhub respository for VM machines
    """
    command_str = """
                    sudo docker pull zzdragon/scrape-reddit:latest &&
                    sudo docker pull zzdragon/scrape-image:latest
    """
    ssh_pull_op_lst = [] 
    n_ssh_connections = int(Variable.get("n_ssh_connections"))
    for i in range(n_ssh_connections):
        ssh_conn_id = SSH_CONNECTION_ID_FOR_STR.format(id = i)
        ssh_pull_op = SSHOperator(
            task_id = f"pull_docker_vm_images_at_{ssh_conn_id}",
            ssh_conn_id = ssh_conn_id,
            command = command_str,
            conn_timeout = 100,
            cmd_timeout = 100
        )
        ssh_pull_op_lst.append(ssh_pull_op)
    return ssh_pull_op_lst

def pull_gpu_docker():
    """_Pull the docker image for GPU"""
    image_names = ["reddit-image-caption", "reddit-sentiment-analysis"] 
    command_str_format = """sudo docker pull zzdragon/{image_name}:latest"""
    gpu_vm_name = Variable.get("gpu_vm_name")
    gpu_ssh_pull_ops = []
    for image_name in image_names:
        ssh_pull_op = SSHOperator(
                task_id = f"gpu_pull_docker_image_of_{image_name}",
                ssh_conn_id = gpu_vm_name,
                command = command_str_format.format(image_name = image_name),
                conn_timeout = 1000,
                cmd_timeout = 1000
            )
        gpu_ssh_pull_ops.append(ssh_pull_op) 
    return gpu_ssh_pull_ops

def ssh_scrape_reddit_ops_generator(ssh_pull_ops):
    """
        Generate the ssh scrape operator 
    """
    n_subreddits = len(SUBREDDITS)
    n_connections = int(Variable.get("n_ssh_connections"))
    scrape_op_lst = []
    client_ids = json.loads(Variable.get("client_id"))
    client_secrets = json.loads(Variable.get("client_secret"))
    for subreddit_idx in range(n_subreddits):
        conn_idx= subreddit_idx % n_connections # the conn id 
        subreddit = SUBREDDITS[subreddit_idx] 
        command_str =f"""
            sudo docker run zzdragon/scrape-reddit:latest \
                --client_id "{client_ids[conn_idx]}" \
                --client_secret "{client_secrets[conn_idx]}" \
                --start_date {Variable.get("start_date")} \
                --end_date {Variable.get("end_date")} \
                --subreddit {subreddit} \
                --directory {Variable.get("directory")} \
                --image_bucket {Variable.get("image_bucket")} \
                --text_bucket {Variable.get("text_bucket")} \
                --meta_bucket {Variable.get("meta_bucket")} 
        """
        ssh_pull_op = ssh_pull_ops[conn_idx] # the previous dependency task
        ssh_conn_id = SSH_CONNECTION_ID_FOR_STR.format(id = conn_idx) # ssh_connection id for SSH operator
        scrape_op = SSHOperator(
            task_id = f"scraping_{subreddit}",
            ssh_conn_id = ssh_conn_id,
            command = command_str,
            conn_timeout = 1000,
            cmd_timeout = 1000
        )
        ssh_pull_op >> scrape_op # specify the dependency
        scrape_op_lst.append(scrape_op)
    return scrape_op_lst 

def dataproc_single_directory_main_wrapper(storage_bucket_name : str):
    """Generate the python operator for merge files in different bucket"""
    return PythonOperator(
        task_id = f"merge_{storage_bucket_name}",
        python_callable = dataproc_single_directory_main,
        op_kwargs = {
            "cluster_name" : Variable.get("cluster_name"),
            "region" : Variable.get("region"),
            "storage_bucket_name" : storage_bucket_name,
            "storage_directory" : Variable.get("directory"),
            "job_bucket_name" : Variable.get("spark_bucket"),
            "job_file_path" : "dataproc_merge_files.py", #Hard code
            "image_bucket_name" : Variable.get("image_bucket"),
        }
    )

def image_scrape_op_generator():
    """scrape the images from the image_url"""
    n_vm_instances = int(Variable.get("n_ssh_connections"))
    image_bucket_name = Variable.get("image_bucket")
    directory = Variable.get("directory")
    ssh_op_lst = []
    for idx in range(n_vm_instances):
        command_str = f"""sudo docker run zzdragon/scrape-image:latest\
            --n_vm_instances {n_vm_instances} \
            --vm_idx {idx} \
            --storage_bucket {image_bucket_name} \
            --directory {directory}
            """
        ssh_conn_id = SSH_CONNECTION_ID_FOR_STR.format(id=idx)
        ssh_op = SSHOperator(
            task_id = f"image_scraping_{idx}",
            ssh_conn_id = ssh_conn_id,
            command = command_str,
            conn_timeout = 1000,
            cmd_timeout = 1000
        )
        ssh_op_lst.append(ssh_op)
    return ssh_op_lst

def image_caption_op_generator():
    conn_id = Variable.get("gpu_vm_name")
    image_bucket_name = Variable.get("image_bucket")
    command_str = f"""sudo docker run --gpus all zzdragon/reddit-image-caption:latest \
        --image_bucket_name {image_bucket_name} \
        --date_directory {Variable.get("directory")} \
        --image_meta_path {DATA_META_COMBINED_PATH}
        """
    return SSHOperator(
            task_id = "generate_image_caption",
            ssh_conn_id = conn_id,
            command = command_str,
            conn_timeout = 10000,
            cmd_timeout = 10000
    )

def dataproc_merge_image_caption_text_op_generator():
    """Merge the text meta with image caption data
    Store the data at the text bucket
    """
    sql_statement = \
    """
        SELECT text.id, IFNULL(concat(text.text, image.image_caption), text.text) as text
        FROM df1 as text
        LEFT JOIN df2 as image
        on text.id = image.id
    """
    return PythonOperator(
        task_id = "merge_image_caption_text",
        python_callable = dataproc_merge_two_files_submit_main,
        op_kwargs = {
            "cluster_name" : Variable.get("cluster_name"),
            "region" : Variable.get("region"),
            "bucket1" : Variable.get("text_bucket"),
            "bucket2" : Variable.get("image_bucket") ,
            "date_directory":Variable.get("directory"),
            "file1_path" : DATA_META_COMBINED_PATH,
            "file2_path" : IMAGE_CAPTION_PATH,
            "job_bucket_name" : Variable.get("spark_bucket"),
            "job_file_path" : "dataproc_merge_two_files.py", # hard code
            "output_bucket" : Variable.get("text_bucket"),
            "output_directory" : IMAGE_TEXT_DIR,
            "output_filename" : IMAGE_TEXT_FILENAME,
            "sql_statement" : sql_statement
        }
    )

def sentiment_analysis_op_generator():
    """Sentiment analysis of the data"""
    conn_id = Variable.get("gpu_vm_name")
    command_str = f"""
        sudo docker run --gpus all zzdragon/reddit-sentiment-analysis:latest --text_bucket_name {Variable.get("text_bucket")} --date_directory {Variable.get("directory")} --image_text_path {f"{IMAGE_TEXT_DIR}/{IMAGE_TEXT_FILENAME}"} --output_bucket_name {Variable.get("text_bucket")} --output_path {IMAGE_TEXT_SENTIMENT_PATH}
    """
    return SSHOperator(
        task_id = "sentiment_analysis",
        ssh_conn_id = conn_id,
        command = command_str,
        conn_timeout = 10000,
        cmd_timeout = 10000
    )

def dataproc_merge_meta_text_op_generator():
    sql_statement = """
        select m.id, m.url, m.score, CONCAT("https://www.reddit.com/user/", m.authorname) as author_url,  
            m.authorname, m.parent, m.create_date, m.subreddit, s.text, s.sentiment
        from df1 as m
        left join df2 as s
        on m.id = s.id
    """
    return PythonOperator(
        task_id = "merge_meta_text",
        python_callable = dataproc_merge_two_files_submit_main,
        op_kwargs = {
            "cluster_name" : Variable.get("cluster_name"),
            "region" : Variable.get("region"),
            "bucket1" : Variable.get("meta_bucket"),
            "bucket2" : Variable.get("text_bucket") ,
            "date_directory" : Variable.get("directory"),
            "file1_path" : DATA_META_COMBINED_PATH,
            "file2_path" : IMAGE_TEXT_SENTIMENT_PATH,
            "job_bucket_name" : Variable.get("spark_bucket"),
            "job_file_path" : "dataproc_merge_two_files.py", # hard code
            "output_bucket" : Variable.get("meta_bucket"),
            "output_directory" :  META_TEXT_DIR,
            "output_filename" : META_TEXT_FILENAME,
            "sql_statement" : sql_statement
        }
    )

def gcs_to_bigquery_wrapper():
    return PythonOperator (
        task_id = "GCS_to_Bigquery",
        python_callable = gcs_to_bigquery_main,
        op_kwargs = {
            "dataset_id" : Variable.get("bigquery_dataset_id"),
            "table_id" : Variable.get("directory"),
            "source_table_bucket" : Variable.get("meta_bucket"),
            "source_table_directory" : Variable.get("directory"),
            "source_table_path" : f"{META_TEXT_DIR}/{META_TEXT_FILENAME}"
        }
    )
    
def generate_data_bigquery_wrapper():
    return PythonOperator(
        task_id = "generate_data_report",
        python_callable = generate_data_main,
        op_kwargs = {
            "storage_bucket" : Variable.get("report_bucket"),
            "storage_directory" : Variable.get("directory"),
            "sql_parent_project" : "/opt/airflow/scripts/etl/sql_queries",
            "project_id" : Variable.get("project_id"),
            "dataset_id" : Variable.get("bigquery_dataset_id"),
            "table_id" : Variable.get("directory")
        }
    )

with DAG(
    dag_id="university-subreddit-data-dashboard",
    start_date=datetime(year=2024, month=1, day=1, hour=0, minute=0, second=1),
    schedule_interval="@yearly",
    tags=["reddit"]
) as dag:
    proj_init = PythonOperator(
        task_id = "project_init",
        python_callable = proj_init_wrapper,
    )
    ssh_hook_generation = PythonOperator(
        task_id = "ssh_hook_generation",
        python_callable = generate_ssh_hooks,
    )
    proj_init >> ssh_hook_generation
    # Pull the docker images 
    with TaskGroup(group_id = "vm_pull_docker_group") as vm_pull_docker_group:
        ssh_vm_docker_pull_lst = pull_vm_dockers()
    with TaskGroup(group_id = "gpu_pull_docker_group") as gpu_pull_docker_group:
        ssh_gpu_docker_pull_lst = pull_gpu_docker()
    ssh_hook_generation >> ssh_vm_docker_pull_lst
    ssh_hook_generation >> ssh_gpu_docker_pull_lst

    # Scrape the docker 
    with TaskGroup(group_id='reddit_scraping_group') as reddit_scraping_group:
        ssh_scrape_reddit_op_lst = ssh_scrape_reddit_ops_generator(ssh_vm_docker_pull_lst)

    # Spark Merge files under the single directory
    with TaskGroup(group_id = "dataproc_spark_merge") as dataproc_spark_merge:
        dataproc_merge_single_directory_ops = []
        image_spark_merge_op = None # create a reference for the dag
        text_spark_merge_op = None
        meta_spark_merge_op = None 
        for bucket_name_key in TERRAFORM_STORAGES:
            cloud_bucket_name = Variable.get(bucket_name_key)
            dataproc_single_directory_merge_op = dataproc_single_directory_main_wrapper(cloud_bucket_name)
            if bucket_name_key == "image_bucket":
                image_spark_merge_op = dataproc_single_directory_merge_op
            if bucket_name_key == "text_bucket":
                text_spark_merge_op = dataproc_single_directory_merge_op
            if bucket_name_key == "meta_bucket":
                meta_spark_merge_op = dataproc_single_directory_merge_op 
            dataproc_merge_single_directory_ops.append(
                dataproc_single_directory_merge_op
            )

    for ssh_op in ssh_scrape_reddit_op_lst:
        ssh_op >> dataproc_merge_single_directory_ops
    
    # scrape the image; it only depends on the image merge op
    with TaskGroup(group_id = "image_scraping_group") as image_scraping_group: 
        ssh_image_scrape_op_lst = image_scrape_op_generator()
        image_spark_merge_op >> ssh_image_scrape_op_lst 

    # image caption generation -> It depends on the ssh_image_scrape
    image_caption_op = image_caption_op_generator()
    ssh_image_scrape_op_lst >> image_caption_op 
    ssh_gpu_docker_pull_lst >> image_caption_op 

    #Merge the image caption and text meta data
    image_text_merge_op = dataproc_merge_image_caption_text_op_generator()
    [image_caption_op, text_spark_merge_op] >> image_text_merge_op

    sentiment_analysis_op = sentiment_analysis_op_generator()
    image_text_merge_op >> sentiment_analysis_op 

    #Merge the meta data and sentiment data
    merge_meta_sentiment_op = dataproc_merge_meta_text_op_generator()
    [meta_spark_merge_op, sentiment_analysis_op] >> merge_meta_sentiment_op
    #Upload the data to the BigQuery & Generate the data for the dashboard
    gcs_to_bigquery_op = gcs_to_bigquery_wrapper()
    bigquery_sqls = generate_data_bigquery_wrapper()
    merge_meta_sentiment_op >> gcs_to_bigquery_op >> bigquery_sqls