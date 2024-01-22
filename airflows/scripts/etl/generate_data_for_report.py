import argparse
import os
import numpy as np 
import pandas as pd 
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path

GCP_PATH = "./gcp_key.json"

def initialize_bigquery_client(credential_path : str = GCP_PATH):
    credentials = service_account.Credentials.from_service_account_file(credential_path)
    bigquery_client = bigquery.Client(credentials=credentials)
    return bigquery_client

def generate_sql_queries(parent_dir_path : str, project_id : str, dataset_id : str, table_id :str) -> dict:
    """Generate the sql queries 
    Args:
        parent_dir_path (str): the parent directory of SQL
        project_id (str) : the project id 
        dataset_id (str) : the dataset id
        table_id (str) : the table id
    Returns:

    """
    file_paths = pd.Series(os.listdir(parent_dir_path)).apply(lambda x: Path(parent_dir_path) / x)
    sql_file_idx = file_paths.apply(lambda x: x.suffix == ".sql")
    sql_file_paths = (file_paths[sql_file_idx])
    return_dict = {}
    for sql_file_path in sql_file_paths:
        with open(sql_file_path, "r") as sql_file:
            sql_query_format_str = sql_file.read()
            sql_query = sql_query_format_str \
                .replace("project_id", project_id) \
                .replace("dataset_id", dataset_id) \
                .replace("table_id", table_id)
            return_dict[sql_file_path.stem] = sql_query 
    return return_dict

def generate_data_main(storage_bucket : str, storage_directory : str, sql_parent_project : str,
                       project_id : str, dataset_id : str, table_id : str):
    """
    Use the sql and bigquery to generate the data for the report(Cost reduction)
    Args:
        storage_bucket (str) : the storage bucket on google cloud
        storage_path (str) : the storage path on google cloud
    """
    bigquery_client = initialize_bigquery_client(GCP_PATH) 
    # generate the sql query
    sql_queries = generate_sql_queries(sql_parent_project, project_id, dataset_id, table_id) 
    for file_name, sql_str in sql_queries.items(): 
        destination_table = f"{project_id}.{dataset_id}.{file_name}"
        sql_job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        query_job = bigquery_client.query(sql_str, job_config=sql_job_config)
        query_job.result()
        destination_uri = f"gs://{storage_bucket}/{storage_directory}/{file_name}.csv"
        extract_job = bigquery_client.extract_table(
            destination_table,
            destination_uri,
            job_config=bigquery.job.ExtractJobConfig(destination_format="CSV")
        )
        extract_job.result()


if __name__ == "__main__":
    argparser = argparse.ArgumentParser("Generate the data for the report")
    argparser.add_argument("--storage_bucket", required=True, type=str, help="the storage bucket")
    argparser.add_argument("--storage_directory", required=True, type=str, help="the storage directory")
    argparser.add_argument("--sql_parent_project", type=str, required=True, help="the parent directory of the sql queries")
    argparser.add_argument("--project_id", type=str, required=True, help="the project id")
    argparser.add_argument("--dataset_id", type=str, required=True, help="the dataset id")
    argparser.add_argument("--table_id", type=str, required=True, help="the table id")

    args = argparser.parse_args()

    generate_data_main(args.storage_bucket, args.storage_directory, args.sql_parent_project,
                       args.project_id, args.dataset_id, args.table_id)
