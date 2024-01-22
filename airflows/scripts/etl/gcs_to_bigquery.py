from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
import argparse
GCP_PATH = "./gcp_key.json"

def gcs_to_bigquery_main(dataset_id : str, table_id : str, 
                         source_table_bucket : str, source_table_directory : str, source_table_path : str):
    """After generating the parquet table, upload the parquet table into bigquery for query execution
    Args:
        dataset_id (str): the dataset id at bigquery(pre-created on terraform)
        table_id (str): the table id specified on the DAG
        source_table_bucket (str): the source table bucket defined on DAG
        source_table_directory (str): the source table directory defined on DAG
        source_table_path (str): the source table directory defined on DAG
    """
    credential = service_account.Credentials.from_service_account_file(GCP_PATH)
    storage_client = storage.Client(credentials=credential)
    bigquery_client = bigquery.Client(credentials=credential) 
    # the job configuration
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.autodetect = True
    # check whether the file exists or not
    file_blob = storage_client.bucket(source_table_bucket).blob(f"{source_table_directory}/{source_table_path}")
    if not file_blob.exists():
        raise FileNotFoundError(f"the file gs://{source_table_bucket}/{source_table_directory}/{source_table_path} on gcp cloud is not found")
    # the parquet source file 
    gcs_uri = f"gs://{source_table_bucket}/{source_table_directory}/{source_table_path}"
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    # load the parquet file into the bigquery 
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )
    load_job.result()

if __name__ == "__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("--dataset_id", type=str, required=True, help="the dataset id")
   parser.add_argument("--table_id", type=str, required=True, help="the table id")
   parser.add_argument("--source_table_bucket", type=str, required=True, help="the source table bucket")
   parser.add_argument("--source_table_directory", type=str, required=True, help="the source table directory")
   parser.add_argument("--source_table_path", type=str, required=True, help="the source table path")
   args = parser.parse_args()
   # pass the arguments
   gcs_to_bigquery_main(args.dataset_id, args.table_id, 
                        args.source_table_bucket, args.source_table_directory, args.source_table_path) 