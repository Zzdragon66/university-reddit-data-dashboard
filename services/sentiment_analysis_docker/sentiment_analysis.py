from transformers import pipeline
import torch
from pathlib import Path
import pandas as pd 
from google.cloud import storage
from google.oauth2 import service_account
import argparse

GCP_PATH = "./gcp_key.json"
LOCAL_STORAGE_PATH = "./text_image.parquet"
LOCAL_SENTIMENT_PATH = "./text_sentiment.parquet"

def initialize_pipeline():
    """Initialize the sentiment analysis pipeline 
    Returns:
        _type_: _description_
    """
    device = None 
    if torch.backends.mps.is_available():
        device = torch.device("mps")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
    else:
        device = torch.device("cpu")
    print(f"the current device is -------{str(device)}----------")
    sentiment_pipeline = pipeline(model="finiteautomata/bertweet-base-sentiment-analysis", device=device)
    return sentiment_pipeline

def predict_sentiment(sentiment_pipeline, texts):
    """Generate the label and map the label to score"""
    senti_map = {"POS" : 1, "NEU" : 0, "NEG" : -1}
    return pd.Series(sentiment_pipeline(texts)).apply(lambda x:senti_map[x["label"]])

def sentiment_main(combined_text_path : str) -> str:
    """Sentiment analysis of the text
    Args:
        combined_text_path (str): the path that combines the text and image text
    Raises:
        FileExistsError: the file did not exists
    RETURNS:
        str: the path that contains the sentiment score
    """
    sentiment_pipeline = initialize_pipeline()
    combined_text_path = Path(combined_text_path)
    if not combined_text_path.exists():
        raise FileExistsError(f"the file {combined_text_path} does not exist")
    # read the data into the dataframe
    combined_text_df = pd.read_parquet(str(combined_text_path))
    text_lst = list(combined_text_df["text"].apply(lambda x:x[:120]))
    sentiment = predict_sentiment(sentiment_pipeline, text_lst)
    combined_text_df["sentiment"] = sentiment 
    # Save the files into a new directory
    combined_text_df.to_parquet(LOCAL_SENTIMENT_PATH) 
    return LOCAL_SENTIMENT_PATH

def cloud_storage_initialize(gcp_path :str = GCP_PATH):
    """Initialize the cloud storage  
    Args: gcp_path: the gcp_key path for initializing the gcp cloud storage
    Returns: the storage client
    """
    credentials = service_account.Credentials.from_service_account_file(gcp_path)
    storage_client = storage.Client(credentials=credentials)
    return storage_client

def get_image_meta(storage_client, text_bucket_name : str, date_directory : str, image_text_path : str) -> pd.DataFrame:
    """get the image meta from the google cloud
    Args:
        storage_client (_type_): the storage client just initialized 
        text_bucket_name (str): the text bucket name on gcp 
        image_text_path (str): the combined image text path
    Returns:
        pd.DataFrame: the pandas dataframe
    """
    text_bucket = storage_client.bucket(text_bucket_name)
    blob_path = f"{date_directory}/{image_text_path}"
    blob = text_bucket.blob(blob_path)
    blob.download_to_filename(LOCAL_STORAGE_PATH)
    return LOCAL_STORAGE_PATH

def upload_to_cloud(storage_client, local_file_path : str, output_bucket_name : str, date_directory:str, output_path : str):
    """upload the file onto the cloud storage

    Args:
        storage_client : the storage client initialized
        local_file_path (str): the local file output
    """
    output_bucket = storage_client.bucket(output_bucket_name)
    write_path = f"{date_directory}/{output_path}"
    output_blob = output_bucket.blob(write_path)
    output_blob.upload_from_filename(local_file_path)

def main(text_bucket_name : str, date_directory : str, image_text_path : str, output_bucket_name : str, output_path : str):
    storage_client = cloud_storage_initialize()
    local_image_text_path = get_image_meta(storage_client, text_bucket_name, date_directory, image_text_path)
    local_text_sentiment_path = sentiment_main(local_image_text_path)
    upload_to_cloud(storage_client, local_text_sentiment_path, output_bucket_name, date_directory, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sentiment analysis of the text")
    parser.add_argument("--text_bucket_name", type=str, required=True, help="the text buckset name")
    parser.add_argument("--date_directory", type=str, required=True, help="the date directory")
    parser.add_argument("--image_text_path", type=str, required=True, help="the combined image text path")
    parser.add_argument("--output_bucket_name", type=str, required=True, help="the output bucketname")
    parser.add_argument("--output_path", type=str, required=True, help = "the output path")
    args = parser.parse_args()
    main(args.text_bucket_name, args.date_directory, args.image_text_path, args.output_bucket_name, args.output_path)
