from transformers import VisionEncoderDecoderModel, ViTImageProcessor, AutoTokenizer
import torch
from pathlib import Path
import pandas as pd 
from PIL import Image
from google.cloud import storage
from google.oauth2 import service_account
import argparse
import os

# Idea:
# download the image meta to local storage
# Generate the image caption from the image
# Combine the image caption and the image meta and upload it back to image_bucket/image_caption/image_caption.parquet

GCP_PATH = "./gcp_key.json"

LOCAL_IMAGE_META_PATH = "./image_meta.parquet"
LOCAL_IMAGE_DIR = "./images"
LOCAL_IMAGE_CAPTION_WRITE_PATH = f"{LOCAL_IMAGE_DIR}/image_caption.parquet"
CLOUD_IMAGE_CAPTION_WRITE_PATH = "image_caption/image_caption.parquet"


def cloud_storage_initialize(gcp_path :str = GCP_PATH):
    """Initialize the cloud storage  
    Args: gcp_path: the gcp_key path for initializing the gcp cloud storage
    Returns: the storage client
    """
    credentials = service_account.Credentials.from_service_account_file(gcp_path)
    storage_client = storage.Client(credentials=credentials) 
    return storage_client

def get_image_meta(storage_client, image_bucket_name : str, date_directory : str, image_meta_path : str) -> pd.DataFrame:
    """get the image meta from the google cloud
    Args:
        storage_client (_type_): the storage client just initialized 
        image_bucket_name (str): the image bucket name on gcp
        image_meta_path (str): the image meta path
    Returns:
        pd.DataFrame: the pandas dataframe
    """
    local_image_dir = Path(LOCAL_IMAGE_DIR)
    if not local_image_dir.exists():
        local_image_dir.mkdir(parents=True)
    image_bucket = storage_client.bucket(image_bucket_name)
    blob_path = f"{date_directory}/{image_meta_path}"
    blob = image_bucket.blob(blob_path)
    blob.download_to_filename(LOCAL_IMAGE_META_PATH)
    return pd.read_parquet(LOCAL_IMAGE_META_PATH)

def get_image(storage_client, image_bucket_name : str, image_path : str) -> str:
    """
        Download the image from the bucket to the local storage and return the local storage path
    Return:
        Return None if the image path does not exist in the cloud
        Return the local storage path if there is an image
    """
    print(f"Start get the file at bucket: {image_bucket_name}, image path: {image_path}")
    image_bucket = storage_client.bucket(image_bucket_name)
    image_blob = image_bucket.blob(image_path)
    # get the local storage if the path does not exist
    local_image_dir = Path(LOCAL_IMAGE_DIR)
    if not local_image_dir.exists:
        local_image_dir.mkdir()
    image_name = Path(image_path).name
    # download to the file and return the path
    image_local_path = local_image_dir / image_name
    print(f"the downloaded file is stored at {image_local_path}")
    try:
        image_blob.download_to_filename(str(image_local_path))
    except:
        return None
    return str(image_local_path)

def model_initialization():
    """
        Initialize the model
    """
    try:
        model = VisionEncoderDecoderModel.from_pretrained("nlpconnect/vit-gpt2-image-captioning")
        feature_extractor = ViTImageProcessor.from_pretrained("nlpconnect/vit-gpt2-image-captioning")
        tokenizer = AutoTokenizer.from_pretrained("nlpconnect/vit-gpt2-image-captioning")
    except:
        raise NotImplementedError("The model has problems")
    # Determine the device 
    device = None 
    if torch.cuda.is_available(): 
        device = torch.device("cuda")
    else:
        device = torch.device("cpu")
    print("the device is: ", device) 
    model.to(device)
    max_length = 16
    num_beams = 4
    gen_kwargs = {"max_length": max_length, "num_beams": num_beams}
    return model, feature_extractor, tokenizer, gen_kwargs, device


def single_image_caption(storage_client, image_bucket_name : str, cloud_image_path : str,
                         model, feature_extractor, tokenizer, device, gen_kwargs) -> str:
    """Generate the caption of a single image
    Args:
        image_path (str): the path to the image 
        model (Huggingfacemodel): the model 
        feature_extractor (_type_): feature extraction of image
        tokenizer (_type_): _description_
        device (_type_): device to run the model
        gen_kwargs (_type_): generation parameters
    Returns:
        str: a string representing the caption of the image
    """ 
    image_path = get_image(storage_client, image_bucket_name, cloud_image_path) 
    print("working on the image path: ", image_path)
    return_val = ""
    if image_path is None:
        return return_val
    try:
        i_image = Image.open(image_path)
        if i_image.mode != "RGB":
            i_image = i_image.convert(mode="RGB")
        pixel_values = feature_extractor(images=i_image, return_tensors="pt").pixel_values
        pixel_values = pixel_values.to(device)
        output_ids = model.generate(pixel_values, **gen_kwargs)
        preds = tokenizer.batch_decode(output_ids, skip_special_tokens=True)
        return_val = preds[-1]
    except:
        print("the model cannot generate caption for this image")
    os.remove(image_path)
    return return_val


def main(image_bucket_name : str, date_directory : str,image_meta_path : str):
    storage_client = cloud_storage_initialize()
    image_meta_df = get_image_meta(storage_client, image_bucket_name, date_directory, image_meta_path)
    model, feature_extractor, tokenizer, gen_kwargs, device = model_initialization()
    # generate the image caption
    image_meta_df["image_caption"] = image_meta_df \
        .apply(
            lambda cur_row : single_image_caption(storage_client, image_bucket_name, cur_row["image_path"],
                                                  model, feature_extractor, tokenizer, device, gen_kwargs)
            ,axis = 1
        )
    # upload the dataframe to the cloud  
    image_meta_df.to_parquet(LOCAL_IMAGE_CAPTION_WRITE_PATH)
    print("write to local parquet path:", LOCAL_IMAGE_CAPTION_WRITE_PATH)
    image_bucket = storage_client.bucket(image_bucket_name)
    image_cloud_write_path = f"{date_directory}/{CLOUD_IMAGE_CAPTION_WRITE_PATH}"
    print("write to cloud storage with cloud storage: ", image_cloud_write_path)
    image_df_blob = image_bucket.blob(image_cloud_write_path)
    image_df_blob.upload_from_filename(LOCAL_IMAGE_CAPTION_WRITE_PATH)

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--image_bucket_name", type = str, required=True, help="the image bucket name")
    parser.add_argument("--date_directory", type=str, required=True, help="the date directory")
    parser.add_argument("--image_meta_path", type=str, required=True, help="the combined image path")

    args=parser.parse_args()
    main(args.image_bucket_name, args.date_directory, args.image_meta_path)