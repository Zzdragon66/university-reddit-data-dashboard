from datetime import datetime
from dateutil import tz
from pathlib import Path
import pandas as pd
import praw
from time import sleep
from zoneinfo import ZoneInfo
from argparse import ArgumentParser
from google.cloud import storage
from google.oauth2 import service_account

SLEEPTIME=6 # Avoid the reddit api limit
LOCAL_STORAGE = "./local_storage/"

meta_post_fields = [
    "id",
    "created_utc",
    "url",
    "score",
]
meta_comments_fields = [
    "id",
    "created_utc",
    "permalink",
    "score"
]
meta_author_fields = [
    "id",
    "name",
]
def reddit_initialization(client_id:str, client_secret:str):
    '''initialize the reddit credential with a json format'''
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent="macos:reddit_app_for_project"
    )
    return reddit

def cloud_storage_init(gcp_path : str = "./gcp_key.json"):
    """Initialize the google storage client

    Args:
        gcp_path (str, optional): the service account json path. Defaults to "./gcp_key.json".

    Returns:
        _type_: the storage client
    """
    credentials = service_account.Credentials.from_service_account_file(gcp_path)
    storage_client = storage.Client(credentials=credentials)
    return storage_client

def store_data(cloud_client, bucket_name : str, dir_path : str, df : pd.DataFrame, file_name : str):
    """
    1. Load the data at the df to the local_storage / file_name with parquet file extension
    2. Upload the data at that path to the google storage with path : bucknet_name / dir / file_name
    Args:
        clouod_client : the google cloud client
        bucket_name (str): the bucket name on google cloud 
        dir (str): the direcotory on the google cloud
        df : the dataframe
        file_name : str
    """
    # 1
    local_storage_path = Path(LOCAL_STORAGE)
    if not local_storage_path.exists():
        local_storage_path.mkdir(parents=True)
    local_file_path = local_storage_path / file_name
    df.to_parquet(local_file_path)
    # 2
    dir_path = Path(dir_path)
    destination_path = dir_path / file_name
    bucket = cloud_client.bucket(bucket_name)
    blob = bucket.blob(str(destination_path))
    blob.upload_from_filename(str(local_file_path))

# def check_if_directory_empty(storage_client, directory:str, bucket_name: str):
#     blobs = storage_client.list_blobs(bucket_name, prefix=directory)
#     return len(list(blobs)) <= 1


def extract_fields(sub, fields, prefix=""):
    '''
    Extract fields from the submission
    Args:
        sub: the submission
        fields: the extracted_fields
        prefix: the prefix for the fields 
    '''
    extracted_fields = dict()
    for field in fields:
        try:
            if hasattr(sub, field):
                extracted_fields[prefix + field] = getattr(sub, field)
            else:
                extracted_fields[prefix + field] = None
        except:
            # to avoid the 404 error
            extracted_fields[prefix + field] = None
    return extracted_fields

def convert_utc_time(sub_dict, time_str) -> dict: 
    '''
        Convert the sub_idct[time_str] to local los angeles time zone
        Delete the sub_dict[time_str] and add a new key called create_date
    '''
    utc_time = sub_dict[time_str]
    cur_time_zone = ZoneInfo('America/Los_Angeles')
    create_date= pd.to_datetime(utc_time, utc=True, unit='s').astimezone(cur_time_zone).date()
    del sub_dict[time_str]
    sub_dict["create_date"] = create_date
    return sub_dict

def extract_post(post, post_fields : list[str], author_fields : list[str]) -> dict:
    """Extract the post from te post fields 
    Args:
        post (_type_): the post submission
        post_fields (_type_): the post fields to be extracted
        author_fields (_type_): the author fields to be extracted
    Returns:
        Returns a dictionary representing a post row
    """
    post_dict = extract_fields(post, post_fields)
    post_dict.update(extract_fields(post.author, author_fields, prefix="author"))
    post_dict["parent"] = None
    return post_dict

def extract_comment(comment, parent_id : str, comment_fields : list[str], author_fields : list[str]) -> dict: 
    """Extract the comment row from the comment submission
    Args:
        comment (_type_): the comment description
        parent_id (str): the parent id of the comment
        comment_fields (list[str]): the fields to be extracted 
        author_fields (list[str]): the fields to be extracted
    Returns:
        dict: a dictionary representing a row of the extracted fields
    """
    comment_dict = extract_fields(comment, comment_fields)
    comment_dict.update(extract_fields(comment.author, author_fields, prefix="author"))
    comment_dict["parent"] = parent_id
    # Change the comment permalink id to url id
    comment_dict["url"] = "https://www.reddit.com" + comment_dict["permalink"]
    del comment_dict["permalink"]
    return comment_dict

def extract_image(post):
    """
    Extract the image features from the post
    Return:
        Return None if the post did not contain the image
        Return an dict if the post contains a image
    """
    if not hasattr(post, "post_hint"):
        return None
    if getattr(post, "post_hint") == "image":
        return {"id" : getattr(post, "id"), "image_url" : getattr(post, "url")}
    return None

def extract_text(sub, title_str=None, body_str=None):
    """Extract the text from posts and comment features 
    Args:
        sub (type): the submission
        title_str (str, optional): the title string. Defaults to None.
        body_str (str, optional): the body string. Defaults to None.
    """
    if title_str is None:
        # comment case 
        if not hasattr(sub, body_str):
            print(f"the attribute {body_str} of the body is None")
            return None
        return {"id" : getattr(sub, "id"), "text" : getattr(sub, body_str)}
    # the post case 
    if not hasattr(sub, title_str) or not hasattr(sub, body_str):
        print(f"the attribute {title_str} or {body_str} is not present")
        return None
    title = getattr(sub, title_str)
    body = getattr(sub, body_str)
    text_str = f"{title}\n{body}"
    return {"id" : getattr(sub, "id"), "text":text_str}

def print_status(post_date, meta_lst : list[dict], image_lst : list[dict], text_lst : list[dict]) -> None:
    """Print the current status of scraping
    Args:
        post_date: the date of the post
        meta_lst (list[dict]): the stored meta list
        image_lst (list[dict]): the stored image list 
        text_lst (list[dict]): the stored text list
    """
    p_str = f"""post_date={str(post_date)}, n_meta_lst={len(meta_lst)}, n_image_list={len(image_lst)}, n_text_list={len(text_lst)}"""
    print(p_str)

def scrape(reddit_instance, storage_client, subreddit_name : str, 
           image_bucket : str, text_bucket : str, meta_bucket : str,
           directory : str,
           time_upper : str, time_lower : str, thres : int = 50):
    """Scrap the subreddit ucla posts and comment

    Args:
        reddit_instance (_type_): the reddit instance initialized with praw
        storage_client: the google storage client
        subreddit_name (str) : the subreddit name to scrap
        image_bukcet (str) : the image bucket 
        text_bucket (str) : the text bucket
        meta_bucket (str) : the meta bucket
        directory (str) : the directory under the bucket
        time_upper (str): the time constraint: upper bound
        time_lower (str): the time constraint: loewr bound
    Returns:
        _type_: _description_
    """
    recent_posts  = reddit_instance.subreddit(subreddit_name).new(limit=None)
    meta_lst, text_lst, image_lst = [], [], []
    time_upper, time_lower = pd.to_datetime(time_upper).date(), pd.to_datetime(time_lower).date()
    meta_cnt, text_cnt, image_cnt = 1, 1, 1
    for post in recent_posts:
        sleep(SLEEPTIME) # avoid the http error from the server for too many requests
        post_dict = extract_post(post, meta_post_fields, meta_author_fields)
        # convert the time to local PST time 
        post_dict = convert_utc_time(post_dict, "created_utc")
        # logic: create_time > time_upper -> continue
        #        create_time < time_lower -> break
        if post_dict["create_date"] > time_upper:
            continue
        if post_dict["create_date"] < time_lower:
            break
        # add the field of subreddit to differentiate between different subreddit
        post_dict["subreddit"] = subreddit_name 
        meta_lst.append(post_dict)
        # Extract the image feature if the post contains an image
        image_meta = extract_image(post)
        if image_meta is not None:
            image_lst.append(image_meta)
        text_meta = extract_text(post, title_str="title", body_str="selftext")
        text_lst.append(text_meta)
        # extract the comments 
        post.comments.replace_more(limit=None)
        parent_id = post_dict["id"]
        print_status(post_dict["create_date"], meta_lst, image_lst, text_lst)
        for comment in post.comments:
            sleep(SLEEPTIME)
            comment_dict = extract_comment(comment, parent_id, meta_comments_fields, meta_author_fields)
            comment_dict = convert_utc_time(comment_dict, "created_utc")
            comment_dict["subreddit"] = subreddit_name 
            meta_lst.append(comment_dict)
            # extract the txt
            text_meta = extract_text(comment, body_str="body")
            text_lst.append(text_meta)
            print_status(post_dict["create_date"], meta_lst, image_lst, text_lst)
        # Write the data back to google cloud storage 
        if len(meta_lst) >= thres:
            df = pd.DataFrame(meta_lst)
            file_name = f"{subreddit_name}-meta-{meta_cnt}.parquet"
            store_data(storage_client, meta_bucket, directory, df, file_name)
            meta_cnt += 1
            meta_lst.clear()
        if len(text_lst) >= thres:
            df = pd.DataFrame(text_lst)
            file_name = f"{subreddit_name}-text-{text_cnt}.parquet"
            store_data(storage_client, text_bucket, directory, df, file_name)
            text_cnt += 1
            text_lst.clear()
        if len(image_lst) >= thres:
            df = pd.DataFrame(image_lst)
            file_name = f"{subreddit_name}-text-{image_cnt}.parquet" 
            store_data(storage_client, image_bucket, directory, df, file_name)
            image_cnt += 1
            image_lst.clear()

    # If there are remaing data in the list
    if len(meta_lst) > 0:
        df = pd.DataFrame(meta_lst)
        file_name = f"{subreddit_name}-meta-{meta_cnt}.parquet"
        store_data(storage_client, meta_bucket, directory, df, file_name)
        meta_cnt += 1
        meta_lst.clear()
    if len(text_lst) > 0:
        df = pd.DataFrame(text_lst)
        file_name = f"{subreddit_name}-text-{text_cnt}.parquet"
        store_data(storage_client, text_bucket, directory, df, file_name)
        text_cnt += 1
        text_lst.clear()
    if len(image_lst) > 0:
        df = pd.DataFrame(image_lst)
        file_name = f"{subreddit_name}-image-{image_cnt}.parquet" 
        store_data(storage_client, image_bucket, directory, df, file_name)
        image_cnt += 1
        image_lst.clear()

def main():
    # parse the argument into the function
    parser = ArgumentParser(description="reddit scraping")
    parser.add_argument("--client_id", type=str, required=True, help="the reddit instance client id")
    parser.add_argument("--client_secret", type=str, required=True,help="the reddit client secrete")
    parser.add_argument("--start_date", type=str, required=True,help="the start date of the post(inclusive)")
    parser.add_argument("--end_date", type=str, required=True,help="the end date of the post(inclusive)")
    parser.add_argument("--subreddit", type=str, required=True,help="the subreddit name")
    parser.add_argument("--directory", type = str, required=True,help = "the directory under the bucket")
    parser.add_argument("--image_bucket", type=str, required=True,help="the image bucket in the google cloud")
    parser.add_argument("--text_bucket", type=str, required=True,help="the text bucket in the google cloud")
    parser.add_argument("--meta_bucket", type=str, required=True,help="the meta bucket in the google cloud")
    args = parser.parse_args()

    # Initialize the instance
    reddit_instance = reddit_initialization(args.client_id, args.client_secret)
    storage_client = cloud_storage_init()
    # if not check_if_directory_empty(storage_client, args.directory, args.meta_bucket): # NOT EMPTY
    #     return None
    # scrape the reddit
    scrape(reddit_instance, storage_client, 
           args.subreddit, args.image_bucket, args.text_bucket, args.meta_bucket,
           args.directory, args.end_date, args.start_date)


if __name__ == "__main__":
    main()