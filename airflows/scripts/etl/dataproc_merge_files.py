from pyspark.sql import SparkSession
from pyspark.sql.functions import split, concat, element_at, size, lit
import os 
from pathlib import Path
import argparse

PARQUET_EXTENSION_STR = ".parquet"

def initialize_spark():
    """Initialize the spark"""
    return SparkSession.builder \
        .appName("Combine Files under single directory") \
        .getOrCreate()

def combine_file(spark, bucket_name : str, directory : str, image_bucket_name : str) -> str:
    """Combine all of the files in the input directory.
    All of the files exists in the bucketname and directory, which are checked in previous steps
    """
    
    gcs_input_path = f"gs://{bucket_name}/{directory}"
    gcs_output_path = f"gs://{bucket_name}/{directory}/combined"
    df = spark.read.parquet(gcs_input_path)
    if image_bucket_name == bucket_name:
        prefix = f"{directory}/images/"
        split_strs = split(df["image_url"], "/")
        image_path_col = concat(lit(prefix), element_at(split_strs, size(split_strs)))
        df = df.withColumn("image_path", image_path_col)
    single_part_df = df.repartition(1)
    single_part_df.write.mode("overwrite").parquet(gcs_output_path)

    
def main():
    parser = argparse.ArgumentParser("merge parquet files under single directory")
    parser.add_argument("--bucket_name", type=str, required=True, help="the bucket name of the files")
    parser.add_argument("--directory", type = str, required=True, help = "the directory under the files")
    parser.add_argument("--image_bucket_name", type=str, required=True, help="Determine whether this is image merge case")
    args = parser.parse_args()
    spark = initialize_spark()
    combine_file(spark, args.bucket_name, args.directory, args.image_bucket_name)

if __name__ == "__main__":
    main()