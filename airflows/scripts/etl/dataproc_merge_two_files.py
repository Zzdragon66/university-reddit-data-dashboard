from pyspark.sql import SparkSession
import argparse

def format_file_name(bucket_name : str, date_directory : str, file_path : str) -> str:
    """Format the file name for google storage"""
    return f"gs://{bucket_name}/{date_directory}/{file_path}"

def initialize_spark():
    """Initialize the spark"""
    return SparkSession.builder \
        .appName("Combine two files") \
        .getOrCreate()


def merge_two_files(spark, file_path1 : str, file_path2 : str, output_path : str, sql_statement : str):
    """Merge two files assume the table for file-path1 is at df1 and table for file-path2 is at df2"""
    df1 = spark.read.parquet(file_path1)
    df2 = spark.read.parquet(file_path2)
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    ret_df = spark.sql(sql_statement)
    ret_df = ret_df.repartition(1)
    ret_df.write.mode("overwrite").parquet(output_path)
    
if __name__ == "__main__":
    spark = initialize_spark()
    parser = argparse.ArgumentParser(description="merge two files")
    parser.add_argument("--date_directory", type=str, required=True, help="the date directory")
    parser.add_argument("--bucket1", type=str, required=True, help="the first bucket")
    parser.add_argument("--bucket2", type=str, required=True, help="the second bucket")
    parser.add_argument("--file1_path", type=str, required=True, help="the first file path")
    parser.add_argument("--file2_path", type=str, required=True, help="the second file path")
    parser.add_argument("--output_bucket", type=str, required=True, help="the output bucket")
    parser.add_argument("--output_directory", type=str, required=True, help="the output directory")
    parser.add_argument("--sql_statement", type=str, required=True, help="the sql statement")
    args = parser.parse_args()
    file_path1 = format_file_name(args.bucket1, args.date_directory, args.file1_path)
    file_path2 = format_file_name(args.bucket2, args.date_directory, args.file2_path)
    output_path = format_file_name(args.output_bucket, args.date_directory, args.output_directory)
    merge_two_files(spark, file_path1, file_path2, output_path, args.sql_statement)