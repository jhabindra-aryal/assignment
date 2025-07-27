import sys
import os
from more_itertools import bucket
from numpy import save
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from utils.minio_helper import save_dataframe_as_parquet_to_minio, read_parquet_from_minio

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env.dev"))


RAW_BUCKET: str = os.getenv("RAW_BUCKET", "")
RAW_FOLDER_NAME: str = os.getenv("RAW_FOLDER_NAME", "")
RAW_FILE_NAME: str = os.getenv("RAW_FILE_NAME", "")
BRONZE_BUCKET: str = os.getenv("BRONZE_BUCKET", "")
BRONZE_FILE_NAME: str = os.getenv("BRONZE_FILE_NAME", "")

file_name = RAW_FILE_NAME if RAW_FILE_NAME.endswith('.parquet') else f"{RAW_FILE_NAME}.parquet"

def main() -> None:
    df = read_parquet_from_minio(RAW_BUCKET, file_name)
    save_dataframe_as_parquet_to_minio(df, BRONZE_FILE_NAME, BRONZE_BUCKET)
    

if __name__ == "__main__":
    main()