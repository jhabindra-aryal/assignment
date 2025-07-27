import os
import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
import io

load_dotenv(dotenv_path=".env.dev")

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY: str = os.getenv("MINIO_ROOT_PASSWORD", "")


minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

def delete_bucket(bucket_name: str):
    minio_client.remove_bucket(bucket_name)
    print(f"Bucket {bucket_name} deleted successfully.")

def save_dataframe_as_parquet_to_minio(df, object_name, MINIO_BUCKET):
    """
    Save a pandas DataFrame as a parquet file to MinIO
    
    Args:
        df (pd.DataFrame): The DataFrame to save
        object_name (str): The name/path for the object in MinIO (should end with .parquet)
    
    Returns:
        bool: True if successful, False otherwise
    """

    
    # Create bucket if not exists
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
    except S3Error as e:
        print(f"MinIO bucket error: {e}")
        return False
    
    try:
        # Convert DataFrame to parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        
        # Upload to MinIO
        minio_client.put_object(
            MINIO_BUCKET,
            object_name,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        
        print(f"Uploaded DataFrame as parquet {object_name} to MinIO bucket {MINIO_BUCKET}")
        return True
        
    except Exception as e:
        print(f"Error uploading parquet file to MinIO: {e}")
        return False
    

def read_parquet_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    response = minio_client.get_object(bucket, object_name)
    data = response.read()  # read parquet bytes into memory
    buffer = io.BytesIO(data)  # wrap bytes in-memory buffer
    df = pd.read_parquet(buffer)
    return df

def save_parquet_file_to_minio(file_path, object_name):
    """
    Save an existing parquet file to MinIO
    
    Args:
        file_path (str): Path to the local parquet file
        object_name (str): The name/path for the object in MinIO
    
    Returns:
        bool: True if successful, False otherwise
    """
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    
    # Create bucket if not exists
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
    except S3Error as e:
        print(f"MinIO bucket error: {e}")
        return False
    
    try:
        with open(file_path, "rb") as file_data:
            file_stat = os.stat(file_path)
            minio_client.put_object(
                MINIO_BUCKET,
                object_name,
                data=file_data,
                length=file_stat.st_size,
                content_type="application/octet-stream"
            )
        print(f"Uploaded parquet file {object_name} to MinIO bucket {MINIO_BUCKET}")
        return True
        
    except Exception as e:
        print(f"Error uploading parquet file to MinIO: {e}")
        return False