from io import StringIO
import sys
import os
import pandas as pd
import requests
import re
from dotenv import load_dotenv


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from utils.minio_helper import save_dataframe_as_parquet_to_minio

# Get the main project directory (2 levels up from this file)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

# Load env variables from .env.dev in the main project directory
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env.dev"))

GUTENBERG_URL: str = os.getenv("GUTENBERG_URL", "")
RAW_TEXT_PATH: str = os.getenv("RAW_TEXT_PATH", "")
RAW_FILE_NAME: str = os.getenv("RAW_FILE_NAME", "")
MINIO_BUCKET: str = os.getenv("RAW_BUCKET", "")
print(GUTENBERG_URL)

def main() -> None:
    os.makedirs(RAW_TEXT_PATH, exist_ok=True)
    response: requests.Response = requests.get(GUTENBERG_URL)
    response.raise_for_status()

    file_path = os.path.join(RAW_TEXT_PATH, RAW_FILE_NAME)
    
    # Save the raw text locally (optional)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"Saved raw text to {file_path}")
    
    # Convert text to DataFrame
    # Split text into lines and create a DataFrame
    lines = response.text.split('\n')
    df = pd.DataFrame({'text': lines})
    
    # Remove empty lines if desired
    df = df[df['text'].str.strip() != '']
    
    print(f"Created DataFrame with {len(df)} lines of text")
    
    # Save DataFrame as parquet to MinIO
    parquet_name = RAW_FILE_NAME.replace('.txt', '.parquet') if RAW_FILE_NAME.endswith('.txt') else f"{RAW_FILE_NAME}.parquet"
    save_dataframe_as_parquet_to_minio(df, f"/{parquet_name}", MINIO_BUCKET)

if __name__ == "__main__":
    main()
