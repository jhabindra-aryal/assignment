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

BRONZE_BUCKET: str = os.getenv("BRONZE_BUCKET", "")
BRONZE_FILE_NAME: str = os.getenv("BRONZE_FILE_NAME", "")
SILVER_BUCKET: str = os.getenv("SILVER_BUCKET", "")
SILVER_FILE_NAME: str = os.getenv("SILVER_FILE_NAME", "")

file_name = BRONZE_FILE_NAME if BRONZE_FILE_NAME.endswith('.parquet') else f"{BRONZE_FILE_NAME}.parquet"

def clean_and_group_lines_to_paragraphs(lines):
    """Group broken lines into paragraphs."""
    paragraphs = []
    current_paragraph = ""

    for line in lines:
        line = line.strip().replace("\r", "")
        print(line)
        if line.startswith("*** START OF") or line.startswith("*** END OF"):
            continue
        if not line:
            if current_paragraph:
                paragraphs.append(current_paragraph.strip())
                current_paragraph = ""
        else:
            if current_paragraph:
                current_paragraph += " " + line
            else:
                current_paragraph = line

    if current_paragraph:
        paragraphs.append(current_paragraph.strip())
    print(current_paragraph)
    return paragraphs

def silver_transform(df: pd.DataFrame):
    lines = df.iloc[:, 0].tolist()  # assuming single-column DataFrame
    paragraphs = clean_and_group_lines_to_paragraphs(lines)
    df_silver = pd.DataFrame(paragraphs, columns=["paragraph"])
    df_silver["length"] = df_silver["paragraph"].apply(len)
    return df_silver


def main() -> None:
    df = read_parquet_from_minio(BRONZE_BUCKET, file_name)
    df = silver_transform(df)
    save_dataframe_as_parquet_to_minio(df, SILVER_FILE_NAME,SILVER_BUCKET)


if __name__ == "__main__":
    main()