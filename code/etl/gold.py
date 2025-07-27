import sys
import os
from more_itertools import bucket
from numpy import save
import pandas as pd
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from utils.minio_helper import save_dataframe_as_parquet_to_minio, read_parquet_from_minio

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env.dev"))

SILVER_BUCKET: str = os.getenv("SILVER_BUCKET", "")
SILVER_FILE_NAME: str = os.getenv("SILVER_FILE_NAME", "")
GOLD_BUCKET: str = os.getenv("GOLD_BUCKET", "")
GOLD_FILE_NAME: str = os.getenv("GOLD_FILE_NAME", "")
BOOK_TITLE: str = os.getenv("BOOK_TITLE", "")
BOOK_AUTHOR: str = os.getenv("BOOK_AUTHOR", "")
SOURCE_URL: str = os.getenv("GUTENBERG_URL", "")

file_name = SILVER_FILE_NAME if SILVER_FILE_NAME.endswith('.parquet') else f"{SILVER_FILE_NAME}.parquet"

def gold_transform(silver_df: pd.DataFrame):
    """Transform silver DataFrame to gold format."""
    silver_df["paragraph_id"] = range(1, len(silver_df) + 1)
    silver_df["book_title"] = BOOK_TITLE
    silver_df["author"] = BOOK_AUTHOR
    silver_df["source_url"] = SOURCE_URL
    silver_df["text_length"] = silver_df["paragraph"].str.len()
    # Generate embeddings
    print("Loading embedding model...")
    model = SentenceTransformer("all-MiniLM-L6-v2")

    print("Generating embeddings...")
    embeddings = model.encode(silver_df["paragraph"].tolist(), show_progress_bar=True)
    silver_df["embedding"] = embeddings.tolist()
    print(silver_df.head())
    return silver_df[["paragraph_id", "paragraph", "text_length", "book_title", "author", "source_url", "embedding"]]

    


def main() -> None:
    df = read_parquet_from_minio(SILVER_BUCKET, file_name)
    df = gold_transform(df)
    save_dataframe_as_parquet_to_minio(df, GOLD_FILE_NAME, GOLD_BUCKET)


if __name__ == "__main__":
    main()