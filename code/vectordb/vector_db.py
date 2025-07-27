import os
import sys
import pandas as pd
from dotenv import load_dotenv
import chromadb
from chromadb.config import Settings

# Setup import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from utils.minio_helper import read_parquet_from_minio

# Load env
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env.dev"))

# Constants
GOLD_BUCKET: str = os.getenv("GOLD_BUCKET", "")
GOLD_FILE_NAME: str = os.getenv("GOLD_FILE_NAME", "")
CHROMA_HOST: str = os.getenv("CHROMA_HOST", "chroma")
CHROMA_PORT: str = os.getenv("CHROMA_PORT", "8000")

# Ensure correct file extension
file_name = GOLD_FILE_NAME if GOLD_FILE_NAME.endswith('.parquet') else f"{GOLD_FILE_NAME}.parquet"

def main() -> None:
    print("Reading embedding data from MinIO...")
    df = read_parquet_from_minio(GOLD_BUCKET, file_name)

    print(f"Connecting to ChromaDB at {CHROMA_HOST}:{CHROMA_PORT}...")
    client = chromadb.Client(Settings(
        chroma_server_host=CHROMA_HOST,
        chroma_server_http_port=CHROMA_PORT
    ))


    collection = client.get_or_create_collection(name="documents")
    # collection = chromadb.Client().get_or_create_collection(name="documents")

    print("Deleting existing documents from ChromaDB collection...")

    print("Adding new embeddings...")
    ids = df['paragraph_id'].astype(str).tolist()
    documents = df['paragraph'].tolist()
    embeddings = df['embedding'].tolist()
    metadatas = df[['book_title', 'author', 'source_url']].to_dict(orient='records')

    print("Number of documents in collection:", collection.count())
    print(ids[:5])  # Print first 5 IDs for debugging
    print(documents[:5])  # Print first 5 documents for debugging       
    print(metadatas[:5])  # Print first 5 metadata records for debugging
    # print(embeddings[:5])  # Print first 5 embeddings for debugging

    collection.add(
        ids=ids,
        embeddings=embeddings,
        documents=documents,
        metadatas=metadatas
    )

    
    print("Number of documents in collection:", collection.count())

    print(f"✅ Successfully loaded {len(ids)} documents into ChromaDB.")

if __name__ == "__main__":
    main()
